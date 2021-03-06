import concurrent.futures
from multiprocessing import cpu_count
import os
import re
import subprocess

import remoto.process

import spark_deploy.internal.defaults.install as install_defaults
import spark_deploy.internal.defaults.submit as defaults
from spark_deploy.internal.remoto.modulegenerator import ModuleGenerator
from spark_deploy.internal.remoto.ssh_wrapper import get_wrapper, get_wrappers, close_wrappers
import spark_deploy.internal.util.fs as fs
import spark_deploy.internal.util.importer as importer
import spark_deploy.internal.util.location as loc
from spark_deploy.internal.util.printer import *


def _submit_spark(remote_connection, module, command, cwd, silent=False):
    remote_module = remote_connection.import_module(module)
    if not silent:
        print('Executing: {}'.format(command))
    return remote_module.submit(command, cwd)


def _generate_module_submit(silent=False):
    '''Generates Spark-submit module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'submit_spark.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'env.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'spark_submit.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    ModuleGenerator().with_module(fs).with_files(*files).generate(generation_loc, silent)
    return importer.import_full_path(generation_loc)



def _get_master_and_workers(reservation, master_id=None):
    '''Divides nodes in 1 master and a list of workers.
    Args:
        reservation (`metareserve.Reservation`): Nodes to divide into a master + workers.
        master_id (optional int): If set, node with given ID will be master. If `None`, node with lowest public ip string value will be master.
    
    Returns:
        1 master, and a list of workers.'''
    if len(reservation) == 1:
        return next(reservation.nodes), []

    if master_id == None: # Pick node with lowest public ip value.
        tmp = sorted(reservation.nodes, key=lambda x: x.ip_public)
        return tmp[0], tmp[1:]
    return reservation.get_node(node_id=master_id), [x for x in reservation.nodes if x.node_id != master_id]


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def _validate_jvm_bytes(string):
    '''Returns `True` if string adheres to JVM byte size notation, `False` otherwise. E.g: 4M, 1G. Accepted suffices include "k, m, g, t". Only integers are accepted.'''
    regex = re.compile(r'[0-9]+[k|b|m|g]', re.IGNORECASE)
    return regex.fullmatch(string) != None


def clean(reservation, key_path, paths, admin_id=None, connectionwrapper=None, silent=False):
    '''Cleans data from the RADOS-Ceph cluster, on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        paths (list(str)): Data paths to delete to the remote cluster. Mountpoint path is always prepended.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        connectionwrapper (RemotoSSHWrapper): If set, uses provided connection to the admin instead of making a new one.
        mountpoint_path (optional str): Path where CephFS is mounted on all nodes.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.

    Returns:
        `True` on success, `False` otherwise.'''
    if (not reservation) or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))
    
    master_picked, workers_picked = _get_master_and_workers(reservation, master_id)
    print('Picked master node: {}'.format(master_picked))

    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    local_connections = connectionwrapper == None
    if local_connections:
        connectionwrapper = get_wrapper(admin_picked, admin_picked.ip_public, ssh_params=ssh_kwargs, silent=silent)

    if any(paths):
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
            if not silent:
                print('Exporting data...')
            rm_futures = [executor.submit(remoto.process.check, connectionwrapper.connection, 'sudo rm -rf {}'.format(fs.join(mountpoint_path, path)), shell=True) for path in paths]

            state_ok = all(x.result()[2] == 0 for x in rm_futures)

    if not any(paths):
        _, _, exitcode = remoto.process.check(connectionwrapper.connection, 'sudo rm -rf {}/*'.format(mountpoint_path), shell=True)
        state_ok = exitcode == 0
    else:
        paths = [x if x[0] != '/' else x[1:] for x in paths]
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
            if not silent:
                print('Deleting data...')
            rm_futures = [executor.submit(remoto.process.check, connectionwrapper.connection, 'sudo rm -rf {}'.format(fs.join(mountpoint_path, path)), shell=True) for path in paths]

            state_ok = all(x.result()[2] == 0 for x in rm_futures)

    if local_connections:
        close_wrappers(connectionwrapper)

    if state_ok:
        prints('Data deleted.')
    else:
        printe('Could not delete data.')
    return state_ok



def submit(reservation, command, paths=[], install_dir=install_defaults.install_dir(), key_path=None, connectionwrappers=None, application_dir=defaults.application_dir(), master_id=None, use_sudo=False, silent=False):
    '''Submit applications using spark-submit on the remote Spark cluster, on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to we run Spark on. 
                                                 Important if we deploy in cluster mode, as every node could be chosen to boot the JAR on, meaning every node must have the JAR.
                                                 In client mode, you can just provide only the master node.
        command (str): Command to propagate to remote "spark-submit" executable.
        paths (optional list(str)): Data paths to offload to the remote cluster. Can be relative to CWD or absolute.
        install_dir (str): Location on remote host where Spark (and any local-installed Java) is installed in.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        connectionwrappers (optional dict(metareserve.Node, RemotoSSHWrapper)): If set, uses provided connections instead of making new ones.
        application_dir (optional str): Location on remote host where we export all given 'paths' to.
                                        Illegal values: 1. ''. 2. '~/'. The reason is that we use rsync for fast file transfer, which messes up homedir permissions if set as destination target.
        master_id (optional int): Node id of the Spark master. If `None`, the node with lowest public ip value (string comparison) will be picked.
        use_sudo (optional bool): If set, uses sudo when deploying.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    if application_dir == '~/' or application_dir == '~' or not application_dir:
        raise ValueError('application_dir must not be equal to "{}". Check the docs.'.format(application_dir))
    if application_dir.startswith('~/'):
        application_dir = application_dir[2:]


    master_picked, workers_picked = _get_master_and_workers(reservation, master_id)
    print('Picked master node: {}'.format(master_picked))

    local_connections = connectionwrappers == None
    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': master_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrappers = get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=lambda node: _merge_kwargs(ssh_kwargs, {'User': node.extra_info['user']}), silent=silent)

    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
        _, _, exitcode = remoto.process.check(connectionwrappers[master_picked].connection, 'ls {}'.format(fs.join(loc.sparkdir(install_dir), 'bin', 'spark-submit')), shell=True)
        if exitcode != 0:
            if local_connections:
                close_wrappers(connectionwrappers)
            raise FileNotFoundError('Could not find spark-submit executable on master. Expected at: {}'.format(fs.join(loc.sparkdir(install_dir), 'bin', 'spark-submit')))

        mkdir_fun = lambda conn: remoto.process.check(conn, 'mkdir -p {}'.format(application_dir), shell=True)[2] == 0
        futures_application_mkdir = [executor.submit(mkdir_fun, connectionwrappers[x].connection) for x in reservation.nodes]
        if not all(x.result() for x in futures_application_mkdir):
            printe('Could not make directory "{}" on all nodes.'.format(application_dir))
            if local_connections:
                close_wrappers(connectionwrappers)
            return False

        if any(paths):
            paths = [fs.abspath(x) for x in paths]
            if not silent:
                print('Transferring application data...')

            if any(True for path in paths if not (fs.exists(path) or fs.issymlink(path))):
                printe('Application data transfer found non-existing source paths:')
                for path in paths:
                    if not (fs.exists(path) or fs.issymlink(path)):
                        print('    {}'.format(path))
                if local_connections:
                    close_wrappers(connectionwrappers)
                return False

            dests = [fs.join(application_dir, fs.basename(path)) for path in paths]
            rsync_global_net_fun = lambda node, conn_wrapper, path, dest: subprocess.call('rsync -e "ssh -F {}" -azL {} {}:{}'.format(conn_wrapper.ssh_config.name, path, node.ip_public, dest), shell=True) == 0
            futures_rsync = [executor.submit(rsync_global_net_fun, node, conn_wrapper, path, dest) for (path, dest) in zip(paths, dests) for (node, conn_wrapper) in connectionwrappers.items()]

            if not all(x.result() for x in futures_rsync):
                printe('Could not deploy data to all remote nodes.')
                if local_connections:
                    close_wrappers(connectionwrappers)
                return False

        if not silent:
            prints('Application data deployed.')

    if not install_dir[0] == '/' and not install_dir[0] == '~': # We deal with a relative path as installdir. This means '~/' must be appended, so we can execute this from non-home cwds.
        installdir = '~/'+install_dir
    run_cmd = '{} {}'.format(fs.join(loc.sparkdir(install_dir), 'bin', 'spark-submit'), command)
    if use_sudo:
        run_cmd = 'sudo '+run_cmd
    submit_module = _generate_module_submit()
    retval = _submit_spark(connectionwrappers[master_picked].connection, submit_module, run_cmd, application_dir, silent=silent)
    if local_connections:
        close_wrappers(connectionwrappers)
    return retval


class SubmitCommandBuilder(object):
    '''Object to assist with generating spark-submit commands.'''
    def __init__(self, cmd_type='java'):
        if cmd_type != 'java' and cmd_type != 'python':
            raise ValueError('Can only build command for types "java", "python". Found cmd_type="{}".'.format(cmd_type))
        self.cmd_type = cmd_type
        
        # Shared options
        self.master = None
        self.deploymode = 'client' if cmd_type == 'python' else 'cluster'
        self.java_options = []
        self.driver_memory = '16G'
        self.executor_memory = '16G'
        self.applicationpath = None
        self.conf_options = []
        self.args = None

        # Java-only options
        self.classname = None
        self.jars = []


    def set_master(self, spark_url):
        self.master = spark_url


    def set_deploymode(self, mode):
        ''' Set spark-submit deploymode. Accepted options are "cluster" and "client".
        In cluster mode, application is transferred to a node in the cluster, to be executed there.
        In client mode, application runs from the calling host.
        Note: Python commands must use client mode.'''
        if mode != 'client' and mode != 'cluster':
            raise ValueError('Only know of "client" and "cluster" deploymodes. Found: "{}"'.format(mode))
        if self.cmd_type == 'python' and mode == 'cluster':
            raise ValueError('Cannot set deploymode to "cluster" for Python deployments.')
        self.deploymode = mode


    def add_java_options(self, *opts):
        '''Add Spark driver Java options, e.g: `-Dlog4j.configuration=file:/path/to/log4j.conf`. Note that Java options can be set for python submits.'''
        self.java_options += list(str(x) for x in opts)


    def set_application(self, path):
        '''File containing the executable code.
        In Python, this is a Python file.
        In Java, this is a JAR containing a mainclass (specify classpath to mainclass with "set_class(class)".'''
        self.applicationpath = path


    def set_memory(self, amount):
        '''Sets Spark driver and executor memory.'
        Args:
            amount (str): JVM byte size indication to use, e.g. 4G (=4g), 400M, 16000k.'''
        if not _validate_jvm_bytes(amount):
            raise ValueError('Given value for amount ("{}") is invalid.'.format(amount))
        self.driver_memory = amount
        self.executor_memory = amount

    def set_driver_memory(self, amount):
        '''Sets Spark driver memory.'
        Args:
            amount (str): JVM byte size indication to use, e.g. 4G (=4g), 400M, 16000k.'''
        if not _validate_jvm_bytes(amount):
            raise ValueError('Given value for amount ("{}") is invalid.'.format(amount))
        self.driver_memory = amount

    def set_executor_memory(self, amount):
        '''Sets Spark executor memory.'
        Args:
            amount (str): JVM byte size indication to use, e.g. 4G (=4g), 400M, 16000k.'''
        if not _validate_jvm_bytes(amount):
            raise ValueError('Given value for amount ("{}") is invalid.'.format(amount))
        self.executor_memory = amount


    def add_conf_options(self, *opts):
        '''Add Spark configuration options. E.g: `spark.driver.extraClassPath=/extra/path`'''
        self.conf_options += list(str(x) for x in opts)


    def set_args(self, args):
        '''Set arguments for the passed application.'''
        self.args = args


    def set_class(self, classname):
        '''Set Java class. Java-only. E.g: `org.sample.somedir.Benchmark`.'''
        if self.cmd_type != 'java':
            raise RuntimeError('Cannot set Java mainclass for non-Java cmd_type="{}".'.format(self.cmd_type))
        self.classname = classname


    def add_jars(self, *jars):
        '''Add extra jars to submit alongside the main jar. Java-only.'''
        self.jars += list(str(x) for x in jars)


    def build(self):
        '''Builds the command to append to calls to spark-submit. Note: This does not contain the call to spark-submit.
        Returns:
            `str` containing command to append to calls to spark-submit.'''
        if not self.master:
            raise RuntimeError('Cannot build submit command with unset master. Provide the spark url with "set_master(spark_url)".')
        if not self.applicationpath:
            raise RuntimeError('Cannot build submit command with unset application. Specify the application (.jar or .py) file using "set_application(path)".')
        if self.cmd_type == 'java' and not self.classname:
            raise RuntimeError('Cannot build java submit command with unset classname. Specify the classname using "set_class(classname)".')
        j_opts = '--driver-java-options "{}"'.format(' '.join(self.java_options)) if self.java_options else ''
        c_opts = ' '.join('--conf {}'.format(x) for x in self.conf_options) if self.conf_options else ''

        args = self.args if self.args else ''
        cmd_base = '{} {} --master {} --deploy-mode {} --driver-memory {} --executor-memory {}'.format(j_opts, c_opts, self.master, self.deploymode, self.driver_memory, self.executor_memory)
        if self.cmd_type == 'java':
            jars = '--jars "{}"'.format(','.join(self.jars)) if self.jars else ''
            cmd_base += ' --class {} {}'.format(self.classname, jars)
        cmd_base += ' {} {}'.format(self.applicationpath, args)
        return cmd_base
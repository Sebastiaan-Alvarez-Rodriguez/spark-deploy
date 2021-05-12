import concurrent.futures

import spark_deploy.internal.defaults.install as install_defaults
import spark_deploy.internal.defaults.start as defaults
from spark_deploy.internal.remoto.modulegenerator import ModuleGenerator
from spark_deploy.internal.remoto.util import get_ssh_connection as _get_ssh_connection
import spark_deploy.internal.util.fs as fs
import spark_deploy.internal.util.location as loc
import spark_deploy.internal.util.importer as importer
from spark_deploy.internal.util.printer import *


def _start_spark_master(remote_connection, module, install_dir, host, port=7077, webui_port=2205, silent=False, retries=5):
    remote_module = remote_connection.import_module(module)
    return remote_module.start_master(loc.sparkdir(install_dir), host, port, webui_port, silent, retries)


def _start_spark_worker(remote_connection, module, install_dir, workdir, master_picked, master_port=7077, silent=False, retries=5):
    remote_module = remote_connection.import_module(module)
    return remote_module.start_worker(loc.sparkdir(install_dir), workdir, master_picked.ip_local, master_port, silent, retries)


def _generate_module_start(silent=False):
    '''Generates Spark-start module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'start_spark.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'env.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'spark_start.py'),
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


def start(reservation, install_dir=install_defaults.install_dir(), key_path=None, master_id=None, master_host=lambda x: x.ip_local, master_port=defaults.masterport(), webui_port=defaults.webuiport(), worker_workdir=defaults.workdir(), silent=False, retries=defaults.retries()):
    '''Boot Spark on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start Spark on.
        install_dir (optional str): Location on remote host where Spark (and any local-installed Java) is installed in.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        master_id (optional int): Node id that must become the master. If `None`, the node with lowest public ip value (string comparison) will be picked.
        master_host (str or function or lambda): IP/Hostname to listen to. 
            Warning: If a globally accessible ip/hostname is set (e.g. 0.0.0.0), then Spark is reachable from the public internet.
                     In such cases, make sure that the Spark `master_port` is not accessible in your firewall, so others cannot submit jobs to run on your hardware.
                     For increased privacy, also ensure `webui_port` is not accessible, so others cannot review node logs, cluster status etc.
            If str, listens to given IP/hostname.
            If function or lambda, passes the node selected as master, and requires an IP/hostname str back to listen to.
        master_port (optional int): port to use for master.
        webui_port (optional int): port for Spark webUI to use.
        worker_workdir (optional str): Path to Spark workdir location for all worker daemons.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.

    Returns:
        `(True, master_node_id)` on success, `(False, None)` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    if master_host == None:
        master_host = lambda x: x.ip_local

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        master_picked, workers_picked = _get_master_and_workers(reservation, master_id)
        printc('Picked master node: {}. Deploying 1 master and {} workers'.format(master_picked, len(workers_picked)), Color.CAN)
        
        if callable(master_host):
            master_host = master_host(master_picked)
        elif not isinstance(master_host, str):
            printe('Given master_host was not a callable function, nor a str. Instead: {} (type: {})'.format(master_host, type(master_host)))
            return False, None

        futures_connection = {x: executor.submit(_get_ssh_connection, x.ip_public, silent=silent, ssh_params=_merge_kwargs(ssh_kwargs, {'User': x.extra_info['user']})) for x in reservation.nodes}
        connectionwrappers = {node: future.result() for node, future in futures_connection.items()}

        module = _generate_module_start()

        future_spark_master = executor.submit(_start_spark_master, connectionwrappers[master_picked].connection, module, install_dir, master_host, port=master_port, webui_port=webui_port, silent=silent, retries=5)

        if not future_spark_master.result():
            printe('Could not start Spark master on node: {}'.format(master_picked))
            return False, None

        futures_spark_workers = {node: executor.submit(_start_spark_worker, conn_wrapper.connection, module, install_dir, worker_workdir, master_picked, master_port=master_port, silent=silent, retries=retries) for node, conn_wrapper in connectionwrappers.items() if node != master_picked}
        state_ok = True
        for node, worker_future in futures_spark_workers.items():
            if not worker_future.result():
                printe('Could not start Spark worker on remote: {}'.format(node))
                state_ok = False
        if state_ok:
            prints('Starting Spark on all nodes succeeded.')
            return True, master_picked.node_id
        else:
            printe('Starting Spark failed on some nodes.')
            return False, None
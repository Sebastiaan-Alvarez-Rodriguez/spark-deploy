import concurrent.futures
from multiprocessing import cpu_count
import os
import subprocess

import remoto.process

import spark_deploy.internal.defaults.install as install_defaults
import spark_deploy.internal.defaults.submit as defaults
from spark_deploy.internal.remoto.util import get_ssh_connection as _get_ssh_connection
import spark_deploy.internal.util.fs as fs
import spark_deploy.internal.util.location as loc
from spark_deploy.internal.util.printer import *



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


def clean(reservation, key_path, paths, admin_id=None, silent=False):
    '''Cleans data from the RADOS-Ceph cluster, on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        paths (list(str)): Data paths to delete to the remote cluster. Mountpoint path is always prepended.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
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

    connection = _get_ssh_connection(admin_picked.ip_public, silent=silent, ssh_params=ssh_kwargs)

    if any(paths):
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
            if not silent:
                print('Exporting data...')
            rm_futures = [executor.submit(remoto.process.check, connection.connection, 'sudo rm -rf {}'.format(fs.join(mountpoint_path, path)), shell=True) for path in paths]

            state_ok = all(x.result()[2] == 0 for x in rm_futures)

    if not any(paths):
        _, _, exitcode = remoto.process.check(connection.connection, 'sudo rm -rf {}/*'.format(mountpoint_path), shell=True)
        state_ok = exitcode == 0
    else:
        paths = [x if x[0] != '/' else x[1:] for x in paths]
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
            if not silent:
                print('Deleting data...')
            rm_futures = [executor.submit(remoto.process.check, connection.connection, 'sudo rm -rf {}'.format(fs.join(mountpoint_path, path)), shell=True) for path in paths]

            state_ok = all(x.result()[2] == 0 for x in rm_futures)

    if state_ok:
        prints('Data deleted.')
    else:
        printe('Could not delete data.')
    return state_ok



def submit(reservation, command, paths=[], install_dir=install_defaults.install_dir(), key_path=None, application_dir=defaults.application_dir(), master_id=None, silent=False):
    '''Deploy data on the RADOS-Ceph cluster, on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        command (str): Command to propagate to remote "spark-submit" executable.
        paths (optional list(str)): Data paths to offload to the remote cluster. Can be relative to CWD or absolute.
        install_dir (str): Location on remote host where Spark (and any local-installed Java) is installed in.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        application_dir (optional str): Location on remote host where we export all given 'paths' to.
        master_id (optional int): Node id of the Spark master. If `None`, the node with lowest public ip value (string comparison) will be picked.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    master_picked, workers_picked = _get_master_and_workers(reservation, master_id)
    print('Picked master node: {}'.format(master_picked))

    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': master_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    connection = _get_ssh_connection(master_picked.ip_public, silent=silent, ssh_params=ssh_kwargs)

    _, _, exitcode = remoto.process.check(connection.connection, 'ls {}'.format(fs.join(loc.sparkdir(install_dir), 'bin', 'spark-submit')), shell=True)
    if exitcode != 0:
        raise FileNotFoundError('Could not find spark-submit executable on remote. Expected at: {}'.format(fs.join(loc.sparkdir(install_dir), 'bin', 'spark-submit')))

    remoto.process.check(connection.connection, 'mkdir -p {}'.format(application_dir), shell=True)
    if any(paths):
        paths = [fs.abspath(x) for x in paths]
        if not silent:
            print('Transferring application data...')
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
            fun = lambda path: subprocess.call('rsync -e "ssh -F {}" -az {} {}:{}'.format(connection.ssh_config.name, path, master_picked.ip_public, fs.join(application_dir, fs.basename(path))), shell=True) == 0
            rsync_futures = [executor.submit(fun, path) for path in paths]

            if not all(x.result() for x in rsync_futures):
                printe('Could not deploy data.')
                return False
        if not silent:
            prints('Application data deployed.')

    if not install_dir[0] == '/' and not install_dir[0] == '~': # We deal with a relative path as installdir. This means '~/' must be appended, so we can execute this from non-home cwds.
        installdir = '~/'+install_dir
    run_cmd = '{} {}'.format(fs.join(loc.sparkdir(install_dir), 'bin', 'spark-submit'), command) # TODO: Default relative path "./deps" does not work here below, when changing the cwd away from $HOME!!!
    print('Executing:\n{}'.format(run_cmd))
    out, err, exitcode = remoto.process.check(connection.connection, run_cmd, shell=True, cwd=application_dir)

    if exitcode == 0:
        prints('Application submission succeeded.')
    else:
        printe('Could not submit application: {}'.format(err))
    return exitcode == 0
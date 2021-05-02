import concurrent.futures

from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.remoto.modules.spark_start as _spark_start
import internal.util.location as loc
from internal.util.printer import *


def _default_workdir():
    return '~/spark_workdir'

def _default_retries():
    return 5


def _start_spark_master(remote_connection, installdir, port=7077, webui_port=2205, silent=False, retries=5):
    remote_connection.import_module(_spark_start)
    return remote_connection.boot_master(loc.sparkdir(installdir), port=port, webui_port=webui_port, silent=silent, retries=retries)


def _start_spark_slave(remote_connection, installdir, workdir, master_picked, master_port=7077, silent=False, retries=5):
    remote_connection.import_module(_spark_start)
    return remote_connection.boot_slave(loc.sparkdir(installdir), workdir, master_picked.ip_local, master_port, silent, retries)


def _get_master_and_slaves(reservation, master_id=None):
    '''Divides nodes in 1 master and a list of slaves.
    Args:
        reservation (`metareserve.Reservation`): Nodes to divide into a master + slaves.
        master_id (optional int): If set, node with given ID will be master. If `None`, node with lowest public ip string value will be master.
    
    Returns:
        1 master, and a list of slaves.'''
    if len(reservation) == 1:
        return next(reservation.nodes), []

    if master == None: # Pick node with lowest public ip value.
        tmp = sorted(reservation.nodes, key=lambda x: x.ip_public)
        return tmp[0], tmp[1:]
    return reservation.get_node(node_id=master_id), [x for x in reservation.nodes if x.node_id != master_id]


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def start(reservation, installdir, key_path, master_id=None, slave_workdir=_default_workdir(), silent=False, retries=_default_retries()):
    '''Boot Spark on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start Spark on.
        installdir (str): Location on remote host where Spark (and any local-installed Java) is installed in.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        master_id (optional int): Node id that must become the master. If `None`, the node with lowest public ip value (string comparison) will be picked.
        slave_workdir (optional str): Path to Spark workdir location for all slave daemons.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        master_picked, slaves_picked = _get_master_and_slaves(reservation, master_id)
        print('Picked master node: {}'.format(master_picked))

        futures_connection = {x: executor.submit(_get_ssh_connection, x.ip_public, silent=silent, ssh_params=_merge_kwargs(ssh_kwargs, 'User': x.extra_info['user'])) for x in reservation.nodes}
        connectionwrappers = {node: future.result() for node, future in futures_connection.items()}

        future_spark_master = executor.submit(_start_spark_master, connectionwrappers[master_picked].connection, installdir, port=master_port, webui_port=webui_port, silent=False, retries=5)
        if not future_spark_master.result():
            printe('Could not start Spark master on node: {}'.format(master_picked))
            return False

        futures_spark_slaves = {node: executor.submit(_start_spark_slave, conn_wrapper.connection, installdir, slave_workdir, master_picked, master_port=master_port, silent=silent, retries=retries): x for node, conn_wrapper in connectionwrappers.items()}
        state_ok = True
        for node, slave_future in futures_start_spark.items():
            if not slave_future.result():
                printe('Could not start Spark slave on remote: {}'.format(node))
                state_ok = False
        return state_ok
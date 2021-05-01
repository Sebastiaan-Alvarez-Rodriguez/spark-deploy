import concurrent.futures
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.remoto.modules.spark_install as _spark_install
import internal.remoto.modules.spark_boot as _spark_boot
from internal.util.printer import *


def _boot_spark(remote_connection, installdir, boot_slave=True, boot_master=False, retries=5):
    if boot_slave and boot_master:
        raise ValueError('Cannot launch both slave and master daemon on 1 node! Spark supports only 1 deamon per node.')
    remote_connection.import_module(_spark_boot)
    if boot_master:
        return remote_connection.boot_master(installdir, retries)
    if boot_slave:
        return remote_connection.boot_slave(installdir, retries)
    return True # We did not have to do anything


def _pick_master(reservation, master_id):
    if master == None: # Pick node with lowest public ip value.
        return sorted(reservation.nodes, key=lambda x: x.ip_public)[0]
    return reservation.get_node(node_id=master_id)


def start(reservation, installdir, key_path, master):
    '''Boot Spark on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start Spark on.
        installdir (str): Location on remote host where Spark is installed.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        master (int): Node id that must become the master. If `None`, the node with lowest public ip value (string comparison) will be picked.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': x.extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        master_picked = _pick_master(reservation, master)

        print('Picked master node: {}'.format(master_picked))
        futures_connection = {executor.submit(_get_ssh_connection, x.ip_public, silent=False, ssh_params=ssh_kwargs): x for x in reservation.nodes}
        connectionwrappers = {key.result(): val for key, val in futures_connection.items()}

        futures_start_spark = {executor.submit(_boot_spark, key.connection, installdir, boot_slave=val!=master_picked, boot_master=val==master_picked): x for key, val in connectionwrappers.items()}
        state_ok = True
        for key, val in futures_start_spark.items():
            if not key.result():
                printe('Could not start Spark on remote {}!'.format(val.connection.hostname))
                state_ok = False
        if not state_ok:
            return False
    return True
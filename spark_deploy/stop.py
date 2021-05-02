import concurrent.futures

from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.remoto.modules.spark_stop as _spark_stop
import internal.util.location as loc
from internal.util.printer import *


def _default_workdir():
    return '~/spark_workdir'


def _default_retries():
    return 5


def _stop_spark(remote_connection, installdir, workdir=None, silent=False, retries=5):
    remote_connection.import_module(_spark_stop)
    return remote_connection.stop_all(loc.sparkdir(installdir), workdir, silent, retries)


def stop(reservation, installdir, key_path, slave_workdir=_default_workdir(), silent=False, retries=5):
    '''Stop Spark on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start Spark on.
        installdir (str): Location on remote host where Spark (and any local-installed Java) is installed in.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        slave_workdir (optional str): Path to Spark workdir location for all slave daemons.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': x.extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path


        futures_connection = {x: executor.submit(_get_ssh_connection, x.ip_public, silent=silent, ssh_params=ssh_kwargs) for x in reservation.nodes}
        connectionwrappers = {node: future.result() for node, future in futures_connection.items()}

        futures_spark_stop = {node: executor.submit(_stop_spark, conn_wrapper.connection, installdir, workdir=slave_workdir, silent=silent, retries=retries): x for node, conn_wrapper in connectionwrappers.items()}
        state_ok = True
        for node, slave_future in futures_spark_stop.items():
            if not key.result():
                printe('Could not stop Spark slave on remote: {}'.format(node))
                state_ok = False
        return state_ok
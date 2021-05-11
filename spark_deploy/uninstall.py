import concurrent.futures

import remoto.process

from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.util.location as loc
from internal.util.printer import *


def _uninstall_spark(connection, install_dir):
    remoto.process.run(connection, ['rm', '-rf', loc.sparkdir(install_dir)])


def _uninstall_java(connection, install_dir):
    remoto.process.run(connection, ['rm', '-rf', loc.java_nonroot_dir(install_dir)])


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def uninstall(reservation, install_dir, key_path):
    '''Uninstall Spark and Java from a reserved cluster.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to remove Spark, Java from.
        install_dir (str): Location on remote host where Spark and dependencies are installed.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.

    Raises:
        Valuerror: When reservation contains 0 nodes or is `None`.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        else:
            printw('Connections have no assigned ssh key. Prepare to fill in your password often.')
        futures_connection = [executor.submit(_get_ssh_connection, x.ip_public, silent=True, ssh_params=_merge_kwargs(ssh_kwargs, {'User': x.extra_info['user']})) for x in reservation.nodes]
        connectionwrappers = [x.result() for x in futures_connection]

        if any(x for x in connectionwrappers if not x):
            printe('Could not connect to some nodes.')
            return False

        futures_uninstall = [executor.submit(_uninstall_spark, x.connection, install_dir) for x in connectionwrappers]
        futures_uninstall+= [executor.submit(_uninstall_java, x.connection, install_dir) for x in connectionwrappers]

        results = [x.result() for x in futures_uninstall]
        prints('Clean successful.')
        return True
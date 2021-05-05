import concurrent.futures

from internal.remoto.modulegenerator import ModuleGenerator
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.util.fs as fs
import internal.util.location as loc
import internal.util.importer as importer
from internal.util.printer import *


def _default_workdir():
    return './spark_workdir'


def _default_retries():
    return 5


def _stop_spark(remote_connection, module, installdir, workdir=None, silent=False, retries=5):
    remote_module = remote_connection.import_module(module)
    return remote_module.stop_all(loc.sparkdir(installdir), workdir, silent, retries)


def _generate_module_stop(silent=False):
    '''Generates Spark-stop module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'start_spark.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'spark_stop.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    ModuleGenerator().with_module(fs).with_files(*files).generate(generation_loc, silent)
    return importer.import_full_path(generation_loc)


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


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
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        futures_connection = {x: executor.submit(_get_ssh_connection, x.ip_public, silent=silent, ssh_params=_merge_kwargs(ssh_kwargs, {'User': x.extra_info['user']})) for x in reservation.nodes}
        connectionwrappers = {node: future.result() for node, future in futures_connection.items()}

        stop_module = _generate_module_stop()

        futures_spark_stop = {node: executor.submit(_stop_spark, conn_wrapper.connection, stop_module, installdir, workdir=slave_workdir, silent=silent, retries=retries) for node, conn_wrapper in connectionwrappers.items()}
        state_ok = True
        for node, slave_future in futures_spark_stop.items():
            if not slave_future.result():
                printe('Could not stop Spark slave on remote: {}'.format(node))
                state_ok = False
        if state_ok:
            prints('Stopping Spark on all nodes succeeded.')
            return True
        else:
            printe('Stopping Spark failed on some nodes.')
            return False
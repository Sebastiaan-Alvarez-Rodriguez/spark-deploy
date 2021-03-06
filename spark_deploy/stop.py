import concurrent.futures


import spark_deploy.internal.defaults.install as install_defaults
import spark_deploy.internal.defaults.start as start_defaults
import spark_deploy.internal.defaults.stop as defaults
from spark_deploy.internal.remoto.modulegenerator import ModuleGenerator
from spark_deploy.internal.remoto.ssh_wrapper import get_wrappers, close_wrappers
import spark_deploy.internal.util.fs as fs
import spark_deploy.internal.util.importer as importer
import spark_deploy.internal.util.location as loc
from spark_deploy.internal.util.printer import *


def _stop_spark(remote_connection, module, install_dir, workdir=None, use_sudo=False, silent=False, retries=5):
    remote_module = remote_connection.import_module(module)
    return remote_module.stop_all(loc.sparkdir(install_dir), workdir, use_sudo, silent, retries)


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


def stop(reservation, install_dir=install_defaults.install_dir(), key_path=None, connectionwrappers=None, worker_workdir=start_defaults.workdir(), use_sudo=False, silent=False, retries=defaults.retries()):
    '''Stop Spark on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start Spark on.
        install_dir (optional str): Location on remote host where Spark (and any local-installed Java) is installed in.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        connectionwrappers (optional dict(metareserve.Node, RemotoSSHWrapper)): If set, uses provided connections instead of making new ones.
        worker_workdir (optional str): Path to Spark workdir location for all worker daemons.
        use_sudo (optional bool): If set, uses sudo when stopping.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    local_connections = connectionwrappers == None
    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrappers = get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=lambda node: _merge_kwargs(ssh_kwargs, {'User': node.extra_info['user']}), silent=silent)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        stop_module = _generate_module_stop()

        futures_spark_stop = {node: executor.submit(_stop_spark, conn_wrapper.connection, stop_module, install_dir, workdir=worker_workdir, use_sudo=use_sudo, silent=silent, retries=retries) for node, conn_wrapper in connectionwrappers.items()}
        state_ok = True
        for node, worker_future in futures_spark_stop.items():
            if not worker_future.result():
                printe('Could not stop Spark worker on remote: {}'.format(node))
                state_ok = False

        if local_connections:
            close_wrappers(connectionwrappers)  

        if state_ok:
            prints('Stopping Spark on all nodes succeeded.')
            return True
        else:
            printe('Stopping Spark failed on some nodes.')
            return False
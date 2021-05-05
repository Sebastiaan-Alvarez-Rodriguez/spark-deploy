import concurrent.futures

from internal.remoto.modulegenerator import ModuleGenerator
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.util.fs as fs
import internal.util.location as loc
import internal.util.importer as importer
from internal.util.printer import *

def _default_spark_url():
    return 'https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz'

def _default_java_url():
    return 'https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz'

def _default_java_min():
    return 11

def _default_java_max():
    return 0

def _default_retries():
    return 5

def _default_use_sudo():
    return False


def _install_spark(connection, spark_module, installdir, spark_url, silent=False, retries=5):
    remote_module = connection.import_module(spark_module)
    return remote_module.spark_install(loc.sparkdir(installdir), spark_url, silent, retries)


def _install_java(connection, java_module, installdir, java_url, java_min, java_max, use_sudo, silent=False, retries=5):
    remote_module = connection.import_module(java_module)
    if use_sudo:
        return remote_module.java_install_sudo(java_min, java_max, silent, retries)
    else:
        return remote_module.java_install_nonsudo(loc.java_nonroot_dir(installdir), java_url, java_min, java_max, silent, retries)


def _generate_module_spark(silent=False):
    '''Generates Spark-install module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'install_spark.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'spark_install.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    ModuleGenerator().with_module(fs).with_files(*files).generate(generation_loc, silent)
    return importer.import_full_path(generation_loc)


def generate_module_java(silent=False):    
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'install_java.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'env.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'java_install.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    ModuleGenerator().with_module(fs).with_files(*files).generate(generation_loc, silent)
    return importer.import_full_path(generation_loc)


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def install(reservation, installdir, key_path, spark_url=_default_spark_url(), java_url=_default_java_url(), java_min=_default_java_min(), java_max=_default_java_max(), use_sudo=_default_use_sudo(), silent=False, retries=_default_retries()):
    '''Install Spark and Java 11 on a reserved cluster. Does not reinstall if already present.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to install Spark on.
        installdir (str): Location on remote host to install Spark in.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        spark_url (optional str): URL to download Spark.
        java_url (optional str): URL to download Java.
        java_min (optional int): Minimal Java version to accept. 0 means no limit.
        java_max (optional int): Maximal Java version to accept. 0 means no limit.
        use_sudo (optional bool): If set, installs some libraries system-wide. Otherwise, performs local installation.
        silent (optional bool): If set, we only print errors and critical info.
        retries (optional int): Number of tries we try to download archives.

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
        futures_connection = [executor.submit(_get_ssh_connection, x.ip_public, silent=silent, ssh_params=_merge_kwargs(ssh_kwargs, {'User': x.extra_info['user']})) for x in reservation.nodes]
        connectionwrappers = [x.result() for x in futures_connection]

        if any(x for x in connectionwrappers if not x):
            printe('Could not connect to some nodes.')
            return False

        spark_module = _generate_module_spark()
        java_module = generate_module_java()

        futures_install_spark = {executor.submit(_install_spark, x.connection, spark_module, installdir, spark_url, silent=silent, retries=retries): x for x in connectionwrappers}
        futures_install_java = {executor.submit(_install_java, x.connection, java_module, installdir, java_url, java_min, java_max, use_sudo, silent=silent, retries=retries): x for x in connectionwrappers}

        state_ok = True
        for key, val in futures_install_spark.items():
            if not key.result():
                printe('Could not install Spark on remote {}!'.format(val.connection.hostname))
                state_ok = False
        for key, val in futures_install_java.items():
            if not key.result():
                printe('Could not install java on remote {}!'.format(val.connection.hostname))
                state_ok = False
        if state_ok:
            prints('Installation on all nodes succeeded.')
            return True
        else:
            printe('Installation failed on some nodes.')
            return False
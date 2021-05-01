import concurrent.futures
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.remoto.modules.spark_install as _spark_install
import internal.remoto.modules.java_install as _java_install
from internal.util.printer import *
import internal.util.location as loc

def _default_spark_url():
    return 'https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz'

def _default_java_url():
    return 'https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz'

def _default_java_min():
    return 11

def _default_java_max():
    return 0


def _install_spark(connection, installdir, spark_url, retries=5):
    remote_module = connection.import_module(_spark_install)
    return remote_module.install(loc.sparkdir(installdir), spark_url, retries)


def _install_java(connection, installdir, java_url, retries=5):
    remote_module = connection.import_module(_java_install)
    return remote_module.install(loc.java_nonroot_dir(installdir), spark_url, retries)



def install(reservation, installdir, key_path, spark_url=_default_spark_url(), java_url, java_min, java_max):
    '''Install Spark and Java 11 on a reserved cluster. Does not reinstall if already present.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to install Spark on.
        installdir (str): Location on remote host to install Spark in.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        spark_url (str): URL to download Spark.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': x.extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        futures_connection = [executor.submit(_get_ssh_connection, x.ip_public, silent=False, ssh_params=ssh_kwargs) for x in reservation.nodes]
        connectionwrappers = [x.result() for x in futures_connection]

        futures_install_spark = {executor.submit(_install_spark, x.connection, installdir, spark_url): x for x in connectionwrappers}
        state_ok = True
        for key, val in futures_install_spark.items():
            if not key.result():
                printe('Could not install Spark on remote {}!'.format(val.connection.hostname))
                state_ok = False
        if state_ok:
            prints('Installation on all nodes succeeded.')
            return True
        else:
            printe('Installation failed on some nodes.')
            return False
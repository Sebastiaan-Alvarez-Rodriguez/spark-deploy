import concurrent.futures
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.remoto.modules.spark_install as _spark_install
from internal.util.printer import *

def _install_spark(connection, spark_installdir, spark_url, retries=5):
    remote_module = connection.import_module(_spark_install)
    return remote_module.install(spark_installdir, spark_url, retries)


def install(reservation, spark_installdir='/users/Sebas/spark/', spark_url='https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz'):
    '''Install Spark and Java 11 on a reserved cluster. Does not reinstall if already present.
    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        futures_connection = [executor.submit(_get_ssh_connection, x.ip_public, silent=False, ssh_params={'IdentityFile': '~/.ssh/geni.rsa', 'IdentitiesOnly': 'yes', 'User': x.extra_info['user'], 'StrictHostKeyChecking': 'no'}) for x in reservation.nodes]
        connectionwrappers = [x.result() for x in futures_connection]

        futures_install_spark = {executor.submit(_install_spark, x.connection, spark_installdir, spark_url): x for x in connectionwrappers}
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
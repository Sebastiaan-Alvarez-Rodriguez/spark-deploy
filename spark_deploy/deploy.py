import concurrent.futures
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.remoto.modules.spark_install as _spark_install

def _install_spark(remote_connection, spark_installdir, retries=5)
    remote_connection.import_module(_spark_install)
    return remote_connection.install(spark_installdir, retries)


def _boot_spark(remote_connection, spark_installdir, retries=5):
    remote_connection.import_module(_spark_boot)
    return remote_connection.boot(spark_installdir, retries)


def start(reservation, spark_installdir='~/spark/'):
    '''Deploy Spark on an existing reservation.
    Returns:
        `True` on success, `False` otherwise.'''
    connections = [_get_ssh_connection(x.ip_public, silent=True) for x in reservation.nodes]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(_install_spark, x, spark_installdir): x for x in connections}
        state_ok = True
        for key, val in futures:
            if not key.result():
                printe('Could not install Spark on remote {} (ip={}, port={})!'.format(val.hostname, val.ip_public, val.port))
                state_ok = False
    if not state_ok:
        return False
    return True
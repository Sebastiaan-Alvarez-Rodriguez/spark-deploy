import logging
import remoto


def quiet_logger(name='SilentLogger'):
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(logging.ERROR)
    return logger


def get_ssh_connection(remote_hostname, silent=True):
    if silent:
        return remoto.Connection(remote_hostname, logger=quiet_logger)
    return remoto.Connection(remote_hostname)
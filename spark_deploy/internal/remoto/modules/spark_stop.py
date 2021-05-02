import os
import socket
import subprocess
import time

from internal.util.printer import *
import internal.util.fs as fs


'''Code in this file boots up Spark instances.'''

def stderr(string, *args, **kwargs):
    kwargs['flush'] = True
    kwargs['file'] = sys.stderr
    print('[{}] {}'.format(socket.gethostname(), string), *args, **kwargs)


def _terminate_daemons(sparkloc, silent, retries, retries_sleep):
    if not os.path.isfile(sparkloc):
        stderr('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False

    scriptloc = os.path.join(sparkloc, 'stop-all.sh')
    if not os.path.isfile(scriptloc):
        stderr('Could not find file at {}. Did Spark not install successfully?'.format(scriptloc))
        return False

    cmd = scriptloc
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {} 
    for x in range(retries):
        if subprocess.call(cmd, shell=True, **kwargs) == 0:
            stderr('All daemons terminated.')
            return True
        time.sleep(retries_sleep)
    stderr('Could not terminate all daemons!')
    return False


def stop_all(sparkloc, workdir=None, silent=False, retries=5, retries_sleep=5):
    '''Stops all Spark daemons on current node. Cleans up workdir location too, if `workdir` given.

    Args:
        sparkloc (str): Location in which Spark is installed.
        workdir (optional str): Workdir location. If set, deletes given location.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.
        retries_sleep (optional int): Number of seconds we sleep between tries.

    Returns:
        `True` on success, `False` otherwise.'''


    if not silent:
        stderr('Terminating daemons.')
    if not _terminate_daemons(sparkloc, silent, retries, retries_sleep):
        return False

    if workdir:
        if not silent:
            stderr('Cleaning workdir.')
        fs.rm(workdir, ignore_errors=True)

    return True



if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
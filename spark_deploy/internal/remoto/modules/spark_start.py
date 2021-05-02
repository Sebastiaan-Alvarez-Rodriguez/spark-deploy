import os
import socket
import subprocess
import time

from internal.util.printer import *


'''Code in this file boots up Spark instances.'''

def stderr(string, *args, **kwargs):
    kwargs['flush'] = True
    kwargs['file'] = sys.stderr
    print('[{}] {}'.format(socket.gethostname(), string), *args, **kwargs)


def start_master(sparkloc, port=7077, webui_port=2205, silent=False, retries=5, retries_sleep=5):
    '''Boots master on given node.
    Note: Spark works with Daemons, so expect to return quickly, probably even before the slave is actually ready.

    Args:
        sparkloc (str): Location in which Spark is installed.
        port (optional int): port to use for master.
        webui_port (optional int): port for Spark webUI to use.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.
        retries_sleep (optional int): Number of seconds we sleep between tries.

    Returns:
        `True` on success, `False` otherwise.'''
    if not os.path.isfile(sparkloc):
        stderr('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False

    scriptloc = os.path.join(sparkloc, 'start-master.sh')
    if not os.path.isfile(scriptloc):
        stderr('Could not find file at {}. Did Spark not install successfully?'.format(scriptloc))
        return False

    if not silent:
        stderr('Spawning master')

    cmd = '{} --host {} --port {} --webui-port {}'.format(scriptloc, node, port, webui_port)
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {} 
    for x in range(retries):
        if subprocess.call(cmd, shell=True, **kwargs) == 0:
            printc('[{}] MASTER ready on spark://{}:{} (webui-port: {})'.format(socket.gethostname(), node, port, webui_port), Color.CAN, file=sys.stderr)
            return True
        time.sleep(retries_sleep)
    stderr('Could not boot master')
    return False


def start_slave(sparkloc, workdir, master_node, master_port=7077, silent=True, retries=5, retries_sleep=5):
    '''Boots a slave.
    Note: Spark works with Daemons, so expect to return quickly, probably even before the slave is actually ready.

    Args:
        sparkloc (str): Location in which Spark is installed.
        workdir (str): Location where Spark workdir must be created.
        master_node (str): ip/hostname of master.
        master_port (optional int): port of master.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.
        retries_sleep (optional int): Number of seconds we sleep between tries.

    Returns:
        `True` on success, `False` otherwise.'''
    if not os.path.isfile(sparkloc):
        stderr('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False

    scriptloc = os.path.join(sparkloc, 'start-slave.sh')
    if not os.path.isfile(scriptloc):
        stderr('Could not find file at {}. Did Spark not install successfully?'.format(scriptloc))
        return False

    master_url = 'spark://{}:{}'.format(master_node, master_port)

    # workdir = fs.join(loc.get_node_local_dir(), lid)
    # fs.rm(workdir, ignore_errors=True)
    # fs.mkdir(workdir)
    if not silent:
        stderr('Spawning slave')

    cmd = '{} {} --work-dir {} {}"'.format(scriptloc, master_url, workdir, '> /dev/null 2>&1' if not debug_mode else '')
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {} 
    
    for x in range(retries):
        if subprocess.call(cmd, shell=True, **kwargs) == 0:
            return True
        time.sleep(retries_sleep)
    stderr('Could not boot slave (failed {} times, {} sleeptime between executions)'.format(retries, retries_sleep))
    return False


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
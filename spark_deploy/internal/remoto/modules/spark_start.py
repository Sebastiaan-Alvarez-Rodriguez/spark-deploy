import builtins
from enum import Enum
import os
import socket
import subprocess
import sys
import time


##########################################################################################
# Here, we copied the contents from internal.util.printer (as we cannot use local imports)
def print(string, *args, **kwargs):
    kwargs['flush'] = True
    kwargs['file'] = sys.stderr  # Print everything to stderr!
    return builtins.print('[{}] {}'.format(socket.gethostname(), string), *args, **kwargs)


class Color(Enum):
    '''An enum to specify what color you want your text to be'''
    RED = '\033[1;31m'
    GRN = '\033[1;32m'
    YEL = '\033[1;33m'
    BLU = '\033[1;34m'
    PRP = '\033[1;35m'
    CAN = '\033[1;36m'
    CLR = '\033[0m'

# Print given text with given color
def printc(string, color, **kwargs):
    print(format(string, color), **kwargs)

# Print given success text
def prints(string, color=Color.GRN, **kwargs):
    print('[SUCCESS] {}'.format(format(string, color)), **kwargs)

# Print given warning text
def printw(string, color=Color.YEL, **kwargs):
    print('[WARNING] {}'.format(format(string, color)), **kwargs)


# Print given error text
def printe(string, color=Color.RED, **kwargs):
    print('[ERROR] {}'.format(format(string, color)), **kwargs)


# Format a string with a color
def format(string, color):
    if os.name == 'posix':
        return '{}{}{}'.format(color.value, string, Color.CLR.value)
    return string

##########################################################################################


def java_home_available():
    return java_home() != None


def java_home():
    return os.getenv('JAVA_HOME')


def start_master(sparkloc, host, port=7077, webui_port=8080, silent=False, retries=5, retries_sleep=5):
    '''Boots master on given node.
    Note: Spark works with Daemons, so expect to return quickly, probably even before the slave is actually ready.

    Args:
        sparkloc (str): Location in which Spark is installed.
        host (str): IP/Hostname to listen to. 
                    Warning: If a globally accessible ip/hostname is set (e.g. 0.0.0.0), then Spark is reachable from the public internet.
                             In such cases, make sure that the Spark `port` is not accessible in your firewall, so others cannot post jobs.
                             For increased privacy, also ensure `webui_port` is not accessible, so others cannot review node logs, cluster status etc.
        port (optional int): port to use for master.
        webui_port (optional int): port for Spark webUI to use.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.
        retries_sleep (optional int): Number of seconds we sleep between tries.

    Returns:
        `True` on success, `False` otherwise.'''
    if not os.path.isdir(sparkloc):
        printe('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False

    scriptloc = os.path.join(sparkloc, 'sbin', 'start-master.sh')
    if not os.path.isfile(scriptloc):
        printe('Could not find file at {}. Did Spark not install successfully?'.format(scriptloc))
        return False

    if not java_home_available():
        printe('JAVA_HOME not found in the current environment.')
        return False

    if not silent:
        print('Spawning master, using hostname {}...'.format(host))

    cmd = '{} --host {} --port {} --webui-port {} 1>&2'.format(scriptloc, host, port, webui_port)
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {} 
    for x in range(retries):
        if subprocess.call(cmd, shell=True, **kwargs) == 0:
            printc('MASTER ready on spark://{}:{} (webui-port: {})'.format(host, port, webui_port), Color.CAN, file=sys.stderr)
            return True
        if x == 0:
            printw('Could not boot master. Retrying...')
        time.sleep(retries_sleep)
    printe('Could not boot master.')
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
    if not os.path.isdir(sparkloc):
        printe('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False

    scriptloc = os.path.join(sparkloc, 'sbin', 'start-worker.sh')
    if not os.path.isfile(scriptloc):
        printe('Could not find file at "{}". Did Spark not install successfully?'.format(scriptloc))
        return False

    if not java_home_available():
        printe('JAVA_HOME not found in the current environment.')
        return False

    master_url = 'spark://{}:{}'.format(master_node, master_port)

    if not silent:
        print('Spawning slave')

    cmd = '{} {} --work-dir {} {} 1>&2'.format(scriptloc, master_url, workdir, '> /dev/null 2>&1' if silent else '')
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {} 
    
    for x in range(retries):
        if subprocess.call(cmd, shell=True, **kwargs) == 0:
            return True
        if x == 0:
            printw('Could not boot slave. retrying...')
        time.sleep(retries_sleep)
    printe('Could not boot slave (failed {} times, {} sleeptime between executions)'.format(retries, retries_sleep))
    return False


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
import builtins
from enum import Enum
import os
import shutil
import socket
import subprocess
import sys
import time

'''Code in this file boots up Spark instances.'''


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


def _rm(directory, *args, ignore_errors=False):
    path = os.path.join(directory, *args)
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=ignore_errors)
    else:
        if ignore_errors:
            try:
                os.remove(path)
            except Exception as e:
                pass
        else:
            os.remove(path)


def _terminate_daemons(sparkloc, silent, retries, retries_sleep):
    if not os.path.isdir(sparkloc):
        printe('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False

    scriptloc = os.path.join(sparkloc, 'sbin', 'stop-all.sh')
    if not os.path.isfile(scriptloc):
        printe('Could not find file at {}. Did Spark not install successfully?'.format(scriptloc))
        return False

    cmd = scriptloc
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {} 
    for x in range(retries):
        if subprocess.call(cmd, shell=True, **kwargs) == 0:
            return True
        time.sleep(retries_sleep)
    printe('Could not terminate all daemons!')
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
        print('Terminating daemons...')
    if not _terminate_daemons(sparkloc, silent, retries, retries_sleep):
        return False

    if workdir:
        if not silent:
            print('Cleaning workdir...')
        _rm(workdir, ignore_errors=True)
    if not silent:
        prints('All daemons terminated.')
    return True



if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
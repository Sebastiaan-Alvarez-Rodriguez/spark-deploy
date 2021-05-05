import re
import subprocess
import sys
import time

'''Code in this file boots up Spark instances.'''



def _terminate_daemon(scriptloc, silent, retries, retries_sleep):
    if not isfile(scriptloc):
        printe('Could not find file at {}. Did Spark not install successfully?'.format(scriptloc))
        return False

    cmd = 'bash {} 1>&2'.format(scriptloc)
    for x in range(retries):
        try:
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode('utf-8').strip()
            return True
        except subprocess.CalledProcessError as e:
            if re.search('no .* to stop', e.output): # There is no real problem. The node just does not currently run the daemon, which is fine.
                return True
            if x == 0:
                printw('Could not execute {}: {}'.format(scriptloc, e))
        time.sleep(retries_sleep)
    return False


def _terminate_daemons(sparkloc, silent, retries, retries_sleep):
    if not isdir(sparkloc):
        printe('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False
    return all(_terminate_daemon(x, silent, retries, retries_sleep) for x in [join(sparkloc, 'sbin', 'stop-worker.sh'), join(sparkloc, 'sbin', 'stop-master.sh')])


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
        rm(workdir, ignore_errors=True)
    if not silent:
        prints('All daemons terminated.')
    return True
import os
import re
import subprocess
import sys
import time


'''Code in this file stops all running Spark daemons.'''


def _terminate_daemon(scriptloc, use_sudo, silent, retries, retries_sleep):
    if not isfile(scriptloc):
        printw('Could not find file at "{}". Did Spark not install successfully?'.format(scriptloc))
        return False

    cmd = 'bash {} 1>&2'.format(scriptloc)
    if use_sudo:
        cmd ='sudo '+cmd
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


def _terminate_daemons(sparkloc, use_sudo, silent, retries, retries_sleep):
    if not isdir(sparkloc):
        printw('Could not find Spark installation at {}. We presume no daemons are running.'.format(sparkloc))
        return True
    scripts = [join(sparkloc, 'sbin', 'stop-master.sh')]
    if isfile(join(sparkloc, 'sbin', 'stop-worker.sh')): # We run Spark 3.1.1 or newer.
        scripts.append(join(sparkloc, 'sbin', 'stop-worker.sh'))
    elif isfile(join(sparkloc, 'sbin', 'stop-slave.sh')): # We run Spark 3.0.2 or older.
        scripts.append(join(sparkloc, 'sbin', 'stop-slave.sh'))
    else:
        printw('Could not find script at "{}", nor at "{}". Did Spark not install successfully?'.format(join(sparkloc, 'sbin', 'stop-worker.sh'), join(sparkloc, 'sbin', 'stop-slave.sh')))
        return False
    return all(_terminate_daemon(x, use_sudo, silent, retries, retries_sleep) for x in scripts)


def stop_all(sparkloc, workdir=None, use_sudo=False, silent=False, retries=5, retries_sleep=5):
    '''Stops all Spark daemons on current node. Cleans up workdir location too, if `workdir` given.
    Args:
        sparkloc (str): Location in which Spark is installed.
        workdir (optional str): Workdir location. If set, deletes given location.
        use_sudo (optional bool): If set, uses sudo when stopping.
        silent (optional bool): If set, we only print errors and critical info (e.g. spark master url). Otherwise, more verbose output.
        retries (optional int): Number of tries we try to connect to the master.
        retries_sleep (optional int): Number of seconds we sleep between tries.

    Returns:
        `True` on success, `False` otherwise.'''
    sparkloc = os.path.expanduser(sparkloc)
    workdir = os.path.expanduser(workdir) if workdir else None
    if not silent:
        print('Terminating daemons...')
    if not _terminate_daemons(sparkloc, use_sudo, silent, retries, retries_sleep):
        return False

    if workdir:
        if not silent:
            print('Cleaning workdir...')
        rm(workdir, ignore_errors=True)
    if not silent:
        prints('All daemons terminated.')
    return True
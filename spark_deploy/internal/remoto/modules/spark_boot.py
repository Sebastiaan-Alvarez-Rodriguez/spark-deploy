# Code in this file boots up a Spark cluster

import socket
import subprocess

import os
from internal.util.printer import *


def boot_master(sparkloc, port=7077, webui_port=2205, silent=False):
    '''Boots master on given node.
    Note: Spark works with Daemons, so expect to return quickly, probably even before the master is actually ready.'''

    scriptloc = os.path.join(sparkloc, 'start-master.sh')

    if not silent:
        print('Spawning master on host {}'.format(socket.gethostname()))

    cmd = '{} --host {} --port {} --webui-port {} {}'.format(scriptloc, node, port, webui_port, '> /dev/null 2>&1' if not debug_mode else '')
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {} 
    state_ok = subprocess.call(cmd, shell=True, **kwargs) == 0

    if not silent:
        if state_ok:
            printc('MASTER ready on spark://{}:{} (webui-port: {})'.format(node, port, webui_port), Color.CAN)
        else:
            printe('Could not boot master on {}'.format(socket.gethostname()))
    return state_ok


def boot_slave(sparkloc, workdir, master_node, master_port=7077, silent=True):
    '''Boots a slave.
    Note: Spark works with Daemons, so expect to return quickly, probably even before the slave is actually ready.

    Provide
        node: ip/hostname of node to boot this worker
        master_node: ip/hostname of master
        master_port: port for master
        procs_per_node: Amount of slaves to spawn on this node
        debug_mode: True if we must print debug info, False otherwiseexecute
        execute: Function executes boot command and returns state_ok (a bool) if True, otherwise returns Executor to boot slave, which you have to execute to deploy slave.'''
    scriptloc = os.path.join(sparkloc, 'start-slave.sh')
    master_url = 'spark://{}:{}'.format(master_node, master_port)

    # workdir = fs.join(loc.get_node_local_dir(), lid)
    # fs.rm(workdir, ignore_errors=True)
    # fs.mkdir(workdir)
    if not silent:
        print('Spawning slave on host {}'.format(socket.gethostname()))

    cmd = '{} {} --work-dir {} {}"'.format(scriptloc, master_url, loc.get_spark_work_dir(deploy_mode), '> /dev/null 2>&1' if not debug_mode else '')
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {} 
    return subprocess.call(cmd, shell=True, **kwargs) == 0


# def boot_slaves(nodes, master_node, master_port=7077, debug_mode=False):
#     '''Boots multiple slaves in parallel.
#     Args:
#         nodes: ip/hostname list of nodes to boot workers on.
#         master_node: ip/hostname of master.
#         master_port: port for master.
#         debug_mode: True if we must print debug info, False otherwise.
#         deploymode: Denotes where we locate the slave work-dir: E.g. on NFS-mount for debugging (slow), on local disk for each node, etc.

#     Returns:
#         `True` on success, `False` otherwise.'''
#     executors = []
#     for node in nodes:
#         executors.append(boot_slave(node, master_node, master_port=master_port, deploy_mode=deploy_mode, debug_mode=debug_mode, execute=False))
#     Executor.run_all(executors)
#     return Executor.wait_all(executors)



if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
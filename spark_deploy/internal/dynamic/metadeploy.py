# This file contains function handles to control spawning and destroying entire clusters,
# to deploy applications and data on running clusters,
# and to clean up after application execution.

from enum import Enum
import os
import sys
import time

from config.meta import cfg_meta_instance as metacfg
from remote.util.deploymode import DeployMode
from util.executor import Executor
import util.fs as fs
import util.location as loc
from util.printer import *
import util.printer as up

class MetaDeployState(Enum):
    '''Possible deployment states.'''
    INIT = 0     # Not working yet
    BUSY = 1     # Working
    COMPLETE = 2 # Finished, with success
    FAILED = 3   # Finished, with failure


class MetaDeploy(object):
    '''Object to dynamically pass to meta deployment setups'''

    def __init__(self):
        self._reservation_numbers = list()
        self.index = -1
        self.amount = -1

    def set_idx_amt(self, index, amount):
        self.index = index
        self.amount = amount

    def print(self, *args, **kwargs):
        up.print('[{}/{}] '.format(self.index, self.amount), end='')
        up.print(*args, **kwargs)

    def printw(self, string, color=Color.YEL, **kwargs):
        up.printw('[{}/{}] {}'.format(self.index, self.amount, string), color, **kwargs)

    def printe(self, string, color=Color.RED, **kwargs):
        up.printe('[{}/{}] {}'.format(self.index, self.amount, string), color, **kwargs)

    def prints(self, string, color=Color.GRN, **kwargs):
        up.prints('[{}/{}] {}'.format(self.index, self.amount, string), color, **kwargs)

    def block(self, command, args=None, sleeptime=60, dead_after_retries=3):
        '''Blocks for given command. Command must return a `MetaDeployState`, optionally with an additional value.
        If the 'COMPLETE' state is returned, blocking stops and we return True.
        If the 'FAILED' state is returned, blocking stops and we return False.
        If the 'BUSY' state is returned, we sleep for sleeptime seconds.
        Note: If the command returns both a MetaDeployState and an additional value, we check the difference with the previous value.
        If the value remains unchanged after dead_after_retries retries, we assume that the application has died, and we return False.

        Args:
            command (function): Function to periodically call.
            args (container): Container with args to use for calling.
            sleeptime: Number of seconds to sleep between calls (default=60).
            dead_after_retries: Number of times to call the command at most before giving up.

        Returns:
            `True` if command returns `MetaDeployState.COMPLETE` before `dead_after_retries` runs out. `False` otherwise.'''
        val = None
        state = MetaDeployState.INIT
        unchanged = 0

        while True:
            if args == None or len(args) == 0:
                tmp = command()
            else:
                tmp = command(*args)
            if len(tmp) == 2:
                state, val_cur = tmp
                if val_cur == val:
                    unchanged += 1
                else:
                    unchanged = 0
                val = val_cur
            else:
                state = tmp

            if state == MetaDeployState.COMPLETE:
                return True # Completed!
            elif state == MetaDeployState.FAILED:
                return False # User function tells we failed

            if unchanged == dead_after_retries:
                printe('Value ({}) did not change in {} times {} seconds!'.format(val, dead_after_retries, sleeptime))
                return False
            time.sleep(sleeptime)


    def cluster_start(self, time_to_reserve, config_filename, debug_mode, deploy_mode, launch_spark=True, retries=5, retry_sleep_time=5):
        '''Starts a cluster with given time, config, etc.
        Args:
            time_to_reserve (str): DAS-understandable allocation time, e.g. '10:00:00' for 10 hours.
            config_filename: Allocation config to use. 
            debug_mode: If set, prints extra information. Do not use for production.
            deploy_mode (`remote.util.deploymode.DeployMode`): determines where Spark worker work directories are placed (e.g. on NFS mount, local disks, RAMdisk, local-ssd).
            no_interact: If set, never asks anything to user (sensible defaults used instead). Recommended for running batch-jobs.
            launch_spark: If set, launches Spark on allocated nodes. Only allocates nodes otherwise (default=`True`).
            retries: Number of tries before giving up starting the cluster.
            retry_sleep_time: Number of seconds to sleep between tries to execute.
        
        Returns:
            `remote.reserver.Reservation` on success, `None` on failure.'''
        object_deploymode = DeployMode.interpret(deploy_mode) if isinstance(deploy_mode, str) else deploy_mode
        reservation = None
        for x in range(retries):
            try:
                if reservation == None:
                    from main import _start_cluster
                    reservation = _start_cluster(time_to_reserve, config_filename)
                if launch_spark:
                    from main import _start_spark_on_cluster
                    if not _start_spark_on_cluster(reservation, debug_mode, object_deploymode):
                        continue
                self._reservation_numbers.append(reservation.number)
                return reservation
            except Exception as e:
                printe('Error during cluster start: ', end='')
                raise e
                time.sleep(retry_sleep_time)
        return None


    def cluster_stop(self, reservation, silent=False, retries=5, retry_sleep_time=5):
        '''Stops a cluster.
        Args:
            reservation (`remote.reserver.Reservation`): Reservation object to halt.
            silent: If set, no output is printed. Otherwise, can print. (default=`False`).
            retries: Number of tries before giving up starting the cluster.
            retry_sleep_time: Number of seconds to sleep between tries to execute.
        
        Returns:
            `True` on success, `False` on failure.'''
        from main import stop
        number = reservation.number
        for x in range(retries):
            if stop([number], silent=silent):
                self._reservation_numbers.remove(number)
                return True
            time.sleep(retry_sleep_time)
        return False


    def clean_junk(self, reservation, deploy_mode=None, fast=False, datadir=None):
        '''Remove junk generated by Spark during each run. Warning: this between runs, not during a run.
        Args:
            reservation (`remote.reserver.Reservation`): Reservation object to halt.
            deploy_mode (optional `remote.util.deploymode.DeployMode`): If set, looks only for junk with given `DeployMode`. Tries all known locations otherwise.
            fase (optional): If set, only cleans local junk. Otherwise, also cleans junk in nodes over ssh.

        Returns:
            `True` on success. `False` on failure.'''
        if deploy_mode == None: # We don't know where to clean. Clean everywhere
            workdirs = ' '.join([loc.get_spark_work_dir(val) for val in DeployMode])
        else:
            object_deploymode = DeployMode.interpret(deploy_mode) if isinstance(deploy_mode, str) else deploy_mode
            workdirs = loc.get_spark_work_dir(object_deploymode)

        state = True
        if not fast:
            datadir = '' if datadir == None else datadir
            clean_command = 'rm -rf {} {}'.format(workdirs, datadir)
            state &= self.deploy_nonspark_application(reservation, clean_command)
        nfs_log = loc.get_spark_logs_dir()
        command = 'rm -rf {} {}'.format(workdirs, nfs_log)
        state &= os.system(command) == 0
        if state:
            prints('Clean success!')
        else:
            printe('Clean failure!')
        return state
            

    def deploy_application(self, reservation, jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir, retries=5, retry_sleep_time=5):
        '''Deploys an application (a .jar) using spark-submit.
        Args:
            reservation (`remote.reserver.Reservation`): Reservation object on which we should deploy.
            jarfile (str): Path to jar file to deploy. Note: All paths must start in `<project root>/jars/`.
            mainclass (str): Java path to mainclass inside the jarfile.
            args (`Iterable` of str, `None`): Arguments to use when calling the jar.
            extra_jars (`Iterable` of str, `None`): Paths to jars to submit alongside the jarfile Note: All paths must start in `<project root>/jars/`.
            submit_opts (str): Extra options for spark-submit (for advanced users).
            no_resultdir (bool): if set, does not generate a result directory in `<project root>/results/`.
            retries: Number of tries before giving up starting the cluster.
            retry_sleep_time: Number of seconds to sleep between tries to execute.

        Returns:
            `True` on success. `False` on failure.'''
        from deploy.application import deploy_application
        for x in range(retries):
            if deploy_application(reservation.number, jarfile, mainclass, args, extra_jars, submit_opts, no_resultdir):
                return True
            time.sleep(retry_sleep_time)
        return False


    def deploy_nonspark_application(self, reservation, command):
        '''Deploy an application (which is not a Spark application) on all nodes to run in parallel. Useful to e.g. generate data.
        Args:
            reservation (`remote.reserver.Reservation`): Reservation object on which we should deploy.
            command (str): Command to execute on each node to execute the non-Spark application.

        Returns:
            `True` on success. `False` on failure.'''
        if not reservation.validate():
            printe('Reservation no longer valid. Cannot execute non-spark command: {}'.format(command))
            return False
        executors = []
        for host in reservation.deployment.nodes:
            executors.append(Executor('ssh {} "{}"'.format(host, command), shell=True))
        Executor.run_all(executors)
        state = Executor.wait_all(executors, stop_on_error=False)
        if state:
            prints('Command "{}" success!'.format(command))
        else:
            printw('Command "{}" failure on some nodes!'.format(command))
        return state


    def deploy_data(self, reservation_or_number, datalist, deploy_mode, skip, subpath='', retries=5, retry_sleep_time=5):
        '''Deploy data on the local drive of a node.
        Args:
            reservation_or_number (`remote.reserver.Reservation`, `int`): The reservation object, or int reservation number to use.
            datalist (`Iterable` of str): Files/Directories to deploy. Note: All paths must start in `<project root>/data/`.
            deploy_mode (`remote.util.deploymode.DeployMode` or str):  `DeployMode` for the data, determining whether data is placed on the NFS mount, local disk, RAMdisk etc.
            skip (bool): If set, skips copying data that already exists in a particular node's local drive. Note: If existing data is old or incomplete with this mode set, the data is not replaced.
            subpath (optional str): The extra path to append to the rsync target location.
            retries: Number of tries before giving up starting the cluster.
            retry_sleep_time: Number of seconds to sleep between tries to execute.

        Returns:
            `True` on success. `False` on failure.'''
        dmode = DeployMode.interpret(deploy_mode) if isinstance(deploy_mode, str) else deploy_mode
        dlist = listdatalist if isinstance(datalist, list) else [datalist]
        from deploy.data import deploy_data
        for x in range(retries):
            if deploy_data(reservation_or_number, dlist, dmode, skip, subpath=subpath):
                return True
            time.sleep(retry_sleep_time)
        return False


    def deploy_ceph(self, slicename):
        '''Deploys a Ceph cluster with given slicename.'''
        return True
        # if cephutil.has_docker():
        #     # Local - we run from here
        #     pass
        # else:
        #     # Remote
        #     import gni.py2bridge as bridge
        #     info = bridge.start_vm(expiration=args.start_vm)
        #     if not info:
        #         print('Could not boot VM')
        #         return False
        #     print('VM info: {}'.format(info))
        #     basecmd = 'ssh -i {} {}@{} -p {}'.format(KEYINFO, info.user, info.ip, info.port)
        #     cmd = '{} '
        #     subprocess.check_output(cmd, shell=True)
        # Need to ssh to remote. For that, need ssh key path.
        # Together with ssh, need to execute a one-liner to:
        #   1. Install docker and docker-compose (maybe ceph-ansible too)



    def deploy_flamegraph(self, reservation_or_number, flame_graph_duration='30s', only_master=False, only_worker=False):
        if flame_graph_duration != None:
            from deploy.flamegraph import deploy_flamegraph
            deploy_flamegraph(reservation_or_number, flame_graph_duration, only_master, only_worker)


    # Print method to print to stderr
    def eprint(self, *args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)

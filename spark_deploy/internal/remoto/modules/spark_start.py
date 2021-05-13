import os
import subprocess
import time


def java_home_available():
    return java_home() != None


def java_home():
    return os.getenv('JAVA_HOME')


def start_master(sparkloc, host, port=7077, webui_port=8080, silent=False, retries=5, retries_sleep=5):
    '''Boots master on given node.
    Note: Spark works with Daemons, so expect to return quickly, probably even before the worker is actually ready.

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
        `(True, master_url)` on success, `(False, None)` otherwise.'''
    sparkloc = os.path.expanduser(sparkloc)
    env = Environment()
    env.load_to_env()
    if not isdir(sparkloc):
        printe('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False, None

    scriptloc = join(sparkloc, 'sbin', 'start-master.sh')
    if not isfile(scriptloc):
        printe('Could not find file at {}. Did Spark not install successfully?'.format(scriptloc))
        return False, None

    if not java_home_available():
        printe('JAVA_HOME not found in the current environment.')
        return False, None

    if not silent:
        print('Spawning master, using hostname {}...'.format(host))

    cmd = '{} --host {} --port {} --webui-port {} 1>&2'.format(scriptloc, host, port, webui_port)
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {}

    master_url = 'spark://{}:{}'.format(host, port)
    for x in range(retries):
        if subprocess.call(cmd, shell=True, **kwargs) == 0:
            printc('MASTER ready on {} (webui address: http://{}:{})'.format(master_url, host, webui_port), Color.CAN)
            return True, master_url
        if x == 0:
            printw('Could not boot master. Retrying...')
        time.sleep(retries_sleep)
    printe('Could not boot master.')
    return False, None


def start_worker(sparkloc, workdir, master_node, master_port=7077, silent=False, retries=5, retries_sleep=5):
    '''Boots a worker.
    Note: Spark works with Daemons, so expect to return quickly, probably even before the worker is actually ready.

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
    sparkloc = os.path.expanduser(sparkloc)
    workdir = os.path.expanduser(workdir)
    env = Environment()
    env.load_to_env()
    if not isdir(sparkloc):
        printe('Could not find Spark installation at {}. Did you run the `install` command for that location?'.format(sparkloc))
        return False

    scriptloc = join(sparkloc, 'sbin', 'start-worker.sh')
    if not isfile(scriptloc):
        printe('Could not find file at "{}". Did Spark not install successfully?'.format(scriptloc))
        return False

    if not java_home_available():
        printe('JAVA_HOME not found in the current environment.')
        return False

    master_url = 'spark://{}:{}'.format(master_node, master_port)

    if not silent:
        print('Spawning worker')

    cmd = '{} {} --work-dir {} {}'.format(scriptloc, master_url, workdir, '> /dev/null 2>&1' if silent else '')
    for x in range(retries):
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('utf-8').strip()
            if len(output.split('\n')) == 1: # Worker script prints exactly 1 line when all is well: "Starting org.apache.spark.deploy.worker.Worker, logging to..."
                return True
            printe('Worker script output indicates failure to launch.')
            if x == 0:
                print('Output: {}'.format(output))
        except Exception as e:
            if x == 0:
                printw('Could not boot worker. retrying...')
            time.sleep(retries_sleep)
    printe('Could not boot worker (failed {} times, {} sleeptime between executions)'.format(retries, retries_sleep))
    return False
import os
import socket
from pathlib import Path
import re
import shutil
import sys
import tempfile
import urllib.request

from internal.util.printer import *


def stderr(string, *args, **kwargs):
    kwargs['flush'] = True
    kwargs['file'] = sys.stderr
    print('[{}] {}'.format(socket.gethostname(), string), *args, **kwargs)

'''In this file, we provide functions to install and interact with Apache Spark.'''


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

def _ls(directory, only_files=False, only_dirs=False, full_paths=False, *args):
    ddir = os.path.join(directory, *args)
    if only_files and only_dirs:
        raise ValueError('Cannot ls only files and only directories')

    if sys.version_info >= (3, 5): # Use faster implementation in python 3.5 and above
        with os.scandir(ddir) as it:
            for entry in it:
                if (entry.is_dir() and not only_files) or (entry.is_file() and not only_dirs):
                    yield os.path.join(ddir, entry.name) if full_paths else entry.name
    else: # Use significantly slower implementation available in python 3.4 and below
        for entry in os.listdir(ddir):
            if (isdir(ddir, entry) and not only_files) or (os.path.isfile(ddir, entry) and not only_dirs):
                yield os.path.join(ddir, entry) if full_paths else entry


def _resolvelink(path, *args):
    str(Path(os.path.join(path, *args)).resolve().absolute())


def java_exec_get_versioninfo(java_exec):
    '''Fetches Java version CLI output.
    Args:
        java_exec (str): Full path to Java executable to test.

    Raises:
        Exception: Any exception may be thrown by `subprocess` module, e.g. when `java_exec` path is invalid, or the executable's returncode is non-zero. 

    Returns:
        Raw decoded CLI version output for given executable.'''
    return subprocess.check_output('{} -version'.format(java_exec), shell=True).decode('utf-8').strip()


def java_installed(location):
    '''Check if Java is installed in given directory.'''
    return os.path.isdir(location) and os.path.isfile(os.path.join(location, 'bin', 'java'))


def java_shell_resolvepath():
    '''Returns the actual (non-symlink) path to which the java shell resolves.'''
    path = subprocess.check_output('which java', shell=True).decode('utf-8').strip()
    return _resolvelink(path) if os.path.islink(path) else path


def java_shell_available():
    '''Check if we can call Java from the shell. Most root installations of Java don't set the `JAVA_HOME`, but do install.
    We can check for those installations if we can call Java.

    Returns:
        `True` if we can call Java, `False` otherwise.'''
    return subprocess.call('java -version', shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0


def java_root_paths():
    '''Returns generator for every Java root installation.'''
    return (x for x in _ls(os.path.join(os.path.abspath(os.sep), 'usr', 'lib', 'jvm'), only_dirs=True, full_paths=True) if java_installed(x))


def java_root_available():
    '''Returns whether at least one Java installation is available as a root installation.'''
    for x in os.path.isfile(os.path.join(os.path.abspath(os.sep), 'usr', 'lib', 'jvm'), only_dirs=True, full_paths=True):
        if java_installed(x):
            return True
    return False


def java_home_valid():
    '''Returns `True` if Java is available in location pointed to by `JAVA_HOME`, `False` if Java is unavailable or `JAVA_HOME` unavailable.'''
    return java_home_available() and java_installed(java_home())


def java_home_available():
    return java_home() != None


def java_home():
    return os.getenv('JAVA_HOME')


def set_java_home(path):
    '''Sets java path, both in ~/.bashrc and directly in the environment through python.'''
    os.environ['JAVA_HOME'] = path

    bashrc_loc = os.path.expanduser('~/.bashrc')
    with open(bashrc_loc, 'r') as f:
        lines = [x for x in f.readlines() if not 'export JAVA_HOME' in x]
    with open(bashrc_loc, 'w') as f:
        f.write(''.join(lines))
        f.write('export JAVA_HOME={}'.format(path))


def java_acceptable_version(versionstring, minversion, maxversion):
    '''Returns `True` if given versionstring contains a correct version number. Strings are matched using regex, finding the first number.'''
    versionnumber = int(re.search(r'\d+', versionstring).group())
    return versionnumber >= minversion and versionnumber <= maxversion


def phase0():
    '''Phase 0: JAVA_HOME check installation. If this succeeds, we don't have to do anything.'''
    return java_home_valid() and java_acceptable_version(os.path.join(java_home(), 'bin', 'java'), minversion, maxversion)


def phase1():
    '''Phase 1: Java shell-check installation. If the shell-default java version has a high-enough number, we only have to set `JAVA_HOME` env variable.'''
    if java_shell_available() and java_acceptable_version(java_exec_get_versioninfo('java'), minversion, maxversion): 
        # Java is available on shell. We might have to set `JAVA_HOME` to the right place.
        java_shell_path = java_shell_resolvepath()
        if java_home_available():
            if (java_shell_path != java_home()): # `JAVA_HOME` is set, but to an incorrect path.
                printw('Found JAVA_HOME={}, version mismatched requirements. Set to matching location={}'.format(java_home(), java_shell_path), file=sys.stderr)
                set_java_home(java_shell_path)
            else: # `JAVA_HOME` is already set correctly. Impossible. We just checked this in phase0. External tampering. Doesn't matter, `JAVA_HOME` is set correctly now.
                pass
        else: # `JAVA_HOME` is not set.
            set_java_home(java_shell_path)
        return True
    return False


def phase2():
    '''Phase 2: Java root availability check installation'''
    if java_root_available():
        for x in java_root_paths():
            if java_acceptable_version(java_exec_get_versioninfo(x), minversion, maxversion):
                prints('Found Java in: {}'.format(x))
                if java_home_available():
                    if (java_shell_path != java_home()): # `JAVA_HOME` is set, but to an incorrect path.
                        printw('Found JAVA_HOME={}, version mismatched requirements. Set to matching location={}'.format(java_home(), java_shell_path), file=sys.stderr)
                        set_java_home(java_shell_path)
                    else: # `JAVA_HOME` is already set correctly. Impossible. We just checked this in phase0. External tampering. Doesn't matter, `JAVA_HOME` is set correctly now.
                        pass
                else: # `JAVA_HOME` is not set.
                    set_java_home(java_shell_path)
                return True
    return False

def install(location, url, minversion, maxversion, retries):
    '''Checks if Java is already available. If not, installs Java by downloading and installing from `.tgz`. Assumes extracted zip layout to look like:
    | some_dir/
    |           conf/
    |           examples/
    |           ...
    |           README.md
    The contents from `some_dir` are copied to given `location`.
    Args:
        location (str): The location where final output will be available on success.
        url (str): URL of zip to download. Look e.g. in 'https://downloads.apache.org/spark/' for zips.
        retries (int): Number of retries to use when downloading, extracting.

    Returns:
        `True` on success, `False` on failure.'''
    stderr('Beginning Java install procedure')

    if phase0() or phase1() or phase2():
        return True

    # Phase 3a: Java root installation
    if use_sudo:
        cmd = 'sudo apt update -y && sudo apt install openjdk-11-jre-headless -y'
        if subprocess.call(cmd, shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0:
            if phase1() or phase2():
                return True
            else:
                printe('Unexpected failure to configure newly downloaded "openjdk-11-jre-headless". Please install it yourself, and point JAVA_HOME to the root installation directory.')
                return False
        else:
            printe('Unexpected error during execution of command: {}'.format(cmd))
            return False
    else: # Phase 3b: Java local installation
        
    
    return False



    with tempfile.TemporaryDirectory() as tmpdir: # We use a tempfile to store the downloaded zip.
        archiveloc = os.path.join(tmpdir, 'spark.tgz')
        if not os.path.isfile(archiveloc):
            for x in range(retries):
                try:
                    _rm(archiveloc, ignore_errors=True)
                    stderr('[{}] Fetching spark from {}'.format(x, url))
                    urllib.request.urlretrieve(url, archiveloc)
                    break
                except Exception as e:
                    if x == 0:
                        stderr('Could not download Spark. Retrying...')
                    elif x == 4:
                        stderr('Could not download Spark: ', e)
                        return False
        for x in range(retries):
            try:
                extractloc = os.path.join(tmpdir, 'extracted')
                os.makedirs(extractloc, exist_ok=True)
                shutil.unpack_archive(archiveloc, extractloc)

                extracted_dir = next(_ls(extractloc, only_dirs=True, full_paths=True)) # find out what the extracted directory is called. There will be only 1 extracted directory.
                for x in _ls(extracted_dir, full_paths=True): # Move every file and directory to the final location.
                    shutil.move(x, location)
                stderr('installation completed')
                return True
            except Exception as e:
                if x == 4:
                    stderr('Could not download zip file correctly: ', e)
                    return False
                elif x == 0:
                    stderr('Could not extract archive. Retrying...', e)
    return False


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
import os
import re
import subprocess
import tempfile
import urllib.request


'''In this file, we provide functions to install Java.'''

def java_exec_get_versioninfo(java_exec, *args):
    '''Fetches Java version CLI output.
    Args:
        java_exec (str): Full path to Java executable to test.

    Raises:
        Exception: Any exception may be thrown by `subprocess` module, e.g. when `java_exec` path is invalid, or the executable's returncode is non-zero. 

    Returns:
        Raw decoded CLI version output for given executable.'''
    return subprocess.check_output('{} -version 2>&1'.format(join(java_exec, *args)), shell=True).decode('utf-8').strip()


def java_installed(location):
    '''Check if Java is installed in given directory.'''
    return isdir(location) and isfile(join(location, 'bin', 'java'))


def java_shell_resolvepath():
    '''Returns the actual (non-symlink) path to which the java shell resolves.'''
    path = subprocess.check_output('which java', shell=True).decode('utf-8').strip()
    return resolvelink(path, full_resolve=True) if issymlink(path) else path


def java_shell_available():
    '''Check if we can call Java from the shell. Most root installations of Java don't set the `JAVA_HOME`, but do install.
    We can check for those installations if we can call Java.

    Returns:
        `True` if we can call Java, `False` otherwise.'''
    return subprocess.call('java -version', shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0


def java_root_paths():
    '''Returns generator for every Java root installation.'''
    return (x for x in ls(join(abspath(os.sep), 'usr', 'lib', 'jvm'), only_dirs=True, full_paths=True) if java_installed(x))


def java_root_available():
    '''Returns whether at least one Java installation is available as a root installation.'''
    path = join(abspath(os.sep), 'usr', 'lib', 'jvm')
    if not isdir(path):
        return False
    for x in _ls(path, only_dirs=True, full_paths=True):
        if java_installed(x):
            return True
    return False


def java_home_valid():
    '''Returns `True` if Java is available in location pointed to by `JAVA_HOME`, `False` if Java is unavailable or `JAVA_HOME` unavailable.'''
    return java_home_available() and java_installed(java_home())


def java_home_available():
    return java_home() != None


def java_home():
    return env.get('JAVA_HOME') or os.getenv('JAVA_HOME')


def set_java_home(path):
    '''Sets java path, both in ~/.bashrc and directly in the environment through python.'''
    env.set('JAVA_HOME', path)

    bashrc_loc = os.path.expanduser('~/.bashrc')
    with open(bashrc_loc, 'r') as f:
        lines = [x for x in f.readlines() if not 'export JAVA_HOME' in x]
    with open(bashrc_loc, 'w') as f:
        f.write(''.join(lines))
        f.write('export JAVA_HOME={}'.format(path))


def java_acceptable_version(versionstring, minversion, maxversion):
    '''Returns `True` if given versionstring contains a correct version number. Strings are matched using regex, finding the first number.'''
    try:
        versionnumber = int(re.search(r'\d+', versionstring).group())
        return (versionnumber >= minversion or minversion == 0) and (versionnumber <= maxversion or maxversion == 0)
    except Exception as e:
        printe('Version string does not have a number contained: "{}"'.format(versionstring))
        return False


def phase0(minversion, maxversion):
    '''Phase 0: JAVA_HOME check installation. If this succeeds, we don't have to do anything.'''
    return java_home_valid() and java_acceptable_version(java_exec_get_versioninfo(java_home(), 'bin', 'java'), minversion, maxversion)


def phase1(minversion, maxversion):
    '''Phase 1: Java shell-check installation. If the shell-default java version has a high-enough number, we only have to set `JAVA_HOME` env variable.'''
    if java_shell_available() and java_acceptable_version(java_exec_get_versioninfo('java'), minversion, maxversion): 
        # Java is available on shell. We might have to set `JAVA_HOME` to the right place.
        java_shell_path = java_shell_resolvepath()
        java_shell_path = dirname(dirname(java_shell_path)) # Go from <java_loc>/bin/java to <java_loc>
        if java_home_available():
            if (java_shell_path != java_home()): # `JAVA_HOME` is set, but to an incorrect path.
                printw('Found JAVA_HOME={}, version mismatched requirements. Set to matching location={}'.format(java_home(), java_shell_path))
                set_java_home(java_shell_path)
            else: # `JAVA_HOME` is already set correctly. Impossible. We just checked this in phase0. External tampering. Doesn't matter, `JAVA_HOME` is set correctly now.
                pass
        else: # `JAVA_HOME` is not set.
            set_java_home(java_shell_path)
        return True
    return False


def phase2(minversion, maxversion):
    '''Phase 2: Java root availability check installation'''
    if java_root_available():
        for x in java_root_paths():
            if java_acceptable_version(java_exec_get_versioninfo(x), minversion, maxversion):
                prints('Found Java in: {}'.format(x))
                if java_home_available():
                    if (x != java_home()): # `JAVA_HOME` is set, but to an incorrect path.
                        printw('Found JAVA_HOME={}, version mismatched requirements. Set to matching location={}'.format(java_home(), x))
                        set_java_home(x)
                    else: # `JAVA_HOME` is already set correctly. Impossible. We just checked this in phase0. External tampering. Doesn't matter, `JAVA_HOME` is set correctly now.
                        pass
                else: # `JAVA_HOME` is not set.
                    set_java_home(x)
                return True
    return False


def java_install_sudo(minversion, maxversion, silent=False, retries=5):
    return java_install(None, None, minversion, maxversion, True, silent, retries)

def java_install_nonsudo(location, url, minversion, maxversion, silent=False, retries=5):
    return java_install(location, url, minversion, maxversion, False, silent, retries)

def java_install(location=None, url=None, minversion=11, maxversion=0, use_sudo=False, silent=False, retries=5):
    '''Checks if Java is already available. If not, installs Java by downloading and installing from `.tgz`. Assumes extracted zip layout to look like:
    | some_dir/
    |           bin/
    |           conf/
    |           ...
    The contents from `some_dir` are copied to given `location`.
    In particular, this module allows us to install using sudo or not.
    Args:
        location (optional str): path to store local Java installation. Note: Ignored if `use_sudo`.
        url (optional str): URL of zip to download. Look e.g. in 'https://jdk.java.net/archive/' for archives. Ignored if `use_sudo`.
        minversion (optional int): Minimal acceptable java version. 0 means no limit.
        maxversion (optional int): Maximal acceptable java version. 0 means no limit.
        use_sudo (optional bool): If set, sudo user rights are used to install system-wide Java distribution. Otherwise, installs locally.
        retries (optional int): Number of retries to use when downloading, extracting.
        silent (optional bool): If set, prints less info.

    Returns:
        `True` on success, `False` on failure.'''
    location = os.path.expanduser(location)
    
    global env
    env = Environment()

    if phase0(minversion, maxversion) or phase1(minversion, maxversion) or phase2(minversion, maxversion):
        if not silent:
            print('Acceptable existing Java installation detected. Skipping installation.')
        return True
    if not silent:
        print('Beginning Java install procedure')

    # Phase 3a: Java root installation
    if use_sudo:
        # Download the greatest version of maxversion, minversion, 15. 15 is picked because we know it exists, and is the latest ATM.
        openjdk = 'openjdk-{}-jre-headless -y'.format(max(maxversion, minversion, 15))
        cmd = 'sudo apt update -y && sudo apt install {}'.format(openjdk)
        if subprocess.call(cmd, shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0:
            if phase1(minversion, maxversion) or phase2(minversion, maxversion):
                if java_acceptable_version(java_exec_get_versioninfo(java_home(), 'bin', 'java'), minversion, maxversion):
                    return True
                else:
                    printe('Installed java does not meet dependencies. Picked Java version min={}, max={}, installed={}'.format(minversion, maxversion, max(maxversion, minversion, 15)))
                    return False
            else:
                printe('Unexpected failure to configure newly downloaded "{}". Please point JAVA_HOME to the root installation directory yourself.'.format(openjdk))
                return False
        else:
            printe('Unexpected error during execution of command: {}'.format(cmd))
            return False
    else: # Phase 3b: Java local installation
        with tempfile.TemporaryDirectory() as tmpdir: # We use a tempfile to store the downloaded archive.
            archiveloc = join(tmpdir, 'java.tar.gz')
            if not silent:
                print('Fetching Java from {}'.format(url))
            for x in range(retries):
                try:
                    rm(archiveloc, ignore_errors=True)
                    urllib.request.urlretrieve(url, archiveloc)
                    break
                except Exception as e:
                    if x == 0:
                        printw('Could not download Java. Retrying...')
                    elif x == retries-1:
                        printe('Could not download Java: {}'.format(e))
                        return False
            try:
                extractloc = join(tmpdir, 'extracted')
                mkdir(extractloc, exist_ok=True)
                unpack(archiveloc, extractloc)

                extracted_dir = next(ls(extractloc, only_dirs=True, full_paths=True)) # find out what the extracted directory is called. There will be only 1 extracted directory.
            except Exception as e:
                printe('Could not extract zip file correctly: {}'.format(e))
                return False
            try:
                rm(location, ignore_errors=True)
                mv(extracted_dir, location)
            except Exception as e:
                printe('Could not move extracted contents ({}) to ({}): {}'.format(extracted_dir, location, e))

            set_java_home(abspath(location))
            if java_acceptable_version(java_exec_get_versioninfo(java_home(), 'bin', 'java'), minversion, maxversion):
                if not silent:
                    prints('installation completed.')
                return True
            else:
                printe('Unexpected failure to configure newly downloaded "{}". Please point JAVA_HOME to the root installation directory yourself.'.format(location))
                return False


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
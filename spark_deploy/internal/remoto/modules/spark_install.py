import builtins
from enum import Enum
import os
import socket
from pathlib import Path
import shutil
import sys
import tempfile
import urllib.request


'''In this file, we provide functions to install Apache Spark.'''


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


def _is_installed(location):
    '''Check if Spark is installed in given directory.'''
    return os.path.isdir(location) and os.path.isdir(os.path.join(location, 'sbin'))


def spark_install(location, url, silent=False, retries=5):
    '''Installs Spark by downloading and installing from `.tgz`. Assumes extracted zip layout to look like:
    | some_dir/
    |           conf/
    |           examples/
    |           ...
    |           README.md
    The contents from `some_dir` are copied to given `location`.
    Args:
        location (str): The location where final output will be available on success.
        url (str): URL of zip to download. Look e.g. in 'https://downloads.apache.org/spark/' for zips.
        silent (optional bool): If set, prints less info.
        retries (optional int): Number of retries to use when downloading, extracting.
    Returns:
        `True` on success, `False` on failure.'''

    if _is_installed(location): # Already installed
        if not silent:
            print('Existing Spark installation detected. Skipping installation.')
        return True

    os.makedirs(location, exist_ok=True)
    if not silent:
        print('Installing Spark in {}...'.format(location))

    with tempfile.TemporaryDirectory() as tmpdir: # We use a tempfile to store the downloaded zip.
        archiveloc = os.path.join(tmpdir, 'spark.tgz')
        for x in range(retries):
            try:
                _rm(archiveloc, ignore_errors=True)
                if not silent:
                    print('Fetching spark from {}'.format(url))
                urllib.request.urlretrieve(url, archiveloc)
                break
            except Exception as e:
                if x == 0:
                    printe('Could not download Spark. Retrying...')
                elif x == 4:
                    printe('Could not download Spark: ', e)
                    return False
        try:
            extractloc = os.path.join(tmpdir, 'extracted')
            os.makedirs(extractloc, exist_ok=True)
            shutil.unpack_archive(archiveloc, extractloc)

            extracted_dir = next(_ls(extractloc, only_dirs=True, full_paths=True)) # find out what the extracted directory is called. There will be only 1 extracted directory.
            for x in _ls(extracted_dir, full_paths=True): # Move every file and directory to the final location.
                shutil.move(x, location)
            if not silent:
                prints('Spark installation completed.')
            return True
        except Exception as e:
            printe('Could not download zip file correctly: ', e)
            return False


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
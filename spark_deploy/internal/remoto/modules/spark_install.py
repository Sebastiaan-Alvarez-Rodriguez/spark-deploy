import os
import socket
from pathlib import Path
import shutil
import sys
import tempfile
import urllib.request


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


def spark_installed(location):
    '''Check if Spark is installed in given directory.'''
    return os.path.isdir(location) and os.path.isdir(os.path.join(location, 'sbin'))


def install(location, url, retries):
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
        retries (int): Number of retries to use when downloading, extracting.

    Returns:
        `True` on success, `False` on failure.'''
    stderr('Beginning install procedure')
    if spark_installed(location): # Already installed
        return True

    os.makedirs(location, exist_ok=True)
    stderr('Installing Spark in {}'.format(location))

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
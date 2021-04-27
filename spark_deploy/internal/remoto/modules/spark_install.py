
import os
from pathlib import Path
import urllib.request
import shutil
import tempfile



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


def spark_installed(location):
    '''Check if Spark is installed in given directory.'''
    return os.path.isdir(location) and os.path.isdir(os.path.join(location, 'sbin'))


def install(location, retries):
    '''Installs Spark by downloading and installing from `.tgz`. 
    Args:
        location (str): The location where final output will be available on success.
        retries (int): Number of retries to use when downloading, extracting.

    Returns:
        `True` on success, `False` on failure.'''
    if spark_installed(location): # Already installed
        return True

    os.makedirs(location, exist_ok=True)
    print('Installing Spark in {}'.format(location))

    url = 'https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz'

    with tempfile.TemporaryDirectory() as tmpdir: # We use a tempfile to store the downloaded zip.
        archiveloc = os.path.join(tmpdir, 'spark-3.0.1.tgz')
        if not os.path.isfile(archiveloc):
            for x in range(retries):
                try:
                    _rm(archiveloc, ignore_errors=True)
                    print('[{}] Fetching spark from {}'.format(x, url))
                    urllib.request.urlretrieve(url, archiveloc)
                    break
                except Exception as e:
                    if x == 0:
                        print('Could not download Spark. Retrying...')
                    elif x == 4:
                        print('Could not download Spark: ', e)
                        return False
        for x in range(retries):
            try:
                extractloc = os.path.join(tmpdir, 'extracted')
                os.makedirs(extractloc, exist_ok=True)
                shutil.unpack_archive(archiveloc, extractloc)
                shutil.move(os.path.join(extractloc, 'spark-3.0.1-bin-hadoop2.7'), location)
                print('installing complete')
                return True
            except Exception as e:
                if x == 4:
                    print('Could not download zip file correctly: ', e)
                    return False
                elif x == 0:
                    print('Could not extract archive. Retrying...', e)
    return False


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))
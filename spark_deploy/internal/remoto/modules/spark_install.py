import os
import tempfile
import urllib.request


'''In this file, we provide functions to install Apache Spark.'''


def _is_installed(location):
    '''Check if Spark is installed in given directory.'''
    return isdir(location) and isdir(join(location, 'sbin'))


def spark_install(location, url, force_reinstall=False, silent=False, retries=5):
    '''Installs Spark by downloading and installing from `.tgz`. Assumes extracted archive layout to look like:
    | some_dir/
    |           conf/
    |           examples/
    |           ...
    |           README.md
    The contents from `some_dir` are copied to given `location`.
    Args:
        location (str): The location where final output will be available on success.
        url (str): URL of zip to download. Look e.g. in 'https://archive.apache.org/dist/spark/' for suitable archives.
        force_reinstall (optional bool): If set, reinstalls Spark if it is found. Otherwise, we skip installation if Spark is found.
        silent (optional bool): If set, prints less info.
        retries (optional int): Number of retries to use when downloading, extracting.
    Returns:
        `True` on success, `False` on failure.'''
    location = os.path.expanduser(location)
    if _is_installed(location) and not force_reinstall: # Already installed
        if not silent:
            print('Existing Spark installation detected. Skipping installation.')
        return True

    rm(location, ignore_errors=True)
    mkdir(location)
    if not silent:
        print('Installing Spark in {}...'.format(location))

    with tempfile.TemporaryDirectory() as tmpdir: # We use a tempfile to store the downloaded zip.
        archiveloc = join(tmpdir, 'spark.tgz')
        for x in range(retries):
            try:
                rm(archiveloc, ignore_errors=True)
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
            extractloc = join(tmpdir, 'extracted')
            mkdir(extractloc, exist_ok=True)
            unpack(archiveloc, extractloc)

            extracted_dir = next(ls(extractloc, only_dirs=True, full_paths=True)) # find out what the extracted directory is called. There will be only 1 extracted directory.
            for x in ls(extracted_dir, full_paths=True): # Move every file and directory to the final location.
                mv(x, location)
            if not silent:
                prints('Spark installation completed.')
            return True
        except Exception as e:
            printe('Could not extract zip file correctly: ', e)
            return False
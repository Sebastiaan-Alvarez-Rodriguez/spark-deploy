import spark_deploy.internal.util.fs as fs


def sparkdir(install_dir):
    '''Path to Spark installation.'''
    return fs.join(install_dir, 'spark')


def java_nonroot_dir(install_dir):
    '''Path to non-root java installation. Warning: If this system detected java is already installed, it will not install java again, and this dir will not exist.'''
    return fs.join(install_dir, 'java')
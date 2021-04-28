class RemotoSSHWrapper(object):
    def __init__(self, connection, ssh_config=None):
        self._connection = connection
        self._ssh_config = ssh_config


    def __enter__(self):
        return self

    @property
    def connection(self):
        return self._connection
    
    @property
    def ssh_config(self):
        return self._ssh_config.conf if self._ssh_config else None
    

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.exit()
        if self._ssh_config:
            self._ssh_config.exit()
        return False


    def exit(self):
        self._connection.exit()
        if self._ssh_config:
            self._ssh_config.exit()
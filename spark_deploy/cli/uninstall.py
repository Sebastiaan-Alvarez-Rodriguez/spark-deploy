import cli.util as _cli_util
import uninstall as _uninstall


'''CLI module to uninstall Spark and Java from a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    uninstallparser = subparsers.add_parser('uninstall', help='Uninstall Spark environment and local java from a server cluster.')
    return [uninstallparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'uninstall'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _uninstall.uninstall(reservation, args.install_dir, args.key_path) if reservation else False
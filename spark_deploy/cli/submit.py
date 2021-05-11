import cli.util as _cli_util
import submit as _submit

import internal.defaults as defaults


'''CLI module to start a Spark cluster.'''

def subparser(subparsers):
    '''Register subparser modules.'''
    submitparser = subparsers.add_parser('submit', help='Submit applications to a running remote Spark cluster.')
    submitparser.add_argument('cmd', metavar='cmd', type=str, default=None, help='Command to execute with "spark-submit". if you need to use flags in "spark-submit" with "-" signs, use e.g. "-- -h" to ignore "-" signs for the rest of the command.')
    submitparser.add_argument('--master', metavar='id', dest='master_id', type=int, default=None, help='ID of the node that will be the master node (command will be executed on this node).')
    submitparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    submitparser.add_argument('--paths', metavar='path', type=str, nargs='+', default=[], help='Paths to files/directories to export to the cluster. These files/directories will be in the CWD when executing "spark-submit".')
    submitparser.add_argument('--application_dir', type=str, default=defaults.application_dir(), help='Location on remote host where we export all given applications to (pointed to by "paths"). The home directory of the remote machines is prepended to this path if it is relative.')
    
    return [submitparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'submit'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _submit.submit(reservation, args.cmd, args.paths, args.install_dir, args.key_path, args.application_dir, args.master_id, silent=args.silent) if reservation else False
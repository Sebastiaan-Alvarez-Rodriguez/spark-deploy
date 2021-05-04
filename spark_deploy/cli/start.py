import cli.util as _cli_util
import start as _start


'''CLI module to start a Spark cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    startparser = subparsers.add_parser('start', help='Orchestrate Spark environment on server cluster.')
    startparser.add_argument('--master', metavar='id', dest='master_id', type=int, default=None, help='ID of the node that will be the master node.')
    startparser.add_argument('--workdir', metavar='path', type=str, default=_start._default_workdir(), help='Path to Spark workdir location for all slave daemons (default={}). Note: The home directory of the remote machines is prepended to this path if it is relative.'.format(_start._default_workdir()))
    startparser.add_argument('--master-host', metavar='host', dest='master_host', type=str, default=None, help='Master hostname to listen on.')
    startparser.add_argument('--master-port', metavar='port', dest='master_port', type=int, default=_start._default_masterport(), help='port to use for master (default={}).'.format(_start._default_masterport()))
    startparser.add_argument('--webui-port', metavar='port', dest='webui_port', type=int, default=_start._default_webuiport(), help='port to use for the Spark webUI (default={}).'.format(_start._default_webuiport()))
    startparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    startparser.add_argument('--retries', metavar='amount', type=int, default=_start._default_retries(), help='Amount of retries to use for risky operations (default={}).'.format(_start._default_retries()))
    return [startparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'start'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _start.start(reservation, args.installdir, args.key_path, args.master_id, master_host=args.master_host, master_port=args.master_port, webui_port=args.webui_port, slave_workdir=args.workdir, silent=args.silent, retries=args.retries) if reservation else False
import spark_deploy.cli.util as _cli_util
import spark_deploy.internal.defaults.start as defaults
import spark_deploy.start as _start

'''CLI module to start a Spark cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    startparser = subparsers.add_parser('start', help='Start Spark on server cluster.')
    startparser.add_argument('--master', metavar='id', dest='master_id', type=int, default=None, help='ID of the node that will be the master node.')
    startparser.add_argument('--workdir', metavar='path', type=str, default=defaults.workdir(), help='Path to Spark workdir location for all worker daemons (default={}).'.format(defaults.workdir()))
    startparser.add_argument('--master-host', metavar='host', dest='master_host', type=str, default=None, help='Master hostname to listen on.')
    startparser.add_argument('--master-port', metavar='port', dest='master_port', type=int, default=defaults.masterport(), help='port to use for master (default={}).'.format(defaults.masterport()))
    startparser.add_argument('--webui-port', metavar='port', dest='webui_port', type=int, default=defaults.webuiport(), help='port to use for the Spark webUI (default={}).'.format(defaults.webuiport()))
    startparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    startparser.add_argument('--retries', metavar='amount', type=int, default=defaults.retries(), help='Amount of retries to use for risky operations (default={}).'.format(defaults.retries()))
    return [startparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'start'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _start(reservation, args.install_dir, args.key_path, args.master_id, master_host=args.master_host, master_port=args.master_port, webui_port=args.webui_port, worker_workdir=args.workdir, silent=args.silent, retries=args.retries)[0] if reservation else False
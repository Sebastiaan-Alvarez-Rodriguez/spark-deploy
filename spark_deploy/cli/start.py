import cli.util as _cli_util
import deploy  as _deploy
from internal.util.printer import *

'''CLI module to deploy Spark on a cluster.'''




def subparser(subparsers):
    '''Register subparser modules'''
    startparser = subparsers.add_parser('start', help='Orchestrate Spark environment on server cluster.')
    # startparser.add_argument('-c', '--clusterconfig', metavar='config', type=str, help='Cluster config filename to use for execution')
    # startparser.add_argument('-d', '--debug-mode', dest='debug_mode', help='Run remote in debug mode', action='store_true')
    # startparser.add_argument('-dm', '--deploy-mode', dest='deploy_mode', type=str, metavar='mode', default=str(DeployMode.STANDARD), help='Deployment mode for cluster', choices=[str(x) for x in DeployMode])
    # startparser.add_argument('--internal', nargs=1, type=str, help=argparse.SUPPRESS)
    return [startparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'start'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _deploy.start(reservation) if reservation else False
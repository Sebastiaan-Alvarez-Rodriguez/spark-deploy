import argparse
import datetime

import spark_deploy.cli.util as _cli_util
import spark_deploy.internal.defaults.start as start_defaults
import spark_deploy.internal.defaults.stop as defaults
import spark_deploy.stop as _stop


'''CLI module to stop a Spark cluster.'''


def _cached(response, cached_val):
    return response if response else cached_val


def subparser(subparsers):
    '''Register subparser modules'''
    stopparser = subparsers.add_parser('stop',  help='Stop Spark cluster.')
    stopparser.add_argument('--workdir', metavar='path', type=str, default=start_defaults.workdir(), help='If set, workdir location will be removed for all slave daemons (default={}). Note: The home directory of the remote machines is prepended to this path if it is relative.'.format(start_defaults.workdir()))
    stopparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    stopparser.add_argument('--retries', metavar='amount', type=int, default=defaults.retries(), help='Amount of retries to use for risky operations (default={}).'.format(defaults.retries()))
    return [stopparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'stop'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _stop(reservation, args.install_dir, args.key_path, args.workdir, silent=args.silent, retries=args.retries) if reservation else False
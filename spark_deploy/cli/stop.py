import argparse
import datetime

from internal.util.printer import *
import internal.util.fs as fs


'''CLI module to start a cluster.'''


def _cached(response, cached_val):
    return response if response else cached_val


def subparser(subparsers):
    '''Register subparser modules'''
    stopparser = subparsers.add_parser('stop',  help='Stop a given reservation or all reservations that currently running on this machine')
    stopparser.add_argument('-n', '--number', nargs='*', metavar='number', type=int, help='Reservation number to stop. If none given, stops all known reservations.')
    return [stopparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'allocate'


def deploy(parsers, args):
    if args.conf_list:
        print_profiles(args.conf_list)
        return True
    return check_and_allocate(args.time, args.amount, args.location, args.name, args.conf)
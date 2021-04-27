import argparse
import datetime


'''CLI module to start a cluster.'''


# Check if required tools (Java11, Scala12) are available
def check(silent=False):
    a = spk.spark_available()
    b = jv.check_version(minVersion=11, maxVersion=11)
    if a and b:
        if not silent:
            prints('Requirements satisfied')
        return True
    if not silent:
        printe('Requirements not satisfied: ')
        if not a:
            print('\tSpark')
        if not b:
            print('\tJava 11')
        print()
    return False


def check(do_repair):
    reservation = _cli_util.read_reservation_cli()
    if not reservation:
        return False
    # TODO: For each node, check whether Spark is installed and running


def subparser(subparsers):
    '''Register subparser modules'''
    checkparser = subparsers.add_parser('check', help='check whether Spark is running on given reservation.')
    checkparser.add_argument('-ar', '--and-repair', dest='and_repair', help='If problems are detected, also try to fix them.')
    return [checkparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'check'


def deploy(parsers, args):
    if args.conf_list:
        print_profiles(args.conf_list)
        return True
    return check(args.and_repair)
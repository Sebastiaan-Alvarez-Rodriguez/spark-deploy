import argparse

import os
import sys

'''Python CLI module to deploy Spark on GENI resources.'''


sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__)))) # Appends main project root as importpath.


def _get_modules():
    import cli.start as start
    import cli.stop as stop
    import cli.check as check
    return [start, stop, check]


# Register subparser modules
def subparser(parser):
    subparsers = parser.add_subparsers(help='Subcommands', dest='command')
    return [x.subparser(subparsers) for x in _get_modules()]


# Processing of deploy commandline args occurs here
def deploy(mainparser, parsers, args):
    for parsers_for_module, module in zip(parsers, _get_modules()):
        if module.deploy_args_set(args):
            return module.deploy(parsers_for_module, args)
    mainparser.print_help()
    return True


def main():
    parser = argparse.ArgumentParser(
        prog='spark-deploy',
        formatter_class=argparse.RawTextHelpFormatter,
        description='Deploy Spark on clusters'
    )
    retval = True
    geniparsers = subparser(parser)

    args = parser.parse_args()
    retval = deploy(parser, geniparsers, args)

    if isinstance(retval, bool):
        exit(0 if retval else 1)
    elif isinstance(retval, int):
        exit(retval)
    else:
        exit(0 if retval else 1)


if __name__ == '__main__':
    main()
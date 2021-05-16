import spark_deploy.cli.util as _cli_util
import spark_deploy.install as _install
import spark_deploy.internal.defaults.install as defaults

'''CLI module to install Spark and Java on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    installparser = subparsers.add_parser('install', help='Install Spark environment on server cluster.')
    installparser.add_argument('--spark-url', dest='spark_url', type=str, default=defaults.spark_url(), help='Spark download URL.')
    installparser.add_argument('--java-url', dest='java_url', type=str, default=defaults.java_url(), help='Java download URL. Make sure the downloaded version is acceptable (between [`java-min`, `java-max`])')
    installparser.add_argument('--java-min', dest='java_min', type=int, default=defaults.java_min(), help='Java minimal version (default={}). 0 means "no limit". use this to ensure a recent-enough version is installed for use with your Spark version.'.format(defaults.java_min()))
    installparser.add_argument('--java-max', dest='java_max', type=int, default=defaults.java_max(), help='Java minimal version (default={}). 0 means "no limit". use this to ensure a recent-enough version is installed for use with your Spark version.'.format(defaults.java_max()))
    installparser.add_argument('--use-sudo', dest='use_sudo', help='If set, uses superuser-priviledged commands during installation. Otherwise, performs local installs, no superuser privileges required.')
    installparser.add_argument('--force-reinstall', dest='force_reinstall', help='If set, we always will re-download and install Spark. Otherwise, we will skip installing if we already have installed Spark.', action='store_true')
    installparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    installparser.add_argument('--retries', metavar='amount', type=int, default=defaults.retries(), help='Amount of retries to use for risky operations (default={}).'.format(defaults.retries()))
    return [installparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'install'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _install(reservation, args.install_dir, args.key_path, args.spark_url, args.java_url, args.java_min, args.java_max, use_sudo=args.use_sudo, force_reinstall=args.force_reinstall, silent=args.silent, retries=args.retries) if reservation else False
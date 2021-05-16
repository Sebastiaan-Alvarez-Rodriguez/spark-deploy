import subprocess


def submit(run_cmd, cwd):
    env = Environment()
    env.load_to_env()

    try:
        output = subprocess.check_output(run_cmd, shell=True, stderr=subprocess.STDOUT, cwd=cwd).decode('utf-8').strip()
        prints('Application submission succeeded.')
        return True
    except subprocess.CalledProcessError as e:
        print('Could not submit application:\n\tExitcode: {}\n\tOutput: {}'.format(e.returncode, e.output.decode('utf-8')))
        return False
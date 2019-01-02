# coding=utf8
import sys, subprocess

logger = None
def run_command(command, check_status=True, block=True):
    logger and logger.info("run command: '%s'", command)

    if not block:
        subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return

    errors = []
    stdout = None
    try:
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        status = p.returncode
        if check_status and status != 0:
            errors.append((command, str(status), stderr))

        if not stderr is None and len(stderr) > 0:
            stdout = None
            errors.append((command, str(status), stderr))
    except:
        errors.append((command, "", str(sys.exc_info())))
        raise RuntimeError('\n'.join(errors))

    return stdout

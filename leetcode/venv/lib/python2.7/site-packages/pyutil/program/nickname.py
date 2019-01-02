#!/usr/bin/python

import commands

CODE_CMD="lsb_release -c"

def get_codename():
    (status, output) = commands.getstatusoutput(CODE_CMD)
    if status != 0:
        return None
    return output.split(":")[1].strip()


if __name__ == "__main__":
    ret = get_codename()
    print ret

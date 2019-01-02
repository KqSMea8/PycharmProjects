#coding=utf8
from pyutil.program.timeline import emit_timeline
import sys

def submit():
    data = {'start_time' : int(sys.argv[1]),
            'user': sys.argv[2],
            'repo': sys.argv[3],
            'args': sys.argv[4] if len(sys.argv) > 4 else "",
            'changelist': sys.argv[5] if len(sys.argv) > 5 else "",
            'commitlist': sys.argv[6] if len(sys.argv) > 6 else "",
            'reviewer' : sys.argv[7] if len(sys.argv) > 7 else "" ,
            'ttype': 2,
            }
    res = emit_timeline(data)
    #print res

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("you must input start_time, submitter, repo\n")        
        exit(-1)
    submit() 

   



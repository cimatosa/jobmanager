#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function
 
import sys
import time
import multiprocessing as mp
import numpy as np
import traceback
 
from os.path import abspath, dirname, split
# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path
 
from jobmanager import jobmanager, progress

jobmanager.Signal_to_sys_exit(verbose=2)
     
class MyManager_Server(jobmanager.BaseManager):
    pass
 
 
port = int(sys.argv[1])
authkey = sys.argv[2]
 
 
q = jobmanager.myQueue()
MyManager_Server.register('get_q', callable = lambda: q)        
m = MyManager_Server(address = ('', port), authkey = bytearray(authkey, encoding='utf8'))
m.start()
print("Server started (manager process at {})".format(m._process.pid))
sys.stdout.flush()
 
try:
    while True:
        time.sleep(10)
        print(time.time())
        sys.stdout.flush()
except:
    print("Exception")
    sys.stdout.flush()
  
finally:
#     print("stop server ...")
#     pm = m._process
#     m.shutdown()
#     progress.check_process_termination(proc=pm, 
#                                        identifier="autoproxy manager subprocess", 
#                                        timeout=2, 
#                                        verbose=2, 
#                                        auto_kill_on_last_resort=True)
    print("Server stopped")
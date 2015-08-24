#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function
 
import os
import sys

# debug:
print(sys.executable)
print(sys.path)
print(os.environ)

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
print("MyManager_Server started (manager process at {})".format(m._process.pid))
sys.stdout.flush()
 
try:
    while True:
        time.sleep(10)
        print("MyManager_Server is running, time:", time.time())
        sys.stdout.flush()
finally:
    print("MyManager_Server stopping")
    sys.stdout.flush()

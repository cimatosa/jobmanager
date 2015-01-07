#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

from os.path import split, dirname, abspath
from os import getpid
import sys
import time
import multiprocessing as mp

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

import jobmanager as jm

AUTHKEY = 'FunctionCall_Client_exmple'
MAX = 100

def my_func(arg1, const_arg1, arg2, const_arg2, __c, __m, **kwargs):
    print("I'm running in PID {} -- arg1={}, arg2={}, const_arg1={}, const_arg2={}, kwargs={}".format(getpid(), arg1, arg2, const_arg1, const_arg2, kwargs))
    __m.value = MAX
    for i in range(MAX):
        __c.value = i
        time.sleep(0.01)
    return arg1 + arg2



def start_server():
    const_arg = {'const_arg1' : 'ca1',
                 'const_arg2' : 'ca2',
                 'other_const': 123}
    
    const_arg['f'] = my_func
    
    arg_list = [ {'arg1': 1, 'arg2': 10, 'other': -1},
                 {'arg1': 2, 'arg2': 20, 'other': -2},
                 {'arg1': 3, 'arg2': 30, 'other': -3},
                 {'arg1': 4, 'arg2': 40, 'other': -4},
                 {'arg1': 5, 'arg2': 50, 'other': -5},
                 {'arg1': 6, 'arg2': 60, 'other': -6} ] 
    
    with jm.JobManager_Server(authkey=AUTHKEY, const_arg=const_arg, fname_dump=None) as server:
        server.args_from_list(arg_list)
        server.start()

def start_client():
    client = jm.clients.FunctionCall_Client(authkey=AUTHKEY, server='localhost', show_statusbar_for_jobs=False, verbose=2)
    client.start()
    
if __name__ == "__main__":
    p_server = mp.Process(target = start_server)
    p_server.start()
    
    time.sleep(0.4)
    
    p_client = mp.Process(target = start_client)
    p_client.start()
    
    p_client.join()
    p_server.join()
    
    
    
    
    
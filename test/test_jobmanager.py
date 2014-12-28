#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import os
import sys
import time
import signal
import multiprocessing as mp
import numpy as np

from os.path import abspath, dirname, split
# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

from jobmanager import jobmanager, progress


def test_Signal_to_SIG_IGN():
    def f():
        jobmanager.Signal_to_SIG_IGN()
        print("before sleep")
        while True:
            time.sleep(1)
        print("after sleep")

        
    p = mp.Process(target=f)
    p.start()
    time.sleep(0.2)
    assert p.is_alive()
    print("[+] is alive")

    print("    send SIGINT")
    os.kill(p.pid, signal.SIGINT)
    time.sleep(0.2)
    assert p.is_alive()
    print("[+] is alive")
    
    print("    send SIGTERM")
    os.kill(p.pid, signal.SIGTERM)
    time.sleep(0.2)
    assert p.is_alive()
    print("[+] is alive")
    
    print("    send SIGKILL")
    os.kill(p.pid, signal.SIGKILL)
    time.sleep(0.2)
    assert not p.is_alive()
    print("[+] terminated")
    
def test_Signal_to_sys_exit():
    def f():
        jobmanager.Signal_to_sys_exit()
        while True:
            try:
                time.sleep(10)
            except SystemExit:
                print("[+] caught SystemExit, keep running")
            else:
                return
        
    p = mp.Process(target=f)
    p.start()
    time.sleep(0.2)
    assert p.is_alive()
    print("[+] is alive")

    print("    send SIGINT")
    os.kill(p.pid, signal.SIGINT)
    time.sleep(0.2)
    assert p.is_alive()
    print("[+] is alive")
    
    print("    send SIGTERM")
    os.kill(p.pid, signal.SIGTERM)
    time.sleep(0.2)
    assert p.is_alive()
    print("[+] is alive")
    
    print("    send SIGKILL")
    os.kill(p.pid, signal.SIGKILL)
    time.sleep(0.2)
    assert not p.is_alive()
    print("[+] terminated")
    
def test_Signal_to_terminate_process_list():
    def child_proc():
        jobmanager.Signal_to_sys_exit()
        try:
            time.sleep(10)
        except:
            err, val, trb = sys.exc_info()
            print("PID {}: caught Exception {}".format(os.getpid(), err))
            
    def mother_proc():
        n = 3
        p = []
        for i in range(n):
            p.append(mp.Process(target=child_proc))
            p[-1].start()
            
        jobmanager.Signal_to_terminate_process_list(p)
        print("spawned {} processes".format(n))        
        for i in range(n):
            p[i].join()
        print("all joined, mother ends gracefully")
            
    p_mother = mp.Process(target=mother_proc)
    p_mother.start()
    time.sleep(0.5)
    print("send SIGINT")
    os.kill(p_mother.pid, signal.SIGINT)
    

 
def start_server(n, read_old_state=False, verbose=1):
    print("START SERVER")
    args = range(1,n)
    authkey = 'testing'
    with jobmanager.JobManager_Server(authkey=authkey,
                                      verbose=verbose,
                                      msg_interval=1,
                                      fname_dump='jobmanager.dump') as jm_server:
        if not read_old_state:
            jm_server.args_from_list(args)
        else:
            jm_server.read_old_state()
        jm_server.start()
    
def start_client(verbose=1):
    print("START CLIENT")
    jm_client = jobmanager.JobManager_Client(server='localhost', 
                                             authkey='testing', 
                                             port=42524, 
                                             nproc=0, verbose=verbose)
    jm_client.start()    

def test_jobmanager_basic():
    """
    start server, start client, process trivial jobs, quit
    
    check if all arguments are found in final_result of dump
    """
    n = 10
    p_server = mp.Process(target=start_server, args=(n,))
    p_server.start()
    
    time.sleep(1)
     
    p_client = mp.Process(target=start_client)
    p_client.start()
     
    p_client.join(30)
    p_server.join(30)
 
    assert not p_client.is_alive(), "the client did not terminate on time!"
    assert not p_server.is_alive(), "the server did not terminate on time!"
    print("[+] client and server terminated")
     
    fname = 'jobmanager.dump'
    with open(fname, 'rb') as f:
        data = jobmanager.JobManager_Server.static_load(f)
    
    final_res_args_set = {a[0] for a in data['final_result']}
         
    set_ref = set(range(1,n))
     
    intersect = set_ref - final_res_args_set
     
    assert len(intersect) == 0, "final result does not contain all arguments!"
    print("[+] all arguments found in final_results")
    
    
    
def test_jobmanager_server_signals():
    print("## TEST SIGTERM ##")
    p_server = mp.Process(target=start_server, args=(30,))
    p_server.start()
    time.sleep(1)
    print("    send SIGTERM")
    os.kill(p_server.pid, signal.SIGTERM)
    assert p_server.is_alive()
    print("[+] still alive (assume shut down takes some time)")
    p_server.join(15)
    assert not p_server.is_alive(), "timeout for server shutdown reached"
    print("[+] now terminated (timeout of 15s not reached)")
    
    fname = 'jobmanager.dump'
    with open(fname, 'rb') as f:
        data = jobmanager.JobManager_Server.static_load(f)    
    
    args_set = data['args_set']
    ref_set = set(range(1,30))
    
    assert len(args_set) == len(ref_set)
    assert len(ref_set - args_set) == 0
    print("[+] args_set from dump contains all arguments")
    

    print("## TEST SIGINT ##")    
    p_server = mp.Process(target=start_server, args=(30,))
    p_server.start()
    time.sleep(1)
    print("    send SIGINT")
    os.kill(p_server.pid, signal.SIGINT)
    assert p_server.is_alive()
    print("[+] still alive (assume shut down takes some time)")
    p_server.join(15)
    assert not p_server.is_alive(), "timeout for server shutdown reached"
    print("[+] now terminated (timeout of 15s not reached)")
    
    fname = 'jobmanager.dump'
    with open(fname, 'rb') as f:
        data = jobmanager.JobManager_Server.static_load(f)    
    
    args_set = data['args_set']
    
    ref_set = set(range(1,30))
    assert len(args_set) == len(ref_set)
    assert len(ref_set - args_set) == 0
    print("[+] args_set from dump contains all arguments")
 
    
def test_shutdown_server_while_client_running():
    """
    start server with 1000 elements in queue
    
    start client
    
    stop server -> client should catch exception, but can't do anything, 
        writing to fail won't work, because server went down
    
    check if the final_result and the args dump end up to include
    all arguments given 
    """
    
    n = 1000
    
    p_server = mp.Process(target=start_server, args=(n,))
    p_server.start()
    
    time.sleep(1)
    
    p_client = mp.Process(target=start_client)
    p_client.start()
    
    time.sleep(2)
    
    os.kill(p_server.pid, signal.SIGTERM)
    
    p_server.join(15)
    p_client.join(15)
    
    assert not p_server.is_alive()
    assert not p_client.is_alive()
    
    fname = 'jobmanager.dump'
    with open(fname, 'rb') as f:
        data = jobmanager.JobManager_Server.static_load(f)    

    args_set = data['args_set']
    final_result = data['final_result']

    final_res_args = {a[0] for a in final_result}
        
    set_ref = set(range(1,n))
    
    set_recover = set(args_set) | set(final_res_args)
    
    intersec_set = set_ref-set_recover

    if len(intersec_set) == 0:
        print("[+] no arguments lost!")

    assert len(intersec_set) == 0, "NOT all arguments found in dump!"

def test_shutdown_client():
    shutdown_client(signal.SIGTERM)
    shutdown_client(signal.SIGINT)

def shutdown_client(sig):
    """
    start server with 100 elements in queue
    
    start client
    
    stop client -> client should catch exception, interrupts the running worker function,
        reinsert arguments, client terminates
        
    start client again, continues to work on the queue
    
    if server does not terminate on time, something must be wrong with args_set
    check if the final_result contain all arguments given 
    """
    
    n = 300
    
    print("## terminate client with {} ##".format(progress.signal_dict[sig]))
    
    p_server = mp.Process(target=start_server, args=(n, ))
    p_server.start()
    
    time.sleep(2)
    
    p_client = mp.Process(target=start_client)
    p_client.start()
    
    time.sleep(5)
    
    print("    send {}".format(progress.signal_dict[sig]))
    os.kill(p_client.pid, sig)
    assert p_client.is_alive()
    print("[+] still alive (assume shut down takes some time)")
    p_client.join(5)
    assert not p_client.is_alive(), "timeout for client shutdown reached"
    print("[+] now terminated (timeout of 5s not reached)")
    
    time.sleep(0.5)
     
    p_client = mp.Process(target=start_client)
    p_client.start()
    
    p_client.join(30)
    p_server.join(30)
    
    assert not p_client.is_alive()
    assert not p_server.is_alive()
    
    print("[+] client and server terminated")
    
    fname = 'jobmanager.dump'
    with open(fname, 'rb') as f:
        data = jobmanager.JobManager_Server.static_load(f)    
    
    assert len(data['args_set']) == 0
    print("[+] args_set is empty -> all args processed & none failed")
    
    final_res_args_set = {a[0] for a in data['final_result']}
         
    set_ref = set(range(1,n))
     
    intersect = set_ref - final_res_args_set
     
    assert len(intersect) == 0, "final result does not contain all arguments!"
    print("[+] all arguments found in final_results")

def test_check_fail():
    class Client_Random_Error(jobmanager.JobManager_Client):
        def func(self, args, const_args, c, m):
            c.value = 0
            m.value = -1
            fail_on = [3,23,45,67,89]
            time.sleep(0.1)
            if args in fail_on:
                raise RuntimeError("fail_on Error")
            return os.getpid()

    
    n = 100
    verbose=2
    p_server = mp.Process(target=start_server, args=(n,))
    p_server.start()
    
    time.sleep(1)
    
    print("START CLIENT")
    jm_client = Client_Random_Error(server='localhost', 
                                    authkey='testing',
                                    port=42524, 
                                    nproc=0, 
                                    verbose=verbose)
    
    p_client = mp.Process(target=jm_client.start)
    p_client.start()
    
    assert p_server.is_alive()
    assert p_client.is_alive()
    
    print("[+] server and client running")
    
    p_server.join(60)
    p_client.join(60)
    
    assert not p_server.is_alive()
    assert not p_client.is_alive()
    
    print("[+] server and client stopped")
    
    fname = 'jobmanager.dump'
    with open(fname, 'rb') as f:
        data = jobmanager.JobManager_Server.static_load(f)    

    
    set_ref = set(range(1,n))
    
    print(data['args_set'])
    print(data['fail_set'])
    
    assert data['args_set'] == data['fail_set']
    
    final_result_args_set = {a[0] for a in data['final_result']}
    
    all_set = final_result_args_set | data['fail_set']
    
    assert len(set_ref - all_set) == 0, "final result union with reported failure do not correspond to all args!" 
    print("[+] all argumsents found in final_results | reported failure")

def test_jobmanager_read_old_stat():
    """
    start server, start client, start process trivial jobs,
    interrupt in between, restore state from dump, finish.
    
    check if all arguments are found in final_result of dump
    """
    n = 100
    p_server = mp.Process(target=start_server, args=(n,))
    p_server.start()
    
    time.sleep(1)
     
    p_client = mp.Process(target=start_client)
    p_client.start()
    
    time.sleep(3)
    
    p_server.terminate()
     
    p_client.join(10)
    p_server.join(10)
 
    assert not p_client.is_alive(), "the client did not terminate on time!"
    assert not p_server.is_alive(), "the server did not terminate on time!"
    print("[+] client and server terminated")
    
    p_server = mp.Process(target=start_server, args=(n,True))
    p_server.start()
    
    time.sleep(2)
     
    p_client = mp.Process(target=start_client)
    p_client.start()

    p_client.join(30)
    p_server.join(30)
 
    assert not p_client.is_alive(), "the client did not terminate on time!"
    assert not p_server.is_alive(), "the server did not terminate on time!"
    print("[+] client and server terminated")    
     
    fname = 'jobmanager.dump'
    with open(fname, 'rb') as f:
        data = jobmanager.JobManager_Server.static_load(f)
    
    final_res_args_set = {a[0] for a in data['final_result']}
         
    set_ref = set(range(1,n))
     
    intersect = set_ref - final_res_args_set
     
    assert len(intersect) == 0, "final result does not contain all arguments!"
    print("[+] all arguments found in final_results")    
    
def test_hashDict():
    s = set()
    
    d1 = jobmanager.hashDict()
    d1['a'] = 1
    d1['b'] = 2
    s.add(d1)
    
    d2 = jobmanager.hashDict()
    d2['a'] = 2
    d2['b'] = 1
    s.add(d2)
    
    d3 = jobmanager.hashDict()
    d3['a'] = 1
    d3['b'] = 2
    
    assert d3 in s
    
    d3['c'] = 0
    assert not d3 in s
    
def test_hashedViewOnNumpyArray():
    s = set()
    
    a = np.ones(4)
    ah = jobmanager.hashableCopyOfNumpyArray(a)
    
    s.add(ah)
    
    b = np.ones(4, dtype=np.int32)
    bh = jobmanager.hashableCopyOfNumpyArray(b)

    # hash function independent of dtype    
    assert hash(ah) == hash(bh)
    # overwritten equal operator ...
    assert ah == bh
    # ... makes such statements possible!
    assert bh in s
     
    # check if it is truly a copy, not just a view
    b[0] = 2
    assert bh[0] == 1
    
    c = np.ones(5)
    ch = jobmanager.hashableCopyOfNumpyArray(c)
    # different array
    assert not ch in s
    
    # check if shape is included in hash calculation
    bh2 = bh.reshape((2,2))
    assert bh2 not in s
    
    # just some redundant back conversion an checking  
    bh2 = bh2.reshape((4,))
    assert bh2 in s

def test_client_status():
    n = 10
    p_server = mp.Process(target=start_server, args=(n,False,0))
    p_server.start()
    
    time.sleep(1)
    
    class Client_With_Status(jobmanager.JobManager_Client):
        def func(self, args, const_args, c, m):
            m.value = 100
            for i in range(m.value):
                c.value = i+1
                time.sleep(0.05)

            return os.getpid()

    client = Client_With_Status(server='localhost', 
                            authkey='testing',
                            port=42524, 
                            nproc=4, 
                            verbose=1)
    client.start()
    p_server.join()
    
def test_jobmanager_local():
    args = range(1,200)
    authkey = 'testing'
    with jobmanager.JobManager_Local(client_class = jobmanager.JobManager_Client,
                                     authkey=authkey,
                                     verbose=1,
                                     verbose_client=0,
                                     ) as jm_server:
        jm_server.args_from_list(args)
        jm_server.start()
        
def _test_interrupt_server():
    start_server(n = 100)
    
def _test_interrupt_client():
    
    class DoNothing_Client(jobmanager.JobManager_Client):
        @staticmethod
        def func(arg, const_arg):
            while True:
               time.sleep(10) 
   
    c = DoNothing_Client(server='localhost', authkey = 'testing', verbose=2, show_statusbar_for_jobs=True)
    c.start()
        

     
    

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == 'server':
            start_server(n = 100)
    else:    
        func = [
    #     test_Signal_to_SIG_IGN,
    #     test_Signal_to_sys_exit,
    #     test_Signal_to_terminate_process_list,
    #           
    #     test_jobmanager_basic,
    #     test_jobmanager_server_signals,
    #     test_shutdown_server_while_client_running,
    #     test_shutdown_client,
    #     test_check_fail,
    #     test_jobmanager_read_old_stat,
    #     test_hashDict,
    #     test_hashedViewOnNumpyArray,
    #     test_client_status,
    #     test_jobmanager_local,
        lambda : print("END")
        ]
    #     for f in func:
    #         print()
    #         print('#'*80)
    #         print('##  {}'.format(f.__name__))
    #         print()
    #         f()
    #         time.sleep(1)
    
        _test_interrupt_client()
    

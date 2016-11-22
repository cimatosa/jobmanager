#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import os
import sys
import time
import multiprocessing as mp
import socket
import signal
import logging
import datetime
import threading
from numpy import random
import pytest

from os.path import abspath, dirname, split

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

import jobmanager
import binfootprint
import progression as progress

if sys.version_info[0] == 2:
    TIMEOUT = 300
elif sys.version_info[0] == 3:
    TIMEOUT = 15

progress.log.setLevel(logging.ERROR)

from jobmanager.jobmanager import log as jm_log

jm_log.setLevel(logging.WARNING)

import warnings
warnings.filterwarnings('error')
#warnings.filterwarnings('always', category=ImportWarning)

AUTHKEY = 'testing'
PORT = random.randint(10000, 60000)
SERVER = socket.gethostname()

def test_Signal_to_SIG_IGN():
    from jobmanager.jobmanager import Signal_to_SIG_IGN
    global PORT
    PORT += 1
    def f():
        Signal_to_SIG_IGN()
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
    from jobmanager.jobmanager import Signal_to_sys_exit
    global PORT
    PORT += 1
    def f():
        Signal_to_sys_exit()
        while True:
            try:
                time.sleep(10)
            except SystemExit:
                print("[+] caught SystemExit, but for further testing keep running")
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
    from jobmanager.jobmanager import Signal_to_sys_exit
    from jobmanager.jobmanager import Signal_to_terminate_process_list
    global PORT
    PORT += 1
    def child_proc():
        Signal_to_sys_exit()
        try:
            time.sleep(10)
        except:
            err, val, trb = sys.exc_info()
            print("PID {}: caught Exception {}".format(os.getpid(), err))
            
    def mother_proc():
        try:
            n = 3
            p = []
            for i in range(n):
                p.append(mp.Process(target=child_proc))
                p[-1].start()
    
            Signal_to_terminate_process_list(process_list=p, identifier_list=["proc_{}".format(i+1) for i in range(n)])
            print("spawned {} processes".format(n))        
            for i in range(n):
                p[i].join()
            print("all joined, mother ends gracefully")
            sys.exit()
        except SystemExit:
            return
        except Exception as e:
            sys.exit(-1)
            
    p_mother = mp.Process(target=mother_proc)
    p_mother.start()
    time.sleep(0.2)
    assert p_mother.is_alive()
    print("send SIGINT")
    os.kill(p_mother.pid, signal.SIGINT)
    time.sleep(0.2)
    assert not p_mother.is_alive()
    assert p_mother.exitcode == 0
    

 
def start_server(n, read_old_state=False, client_sleep=0.1, hide_progress=False, job_q_on_disk=False):
    print("START SERVER")
    args = range(1,n)
    with jobmanager.JobManager_Server(authkey       = AUTHKEY,
                                      port          = PORT,
                                      msg_interval  = 1,
                                      const_arg     = client_sleep,
                                      fname_dump    = 'jobmanager.dump',
                                      hide_progress = hide_progress,
                                      job_q_on_disk = job_q_on_disk) as jm_server:
        if not read_old_state:
            jm_server.args_from_list(args)
        else:
            jm_server.read_old_state()
        jm_server.start()        
    
def start_client(hide_progress=True):
    print("START CLIENT")
    jm_client = jobmanager.JobManager_Client(server  = SERVER,
                                             authkey = AUTHKEY,
                                             port    = PORT,
                                             nproc   = 3,
                                             reconnect_tries = 0,
                                             hide_progress = hide_progress)
    jm_client.start()
    
def test_start_server_with_no_args():
    n = 10
    args = range(1,n)
    
    with jobmanager.JobManager_Server(authkey      = AUTHKEY,
                                      port         = PORT,
                                      msg_interval = 1,
                                      fname_dump   = 'jobmanager.dump') as jm_server:
        jm_server.start()    
    
def test_start_server():
    n = 10
    args = range(1,n)
    
    def send_SIGINT(pid):
        time.sleep(0.5)
        sys.stderr.write("send SIGINT\n")
        os.kill(pid, signal.SIGINT)    
    thr = threading.Thread(target=send_SIGINT, args=(os.getpid(),))
    thr.daemon = True    
    
    with jobmanager.JobManager_Server(authkey      = AUTHKEY,
                                      port         = PORT,
                                      msg_interval = 1,
                                      fname_dump   = 'jobmanager.dump') as jm_server:
        jm_server.args_from_list(args)       
        thr.start()
        jm_server.start()
    
def test_jobmanager_static_client_call():
    jm_client = jobmanager.JobManager_Client(server  = SERVER,
                                             authkey = AUTHKEY,
                                             port    = PORT,
                                             nproc   = 3,
                                             reconnect_tries = 0)
    jm_client.func(arg=1, const_arg=1)


@pytest.mark.skipif(sys.version_info.major == 2,
                    reason="causes unknown trouble")
def test_client():
    global PORT
    PORT += 1    
    p_server = None
    n = 5

    try:
        # start a server
        p_server = mp.Process(target=start_server, args=(n,False,0.5))
        p_server.start()
        time.sleep(0.5)
        
        jmc = jobmanager.JobManager_Client(server  = SERVER,
                                           authkey = AUTHKEY,
                                           port    = PORT,
                                           nproc   = 3,
                                           reconnect_tries = 0)
        jmc.start()
        
        p_server.join(5)
        assert not p_server.is_alive(), "server did not end on time"
        
    except:
        if p_server is not None:
            p_server.terminate()
        raise


        

def test_jobmanager_basic():
    """
    start server, start client, process trivial jobs, quit
    
    check if all arguments are found in final_result of dump
    """
    global PORT
   
    
    for jqd in [False, True]:
        PORT += 1
        n = 5
        p_server = None
        p_client = None

        try:
            # start a server
            p_server = mp.Process(target=start_server, args=(n,False), kwargs={'job_q_on_disk': jqd})
            p_server.start()
            time.sleep(0.5)
            # server needs to be running
            assert p_server.is_alive()
        
            # start client
            p_client = mp.Process(target=start_client)
            p_client.start()
            
            p_client.join(10)
            # client should have processed all
            if sys.version_info.major == 2:
                p_client.terminate()
                p_client.join(3)
    
            assert not p_client.is_alive(), "the client did not terminate on time!"
            # client must not throw an exception
            assert p_client.exitcode == 0, "the client raised an exception"
            p_server.join(5)
            # server should have come down
            assert not p_server.is_alive(), "the server did not terminate on time!"
            assert p_server.exitcode == 0, "the server raised an exception"
            print("[+] client and server terminated")
              
            fname = 'jobmanager.dump'
            with open(fname, 'rb') as f:
                data = jobmanager.JobManager_Server.static_load(f)
            
            
            final_res_args_set = {a[0] for a in data['final_result']}
            set_ref = set(range(1,n))
            intersect = set_ref - final_res_args_set
              
            assert len(intersect) == 0, "final result does not contain all arguments!"
            print("[+] all arguments found in final_results")
        except:
            if p_server is not None:
                p_server.terminate()
            if p_client is not None:
                p_client.terminate()
            raise
    
def test_jobmanager_server_signals():
    """
        start a server (no client), shutdown, check dump 
    """
    global PORT
    timeout = 5
    n = 15
    sigs = [('SIGTERM', signal.SIGTERM), ('SIGINT', signal.SIGINT)]
    
    for signame, sig in sigs:
        PORT += 1
        p_server = None   
        try:
            print("## TEST {} ##".format(signame))
            p_server = mp.Process(target=start_server, args=(n,))
            p_server.start()
            time.sleep(0.5)
            assert p_server.is_alive()
            print("    send {}".format(signame))
            os.kill(p_server.pid, sig)
            print("[+] still alive (assume shut down takes some time)")
            p_server.join(timeout)
            assert not p_server.is_alive(), "timeout for server shutdown reached"
            assert p_server.exitcode == 0, "the server raised an exception"
            print("[+] now terminated (timeout of {}s not reached)".format(timeout))
            
            fname = 'jobmanager.dump'
            with open(fname, 'rb') as f:
                data = jobmanager.JobManager_Server.static_load(f)    

            from jobmanager.jobmanager import ArgsContainer
            ac = ArgsContainer()
            ac.setstate(data['job_q_state'])

            for a in range(1,n):
                ac.mark(a)

            print("[+] args_set from dump contains all arguments")
        except:
            if p_server is not None:
                p_server.terminate()
            raise
    
def test_shutdown_server_while_client_running():
    """
    start server with 100 elements in queue
    
    start client
    
    stop server -> client should catch exception, but can't do anything, 
        writing to fail won't work, because server went down
        except do emergency dump
    
    check if the final_result and the args dump end up to include
    all arguments given 
    """
    global PORT
   
    n = 100

    sigs = [('SIGTERM', signal.SIGTERM), ('SIGINT', signal.SIGINT)]

    for signame, sig in sigs:
        PORT += 1
    
        p_server = None
        p_client = None
        
        try:
            p_server = mp.Process(target=start_server, args=(n,False,1))
            p_server.start()       
            time.sleep(0.5)
            assert p_server.is_alive()
            
            p_client = mp.Process(target=start_client)
            p_client.start()
            time.sleep(2)
            assert p_client.is_alive()
            
            print("    send {} to server".format(signame))
            os.kill(p_server.pid, sig)
            
            p_server.join(TIMEOUT)
            assert not p_server.is_alive(), "server did not shut down on time"
            assert p_server.exitcode == 0, "the server raised an exception"
            p_client.join(TIMEOUT)
            assert not p_client.is_alive(), "client did not shut down on time"
            assert p_client.exitcode == 0, "the client raised an exception"


            print("[+] server and client joined {}".format(datetime.datetime.now().isoformat()))
            
            fname = 'jobmanager.dump'
            with open(fname, 'rb') as f:
                data = jobmanager.JobManager_Server.static_load(f)    
        
            args_set = set(data['args_dict'].keys())
            final_result = data['final_result']
        
            final_res_args = {binfootprint.dump(a[0]) for a in final_result}
                
            args_ref = range(1,n)
            set_ref = set()
            for a in args_ref:
                set_ref.add(binfootprint.dump(a))    
            
            set_recover = set(args_set) | set(final_res_args)
            
            intersec_set = set_ref-set_recover
        
            if len(intersec_set) == 0:
                print("[+] no arguments lost!")
        
            assert len(intersec_set) == 0, "NOT all arguments found in dump!"
        except:
            if p_server is not None:
                p_server.terminate()
            if p_client is not None:
                p_client.terminate()
            raise

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
    global PORT
    PORT += 1
    n = 30
    
    print("## terminate client with {} ##".format(progress.signal_dict[sig]))

    p_server = None
    p_client = None
    
    try:
    
        p_server = mp.Process(target=start_server, args=(n, False, 0.4))
        p_server.start()

        time.sleep(0.5)

        p_client = mp.Process(target=start_client)
        p_client.start()

        time.sleep(3)

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

        p_client.join(TIMEOUT)
        p_server.join(TIMEOUT)

        assert not p_client.is_alive()
        assert not p_server.is_alive()

        print("[+] client and server terminated")

        fname = 'jobmanager.dump'
        with open(fname, 'rb') as f:
            data = jobmanager.JobManager_Server.static_load(f)

        assert len(data['args_dict']) == 0
        print("[+] args_set is empty -> all args processed & none failed")

        final_res_args_set = {a[0] for a in data['final_result']}

        set_ref = set(range(1,n))

        intersect = set_ref - final_res_args_set

        assert len(intersect) == 0, "final result does not contain all arguments!"
        print("[+] all arguments found in final_results")
    except:
        if p_server is not None:
            p_server.terminate()
        if p_client is not None:
            p_client.terminate()
        raise

def test_jobmanager_read_old_stat():
    """
    start server, start client, start process trivial jobs,
    interrupt in between, restore state from dump, finish.
    
    check if all arguments are found in final_result of dump
    """
    global PORT
    PORT += 1
    n = 50
    p_server = mp.Process(target=start_server, args=(n,))
    p_server.start()
    
    time.sleep(1)
     
    p_client = mp.Process(target=start_client)
    p_client.start()
    
    time.sleep(1.5)

    # terminate server ... to start again using reload_from_dump
    p_server.terminate()
     
    p_client.join(10)
    p_server.join(10)
 
    assert not p_client.is_alive(), "the client did not terminate on time!"
    assert not p_server.is_alive(), "the server did not terminate on time!"
    assert p_client.exitcode == 0
    assert p_server.exitcode == 0
    print("[+] client and server terminated")
    
    time.sleep(1)
    PORT += 1
    # start server using old dump
    p_server = mp.Process(target=start_server, args=(n,True))
    p_server.start()
    
    time.sleep(1)
     
    p_client = mp.Process(target=start_client)
    p_client.start()

    p_client.join(30)
    p_server.join(30)
 
    assert not p_client.is_alive(), "the client did not terminate on time!"
    assert not p_server.is_alive(), "the server did not terminate on time!"
    assert p_client.exitcode == 0
    assert p_server.exitcode == 0
    print("[+] client and server terminated")    
     
    fname = 'jobmanager.dump'
    with open(fname, 'rb') as f:
        data = jobmanager.JobManager_Server.static_load(f)
    
    final_res_args_set = {a[0] for a in data['final_result']}
         
    set_ref = set(range(1,n))
     
    intersect = set_ref - final_res_args_set
    print(intersect)
     
    assert len(intersect) == 0, "final result does not contain all arguments!"
    print("[+] all arguments found in final_results")    


def test_client_status():
    global PORT
    PORT += 1
    n = 10
    p_server = None
    
    try:
        p_server = mp.Process(target=start_server, args=(n,False,None, True))
        p_server.start()
        
        time.sleep(1)
        
        class Client_With_Status(jobmanager.JobManager_Client):
            def func(self, args, const_args, c, m):
                m.value = 30
                for i in range(m.value):
                    c.value = i+1
                    time.sleep(0.05)
     
                return os.getpid()
    
        client = Client_With_Status(server  = SERVER, 
                                    authkey = AUTHKEY,
                                    port    = PORT, 
                                    nproc   = 4)
        client.start()
        p_server.join(5)
        assert not p_server.is_alive()
    except:
        if p_server is not None:
            p_server.terminate()
        raise
    
def test_jobmanager_local():
    global PORT
    PORT += 1
    args = range(1,200)
    with jobmanager.JobManager_Local(client_class = jobmanager.JobManager_Client,
                                     authkey      = AUTHKEY,
                                     port         = PORT,
                                     const_arg    = 0.1) as jm_server:
        jm_server.args_from_list(args)
        jm_server.start()
    
    assert jm_server.all_successfully_processed()
        
def test_start_server_on_used_port():
    global PORT
    PORT += 1
    def start_server():
        const_arg = None
        arg = [10,20,30]
        with jobmanager.JobManager_Server(authkey = AUTHKEY,
                                          port    = PORT, 
                                          const_arg=const_arg,
                                          fname_dump=None) as server:
            server.args_from_list(arg)
            server.start()
            
    def start_server2():
        const_arg = None
        arg = [10,20,30]
        with jobmanager.JobManager_Server(authkey=AUTHKEY,
                                          port = PORT, 
                                          const_arg=const_arg,
                                          fname_dump=None) as server:
            server.args_from_list(arg)
            server.start()
            
    p1 = mp.Process(target=start_server)
    p1.start()
    
    time.sleep(1)
    
    other_error = False
    
    try:
        start_server2()
    except (RuntimeError, OSError) as e:
        print("caught Exception '{}' {}".format(type(e).__name__, e))
    except:
        other_error = True
    
    time.sleep(1)
    p1.terminate()
    time.sleep(1)
    p1.join()    
    
    assert not other_error
        
def test_shared_const_arg():
    global PORT
    PORT += 1
    def start_server():
        const_arg = {1:1, 2:2, 3:3}
        arg = [10,20,30]
        with jobmanager.JobManager_Server(authkey=AUTHKEY,
                                          port = PORT, 
                                          const_arg=const_arg,
                                          fname_dump=None) as server:
            server.args_from_list(arg)
            server.start()
            
        print("const_arg at server side", const_arg)
            
    def start_client():
        class myClient(jobmanager.JobManager_Client):
            @staticmethod
            def func(arg, const_arg):
                const_arg[os.getpid()] = os.getpid() 
                print(os.getpid(), arg, const_arg)
                return None
            
        client = myClient(server=SERVER,
                          authkey=AUTHKEY,
                          port = PORT,
                          nproc=1)
        
        client.start()
            
    PORT += 1
    p1 = mp.Process(target=start_server)
    p2 = mp.Process(target=start_client)
    
    p1.start()
    
    time.sleep(1)
    
    p2.start()
    
    p2.join()
    
    time.sleep(1)
    p1.join()
    
def test_digest_rejected():
    global PORT
    PORT += 1
    n = 10
    p_server = mp.Process(target=start_server, args=(n,False))
    p_server.start()
    
    time.sleep(1)
    
    class Client_With_Status(jobmanager.JobManager_Client):
        def func(self, args, const_args, c, m):
            m.value = 100
            for i in range(m.value):
                c.value = i+1
                time.sleep(0.05)

            return os.getpid()

    client = Client_With_Status(server = SERVER, 
                                authkey = AUTHKEY+' not the same',
                                port    = PORT, 
                                nproc   = 4)
    try:
        client.start()
    except ConnectionError as e:
        print("Not an error: caught '{}' with message '{}'".format(e.__class__.__name__, e))
        p_server.terminate()
        
    p_server.join()
            
def test_hum_size():  
    # bypassing the __all__ clause in jobmanagers __init__
    from jobmanager.jobmanager import humanize_size
      
    assert humanize_size(1) == '1.00kB'
    assert humanize_size(110) == '0.11MB'
    assert humanize_size(1000) == '0.98MB'
    assert humanize_size(1024) == '1.00MB'
    assert humanize_size(1024**2) == '1.00GB'
    assert humanize_size(1024**3) == '1.00TB'
    assert humanize_size(1024**4) == '1024.00TB'

def test_ArgsContainer():
    from jobmanager.jobmanager import ArgsContainer

    for fname in [None, 'argscont']:

        ac = ArgsContainer(fname)
        for arg in 'abcde':
            ac.put(arg)

        assert ac.qsize() == 5

        assert ac.get() == 'a'
        assert ac.get() == 'b'

        assert ac.qsize() == 3
        assert ac.marked_items() == 0
        assert ac.unmarked_items() == 5

        ac.mark('b')
        assert ac.qsize() == 3
        assert ac.marked_items() == 1
        assert ac.unmarked_items() == 4

        ac.mark('a')
        assert ac.qsize() == 3
        assert ac.marked_items() == 2
        assert ac.unmarked_items() == 3

        try:
            ac.mark('a')
        except RuntimeWarning:
            pass
        else:
            assert False

        try:
            ac.put('a')
        except ValueError:
            pass
        else:
            assert False

        import pickle
        ac_dump = pickle.dumps(ac)
        del ac
        ac2 = pickle.loads(ac_dump)

        assert ac2.qsize() == 3
        assert ac2.marked_items() == 2
        assert ac2.unmarked_items() == 3

        assert ac2.get() == 'c'
        assert ac2.get() == 'd'
        assert ac2.get() == 'e'

        assert ac2.qsize() == 0
        assert ac2.marked_items() == 2
        assert ac2.unmarked_items() == 3

        import queue
        try:
            ac2.get()
        except queue.Empty:
            pass
        else:
            assert False

        ac2.clear()


def test_ArgsContainer_BaseManager():
    from jobmanager.jobmanager import ArgsContainer
    from multiprocessing.managers import BaseManager

    global PORT

    class MM(BaseManager):
        pass

    for fname in [None, 'argscont']:
        PORT += 1
        ac_inst = ArgsContainer(fname)
        MM.register('get_job_q', callable=lambda: ac_inst)

        m = MM(('', PORT), b'test_argscomnt')
        m.start()
        ac = m.get_job_q()
        for arg in 'abcde':
            ac.put(arg)

        assert ac.qsize() == 5

        assert ac.get() == 'a'
        assert ac.get() == 'b'

        assert ac.qsize() == 3
        assert ac.marked_items() == 0
        assert ac.unmarked_items() == 5

        ac.mark('b')
        assert ac.qsize() == 3
        assert ac.marked_items() == 1
        assert ac.unmarked_items() == 4

        ac.mark('a')
        assert ac.qsize() == 3
        assert ac.marked_items() == 2
        assert ac.unmarked_items() == 3

        try:
            ac.mark('a')
        except RuntimeWarning:
            pass
        else:
            assert False

        try:
            ac.put('a')
        except ValueError:
            pass
        else:
            assert False

        import pickle
        ac_dump = pickle.dumps(ac.getstate())
        del ac
        ac_state = pickle.loads(ac_dump)
        ac2 = m.get_job_q()
        ac2.setstate(ac_state)


        assert ac2.qsize() == 3
        assert ac2.marked_items() == 2
        assert ac2.unmarked_items() == 3

        assert ac2.get() == 'c'
        assert ac2.get() == 'd'
        assert ac2.get() == 'e'

        assert ac2.qsize() == 0
        assert ac2.marked_items() == 2
        assert ac2.unmarked_items() == 3

        import queue
        try:
            ac2.get()
        except queue.Empty:
            pass
        else:
            assert False

        ac2.clear()




    
if __name__ == "__main__":  
    jm_log.setLevel(logging.INFO)
#     progress.log.setLevel(logging.DEBUG)
#     jm_log.setLevel(logging.ERROR)
    progress.log.setLevel(logging.ERROR)

    if len(sys.argv) > 1:
        pass
    else:    
        func = [
            test_ArgsContainer,
            test_ArgsContainer_BaseManager,
        test_hum_size,
        test_Signal_to_SIG_IGN,
        test_Signal_to_sys_exit,
        test_Signal_to_terminate_process_list,
        test_jobmanager_static_client_call,
        test_start_server_with_no_args,
        test_start_server,
        test_client,
        test_jobmanager_basic,
        test_jobmanager_server_signals,
        test_shutdown_server_while_client_running,
        test_shutdown_client,
        # test_jobmanager_read_old_stat,
        test_client_status,
        test_jobmanager_local,
        test_start_server_on_used_port,
        test_shared_const_arg,
        test_digest_rejected,
        test_hum_size,

        lambda : print("END")
        ]
        for f in func:
            print()
            print('#'*80)
            print('##  {}'.format(f.__name__))
            print()
            f()
            #time.sleep(1)
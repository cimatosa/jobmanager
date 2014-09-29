import os
import sys
import time
import signal
import multiprocessing as mp
import pickle
from numpy.random import rand
import numpy as np
import psutil

path = os.path.dirname(__file__)
sys.path.append(path)

import jobmanager

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
    
def test_loop_basic():
    """
    run function f in loop
    
    check if it is alive after calling start()
    check if it is NOT alive after calling stop()
    """
    f = lambda: print("        I'm process {}".format(os.getpid()))
    loop = jobmanager.Loop(func=f, interval=0.8, verbose=2)
    loop.start()
    pid = loop.getpid()    
    time.sleep(1)
    assert loop.is_alive()
    print("[+] loop started")
    time.sleep(1)
    loop.stop()
    assert not loop.is_alive()
    print("[+] loop stopped")

def test_loop_signals():
    f = lambda: print("        I'm process {}".format(os.getpid()))
    loop = jobmanager.Loop(func=f, interval=0.8, verbose=2, sigint='stop', sigterm='stop')
    
    print("## stop on SIGINT ##")
    loop.start()
    time.sleep(1)
    loop.is_alive()
    
    pid = loop.getpid()
    print("    send SIGINT")
    os.kill(pid, signal.SIGINT)
    time.sleep(1)
    assert not loop.is_alive()
    print("[+] loop stopped running")

    print("## stop on SIGTERM ##")    
    loop.start()
    time.sleep(1)
    pid = loop.getpid()
    print("    send SIGTERM")
    os.kill(pid, signal.SIGTERM)
    time.sleep(1)
    assert not loop.is_alive()
    print("[+] loop stopped running")
    
    print("## ignore SIGINT ##")
    loop = jobmanager.Loop(func=f, interval=0.8, verbose=0, sigint='ign', sigterm='ign')

    loop.start()
    time.sleep(1)
    pid = loop.getpid()
    os.kill(pid, signal.SIGINT)
    print("    send SIGINT")
    time.sleep(1)
    assert loop.is_alive()
    print("[+] loop still running")
    print("    send SIGKILL")
    os.kill(pid, signal.SIGKILL)
    time.sleep(1)
    assert not loop.is_alive()
    print("[+] loop stopped running")
    
    print("## ignore SIGTERM ##")
    loop.start()
    time.sleep(1)
    pid = loop.getpid()
    print("    send SIGTERM")
    os.kill(pid, signal.SIGTERM)
    time.sleep(1)
    assert loop.is_alive()
    print("[+] loop still running")
    print("    send SIGKILL")    
    os.kill(pid, signal.SIGKILL)
    time.sleep(1)
    assert not loop.is_alive()
    print("[+] loop stopped running")


def non_stopping_function():
    print("        I'm pid", os.getpid())
    print("        I'm NOT going to stop")
    
    while True:         # sleep will be interrupted by signal
        time.sleep(1)   # while True just calls sleep again
                        # only SIGKILL helps
                        

def normal_function():
    print("        I'm pid", os.getpid())
    
def long_sleep_function():
    print("        I'm pid", os.getpid())
    print("        I will sleep for seven years")
    time.sleep(60*60*12*356*7)
    
def test_loop_normal_stop():
    with jobmanager.Loop(func=normal_function, 
                         verbose=2,
                         name='loop') as loop:
        loop.start()
        time.sleep(0.1)
        assert loop.is_alive()
        print("[+] normal loop running")
        
    assert not loop.is_alive()
    print("[+] normal loop stopped")
    
def test_loop_need_sigterm_to_stop():
    with jobmanager.Loop(func=long_sleep_function, 
                         verbose=2,
                         name='loop') as loop:
        loop.start()
        time.sleep(0.1)
        assert loop.is_alive()
        print("[+] sleepy loop running")
        
    assert not loop.is_alive()
    print("[+] sleepy loop stopped")
    
def test_loop_need_sigkill_to_stop():
    with jobmanager.Loop(func=non_stopping_function, 
                         verbose=2,
                         name='loop',
                         auto_kill_on_last_resort=True) as loop:
        loop.start()
        time.sleep(0.1)
        assert loop.is_alive()
        print("[+] NON stopping loop running")

    assert not loop.is_alive()
    print("[+] NON stopping loop stopped")
        
def test_why_with_statement():
    class ErrorLoop(jobmanager.Loop):
        def raise_error(self):
            raise RuntimeError("on purpose error")
    v=2
    
    def t(shared_mem_pid):
        l = ErrorLoop(func=normal_function, verbose=v)
        l.start()
        time.sleep(0.2)
        shared_mem_pid.value = l.getpid()
        l.raise_error()
        l.stop()
        
    def t_with():
        with ErrorLoop(func=normal_function, verbose=v) as l:
            l.start()
            time.sleep(0.2)
            l.raise_error()
            l.stop()
        
    print("## start without with statement ...")
    subproc_pid = jobmanager.UnsignedIntValue()
    
    p = mp.Process(target=t, args=(subproc_pid, ))
    p.start()
    time.sleep(0.3)
    print("## now an exception gets raised ... but you don't see it!")
    time.sleep(3)
    print("## ... and the loop is still running so we have to kill the process")
    p.terminate()
    print("## ... done!")
    p_sub = psutil.Process(subproc_pid.value)
    if p_sub.is_running():
        print("## terminate loop process from extern ...")
        p_sub.terminate()
        
    p_sub.wait(1)
    assert not p_sub.is_running()
    print("## process with PID {} terminated!".format(subproc_pid.value))
    
    time.sleep(3)
    
    print("\n##\n## now to the same with the with statement ...")
    p = mp.Process(target=t_with)
    p.start()
    
    time.sleep(3)
    print("## no special care must be taken ... cool eh!")
    
    print("## ALL DONE! (and now comes some exception, strange!)")
    
    
def test_statusbar():
    """
    deprecated
    """
    count = jobmanager.UnsignedIntValue()
    max_count = jobmanager.UnsignedIntValue(100)
    sb = jobmanager.StatusBar(count, max_count, verbose=2)
    assert not sb.is_alive()
    
    sb.start()
    time.sleep(0.2)
    assert sb.is_alive()
    pid = sb.getpid()
    
    sb.start()
    time.sleep(0.2)
    assert pid == sb.getpid()
    
    sb.stop()
    assert not sb.is_alive()
    
    time.sleep(0.2)
    sb.stop()
    
def test_statusbar_with_statement():
    count = jobmanager.UnsignedIntValue()
    max_count = jobmanager.UnsignedIntValue(100)
    with jobmanager.StatusBar(count, max_count, verbose=2) as sb:
        assert not sb.is_alive()
    
        sb.start()
        time.sleep(0.2)
        assert sb.is_alive()
        pid = sb.getpid()
    
        sb.start()
        time.sleep(0.2)
        assert pid == sb.getpid()
    
    assert not sb.is_alive()
    
    time.sleep(0.2)
    sb.stop()
 
def start_server(n, read_old_state=False):
    print("START SERVER")
    args = range(1,n)
    authkey = 'testing'
    with jobmanager.JobManager_Server(authkey=authkey,
                                      verbose=2,
                                      msg_interval=1,
                                      fname_dump='jobmanager.dump') as jm_server:
        if not read_old_state:
            jm_server.args_from_list(args)
        else:
            jm_server.read_old_state()
        jm_server.start()
    
def start_client():
    print("START CLIENT")
    jm_client = jobmanager.JobManager_Client(server='localhost', 
                                             authkey='testing', 
                                             port=42524, 
                                             nproc=0, verbose=2)
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
    
    print("## terminate client with {} ##".format(jobmanager.signal_dict[sig]))
    
    p_server = mp.Process(target=start_server, args=(n, ))
    p_server.start()
    
    time.sleep(2)
    
    p_client = mp.Process(target=start_client)
    p_client.start()
    
    time.sleep(5)
    
    print("    send {}".format(jobmanager.signal_dict[sig]))
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
        def func(self, args, const_args):
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
    
    
    
def test_sb_multi():
    n = 4
    max_count_value = 100
    
    count = []
    max_count = []
    prepend = []
    for i in range(n):
        count.append(jobmanager.UnsignedIntValue(0))
        max_count.append(jobmanager.UnsignedIntValue(max_count_value))
        prepend.append('{}: '.format(i))
    
    
    
    sbm = jobmanager.StatusBarMulti(count=count,
                                    max_count=max_count,
                                    interval=0.2,
                                    speed_calc_cycles=10,
                                    width='auto',
                                    verbose=1,
                                    sigint='stop',
                                    sigterm='stop',
                                    name='sb multi',
                                    prepend=prepend)
    
    sbm.start()
    
    while True:
        i = np.random.randint(low=0, high=n)
        with count[i].get_lock():
            count[i].value += 1
            
        if count[i].value > 100:
            sbm.reset(i)
            print(count[i].value)
            
            sys.exit()
            
        time.sleep(0.002)
        
    
def test_sb():
    max_count_value = 100
    count = jobmanager.UnsignedIntValue(0)
    max_count = jobmanager.UnsignedIntValue(max_count_value)
    prepend = '{}: '.format(0)
    
    sbm = jobmanager.StatusBar(count=count,
                                    max_count=max_count,
                                    interval=0.2,
                                    speed_calc_cycles=10,
                                    width='auto',
                                    verbose=2,
                                    sigint='stop',
                                    sigterm='stop',
                                    name='sb',
                                    prepend=prepend)
    
    sbm.start()
    
    while True:
        with count.get_lock():
            count.value += 1
            
        if count.value > 50:
            sbm.reset()
            sys.exit()
           
        time.sleep(0.02)


if __name__ == "__main__":
    
#     test_Signal_to_SIG_IGN()
#     test_Signal_to_sys_exit()
#     test_Signal_to_terminate_process_list()
#        
#     test_loop_basic()
#     test_loop_signals()
#     test_loop_normal_stop()
#     test_loop_need_sigterm_to_stop()
#     test_loop_need_sigkill_to_stop()
#      
#     test_why_with_statement()
#       
#     test_statusbar()
#     test_statusbar_with_statement()
      
#     test_jobmanager_basic()
#     test_jobmanager_server_signals()
#     test_shutdown_server_while_client_running()
#     test_shutdown_client()
#     test_check_fail()
#     test_jobmanager_read_old_stat()
#     test_hashDict()
#     test_hashedViewOnNumpyArray()
#     test_sb_multi()
    test_sb()
    pass
    
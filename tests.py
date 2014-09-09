import jobmanager
import os
import sys
import time
import signal
import multiprocessing as mp
import pickle
from numpy.random import rand

def test_loop_basic():
    """
    run function f in loop
    
    check if it is alive after calling start()
    check if it is NOT alive after calling stop()
    """
    f = lambda: print("        I'm process {}".format(os.getpid()))
    loop = jobmanager.Loop(func=f, interval=0.8, verbose=0)
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
    loop = jobmanager.Loop(func=f, interval=0.8, verbose=0, sigint='stop', sigterm='stop')
    
    print("## stop on SIGINT ##")
    loop.start()
    time.sleep(1)
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
    
def start_server(verbose, n=30):
    print("START SERVER")
    args = range(1,n)
    authkey = 'testing'
    jm_server = jobmanager.JobManager_Server(authkey=authkey,
                                             fname_for_final_result_dump='final_result.dump',
                                             verbose=verbose, 
                                             fname_for_args_dump='args.dump',
                                             fname_for_fail_dump='fail.dump')
    jm_server.args_from_list(args)
    jm_server.start()
    
def start_client(verbose):
    print("START CLIENT")
    jm_client = jobmanager.JobManager_Client(ip='localhost', authkey='testing', port=42524, nproc=0, verbose=verbose)
    jm_client.start()    

def test_jobmanager_basic():
    """
    start server, start client, process trivial jobs, quit
    """
    n = 10
    p_server = mp.Process(target=start_server, args=(0,n))
    p_server.start()
    
    time.sleep(1)
    
    p_client = mp.Process(target=start_client, args=(0,))
    p_client.start()
    
    p_client.join(30)
    p_server.join(30)

    assert not p_client.is_alive(), "the client did not terminate on time!"
    assert not p_server.is_alive(), "the server did not terminate on time!"
    print("[+] client and server terminated")
    
    fname = 'final_result.dump'
    with open(fname, 'rb') as f:
        final_res = pickle.load(f)
    
    final_res_args_set = {a[0] for a in final_res}
        
    set_ref = set(range(1,n))
    
    intersect = set_ref - final_res_args_set
    
    assert len(intersect) == 0, "final result does not contain all arguments!"
    print("[+] all arguments found in final_results")
    
    
    
def test_jobmanager_server_signals():
    print("## TEST SIGTERM ##")
    p_server = mp.Process(target=start_server, args=(0,))
    p_server.start()
    time.sleep(1)
    print("    send SIGTERM")
    os.kill(p_server.pid, signal.SIGTERM)
    assert p_server.is_alive()
    print("[+] still alive (assume shut down takes some time)")
    p_server.join(15)
    assert not p_server.is_alive(), "timeout for server shutdown reached"
    print("[+] now terminated (timeout of 15s not reached)")
    
    fname = 'args.dump'
    with open(fname, 'rb') as f:
        args = pickle.load(f)
    
    for i,a in enumerate(range(1,30)):
        assert a == args[i], "checking the args.dump failed"
    print("[+] args.dump contains all arguments")
    

    print("## TEST SIGINT ##")    
    p_server = mp.Process(target=start_server, args=(0,))
    p_server.start()
    time.sleep(1)
    print("    send SIGINT")
    os.kill(p_server.pid, signal.SIGINT)
    assert p_server.is_alive()
    print("[+] still alive (assume shut down takes some time)")
    p_server.join(15)
    assert not p_server.is_alive(), "timeout for server shutdown reached"
    print("[+] now terminated (timeout of 15s not reached)")
    
    fname = 'args.dump'
    with open(fname, 'rb') as f:
        args = pickle.load(f)
    
    for i,a in enumerate(range(1,30)):
        assert a == args[i], "checking the args.dump failed"
    print("[+] args.dump contains all arguments")   
    
def test_shutdown_server_while_client_running():
    """
    start server with 1000 elements in queue
    
    start client
    
    stop server -> client should catch exception, but can't do anything, 
        writing to fail won't work, because server went down
    
    check if the final_result and the args dump end up to include
    all arguments given 
    """
    p_server = mp.Process(target=start_server, args=(0,1000))
    p_server.start()
    
    time.sleep(1)
    
    p_client = mp.Process(target=start_client, args=(0,))
    p_client.start()
    
    time.sleep(2)
    
    os.kill(p_server.pid, signal.SIGTERM)
    
    p_server.join(15)
    p_client.join(15)
    
    assert not p_server.is_alive()
    assert not p_client.is_alive()

    fname = 'args.dump'
    with open(fname, 'rb') as f:
        args = pickle.load(f)
        
    fname = 'final_result.dump'
    with open(fname, 'rb') as f:
        final_res = pickle.load(f)
    
    final_res_args = [a[0] for a in final_res]
        
    set_ref = set(range(1,1000))
    
    set_recover = set(args) | set(final_res_args)
    
    intersec_set = set_ref-set_recover

    if len(intersec_set) == 0:
        print("[+] no arguments lost!")

    assert len(intersec_set) == 0, "args.dump and final_result_dump do NOT contain all arguments -> some must have been lost!"

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
    
    n = 100
    
    print("## terminate client with {} ##".format(jobmanager.signal_dict[sig]))
    
    p_server = mp.Process(target=start_server, args=(0,n))
    p_server.start()
    
    time.sleep(0.5)
    
    p_client = mp.Process(target=start_client, args=(0,))
    p_client.start()
    
    time.sleep(1)
    
    print("    send {}".format(jobmanager.signal_dict[sig]))
    os.kill(p_client.pid, sig)
    assert p_client.is_alive()
    print("[+] still alive (assume shut down takes some time)")
    p_client.join(5)
    assert not p_client.is_alive(), "timeout for client shutdown reached"
    print("[+] now terminated (timeout of 5s not reached)")
    
    time.sleep(0.5)
     
    p_client = mp.Process(target=start_client, args=(0,))
    p_client.start()
    
    p_client.join(30)
    p_server.join(30)
    
    print("[+] client and server terminated")
    
    fname = 'final_result.dump'
    with open(fname, 'rb') as f:
        final_res = pickle.load(f)
    
    final_res_args_set = {a[0] for a in final_res}
        
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
    verbose=0
    p_server = mp.Process(target=start_server, args=(0,n))
    p_server.start()
    
    time.sleep(1)
    
    print("START CLIENT")
    jm_client = Client_Random_Error(ip='localhost', 
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
    
    fname = 'final_result.dump'
    with open(fname, 'rb') as f:
        final_res = pickle.load(f)
    
    final_res_args_set = {a[0] for a in final_res}
    
    fname = 'fail.dump'
    with open(fname, 'rb') as f:
        fail = pickle.load(f)
    
    fail_set = {a[0] for a in fail}
        
    set_ref = set(range(1,n))
    
    all_set = final_res_args_set | fail_set
    assert len(set_ref - all_set) == 0, "final result union with reported failure do not correspond to all args!" 
    print("[+] all arguments found in final_results | reported failure")

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


if __name__ == "__main__":
    test_Signal_to_SIG_IGN()
    test_Signal_to_sys_exit()
    test_Signal_to_terminate_process_list()
    test_loop_basic()
    test_loop_signals()
    test_jobmanager_basic()
    test_jobmanager_server_signals()
    test_shutdown_server_while_client_running()
    test_shutdown_client()
    test_check_fail()
    pass
    
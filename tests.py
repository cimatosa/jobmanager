import jobmanager
import os
import time
import signal
import multiprocessing as mp
import pickle

def test_loop_basic():
# 2nd change
    """
    run function f in loop
    
    check if it is alive after calling start()
    check if it is NOT alive after calling stop()
    """
    f = lambda: print("I'm process {}".format(os.getpid()))
    loop = jobmanager.Loop(func=f, interval=0.8, verbose=2)
    loop.start()
    pid = loop.getpid()    
    time.sleep(2)
    assert loop.is_alive()
    loop.stop()
    assert not loop.is_alive()
    
def test_loop_signals():
    f = lambda: print("I'm process {}".format(os.getpid()))
    loop = jobmanager.Loop(func=f, interval=0.8, verbose=2, sigint='stop', sigterm='stop')
    
    print("TEST: stop on SIGINT\n")
    loop.start()
    time.sleep(1)
    pid = loop.getpid()
    print("send SIGINT")
    os.kill(pid, signal.SIGINT)
    time.sleep(1)
    assert not loop.is_alive()
    print("loop stopped running")
    print("-"*40)

    print("\nTEST: stop on SIGTERM\n")    
    loop.start()
    time.sleep(1)
    pid = loop.getpid()
    print("send SIGTERM")
    os.kill(pid, signal.SIGTERM)
    time.sleep(1)
    assert not loop.is_alive()
    print("loop stopped running")
    print("-"*40)
    
    print("\nTEST: ignore SIGINT\n")
    loop = jobmanager.Loop(func=f, interval=0.8, verbose=2, sigint='ign', sigterm='ign')

    loop.start()
    time.sleep(1)
    pid = loop.getpid()
    os.kill(pid, signal.SIGINT)
    print("send SIGINT")
    time.sleep(1)
    assert loop.is_alive()
    print("loop still running")
    print("send SIGKILL")
    os.kill(pid, signal.SIGKILL)
    time.sleep(1)
    assert not loop.is_alive()
    print("loop stopped running")
    print("-"*40)
    
    print("\nTEST: ignore SIGTERM\n")
    loop.start()
    time.sleep(1)
    pid = loop.getpid()
    print("send SIGTERM")
    os.kill(pid, signal.SIGTERM)
    time.sleep(1)
    assert loop.is_alive()
    print("loop still running")
    print("send SIGKILL")    
    os.kill(pid, signal.SIGKILL)
    time.sleep(1)
    assert not loop.is_alive()
    print("loop stopped running")
    print("-"*40)    
    
def start_server(verbose, n=30):
    print("\nSTART SERVER")
    args = range(1,n)
    authkey = 'testing'
    jm_server = jobmanager.JobManager_Server(authkey=authkey,
                                             fname_for_final_result_dump='final_result.dump',
                                             verbose=verbose, 
                                             fname_for_job_q_dump='job_q.dump')
    jm_server.args_from_list(args)
    jm_server.start()
    
def start_client(verbose):
    print("\nSTART CLIENT")
    jm_client = jobmanager.JobManager_Client(ip='localhost', authkey='testing', port=42524, nproc=0, verbose=verbose)
    jm_client.start()    

def test_jobmanager_basic():
    """
    start server, start client, process trivial jobs, quit
    """
    p_server = mp.Process(target=start_server, args=(2,))
    p_server.start()
    
    time.sleep(1)
    
    p_client = mp.Process(target=start_client, args=(2,))
    p_client.start()
    
    p_client.join(30)
    p_server.join(30)
    
    assert not p_client.is_alive()
    assert not p_server.is_alive()
    
def test_jobmanager_server_signals():
    print("\nTEST SIGTERM\n")
    p_server = mp.Process(target=start_server, args=(2,))
    p_server.start()
    time.sleep(1)
    print("SEND SIGTERM")
    os.kill(p_server.pid, signal.SIGTERM)
    assert p_server.is_alive()
    print("STILL ALIVE (assume shut down takes some time)")
    p_server.join(15)
    assert not p_server.is_alive()
    print("NOW TERMINATED (timeout of 15s not reached)")
    
    fname = 'job_q.dump'
    with open(fname, 'rb') as f:
        args = pickle.load(f)
    
    print("check job_q.dump ... ", end='', flush=True)    
    for i,a in enumerate(range(1,30)):
        assert a == args[i]
    print("done!")
    

    print("\nTEST SIGINT\n")    
    p_server = mp.Process(target=start_server, args=(2,))
    p_server.start()
    time.sleep(1)
    print("SEND SIGINT")
    os.kill(p_server.pid, signal.SIGINT)
    assert p_server.is_alive()
    print("STILL ALIVE (assume shut down takes some time)")
    p_server.join(15)
    assert not p_server.is_alive()
    print("NOW TERMINATED (timeout of 15s not reached)")
    
    fname = 'job_q.dump'
    with open(fname, 'rb') as f:
        args = pickle.load(f)
    
    print("check job_q.dump ... ", end='', flush=True)    
    for i,a in enumerate(range(1,30)):
        assert a == args[i]    
    print("done!")    
    
def test_shutdown_server_while_client_running():
    """
    start server with 1000 elements in queue
    
    start client
    
    stop server -> client should catch exception, but can't do anything, 
        writing to fail_q won't work, because server went down
    
    check if the final_result and the job_q dump end up to include
    all arguments given 
    """
    p_server = mp.Process(target=start_server, args=(2,1000))
    p_server.start()
    
    time.sleep(1)
    
    p_client = mp.Process(target=start_client, args=(2,))
    p_client.start()
    
    time.sleep(2)
    
    os.kill(p_server.pid, signal.SIGTERM)
    
    p_server.join(15)
    p_client.join(15)
    
    assert not p_server.is_alive()
    assert not p_client.is_alive()

    fname = 'job_q.dump'
    with open(fname, 'rb') as f:
        args = pickle.load(f)
        
    fname = 'final_result.dump'
    with open(fname, 'rb') as f:
        final_res = pickle.load(f)
    
    final_res_args = [a[0] for a in final_res]
        
    set_ref = set(range(1,1000))
    
    set_recover = set(args) | set(final_res_args)
    
    intersec_set = set_ref-set_recover
    print(len(intersec_set))
    print(intersec_set)

    assert(len(intersec_set) == 0)

if __name__ == "__main__":
#     test_loop_basic()
#     test_loop_signals()
    test_jobmanager_basic()
    test_jobmanager_server_signals()
    test_shutdown_server_while_client_running()
    
"""
MEMO:
    client shuts down early -> reinsert argument
    
    client catches exception -> save trace back, report to fail_q, process next argument
    
    everything from the fail_q is also considered process
        - remove from args_set
        - keep track for summary at the end 

"""


#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import multiprocessing as mp
import numpy as np
import os
import warnings

try:
    import psutil
except ImportError:
    warnings.warn("can not import 'psutil' -> some tests will not work")
    
import signal
import sys
import time
import traceback

from os.path import abspath, dirname, split

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

from jobmanager import progress

def _safe_assert_not_loop_is_alive(loop):
    try:
        assert not loop.is_alive()
    except AssertionError:
        os.kill(loop.getpid(), signal.SIGKILL)
        raise
    

def test_loop_basic():
    """
    run function f in loop
    
    check if it is alive after calling start()
    check if it is NOT alive after calling stop()
    """
    f = lambda: print("        I'm process {}".format(os.getpid()))
    loop = progress.Loop(func=f, interval=0.8, verbose=2)
    loop.start()
    
    time.sleep(1)

    assert loop.is_alive()
    print("[+] loop started")
    
    time.sleep(1)
    
    loop.stop()
    
    _safe_assert_not_loop_is_alive(loop)  
    print("[+] loop stopped")

def test_loop_signals():
    f = lambda: print("        I'm process {}".format(os.getpid()))
    loop = progress.Loop(func=f, interval=0.8, verbose=2, sigint='stop', sigterm='stop')
    
    print("## stop on SIGINT ##")
    loop.start()
    time.sleep(1)
    loop.is_alive()
    
    pid = loop.getpid()
    print("    send SIGINT")
    os.kill(pid, signal.SIGINT)
    time.sleep(1)
    _safe_assert_not_loop_is_alive(loop)
    print("[+] loop stopped running")

    print("## stop on SIGTERM ##")    
    loop.start()
    time.sleep(1)
    pid = loop.getpid()
    print("    send SIGTERM")
    os.kill(pid, signal.SIGTERM)
    time.sleep(1)
    _safe_assert_not_loop_is_alive(loop)
    print("[+] loop stopped running")
    
    print("## ignore SIGINT ##")
    loop = progress.Loop(func=f, interval=0.8, verbose=0, sigint='ign', sigterm='ign')

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
    with progress.Loop(func    = normal_function, 
                       verbose = 2,
                       name    = 'loop') as loop:
        loop.start()
        time.sleep(0.1)
        assert loop.is_alive()
        print("[+] normal loop running")
    
    _safe_assert_not_loop_is_alive(loop)
    print("[+] normal loop stopped")
    
def test_loop_need_sigterm_to_stop():
    with progress.Loop(func    = long_sleep_function, 
                       verbose = 2,
                       name    = 'loop') as loop:
        loop.start()
        time.sleep(0.1)
        assert loop.is_alive()
        print("[+] sleepy loop running")
        
    _safe_assert_not_loop_is_alive(loop)
    print("[+] sleepy loop stopped")
    
def test_loop_need_sigkill_to_stop():
    with progress.Loop(func                     = non_stopping_function, 
                       verbose                  = 2,
                       name                     = 'loop',
                       auto_kill_on_last_resort = True) as loop:
        loop.start()
        time.sleep(0.1)
        assert loop.is_alive()
        print("[+] NON stopping loop running")

    _safe_assert_not_loop_is_alive(loop)
    print("[+] NON stopping loop stopped")
        
def test_why_with_statement():
    """
        here we demonstrate why you should use the with statement
    """
    class ErrorLoop(progress.Loop):
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
        
    def t_with(shared_mem_pid):
        with ErrorLoop(func=normal_function, verbose=v) as l:
            l.start()
            time.sleep(0.2)
            shared_mem_pid.value = l.getpid()
            l.raise_error()
            l.stop()
        
    print("## start without with statement ...")
    
    # the pid of the loop process, which is spawned inside 't'
    subproc_pid = progress.UnsignedIntValue()
    
    p = mp.Process(target=t, args=(subproc_pid, ))
    p.start()
    time.sleep(0.3)
    print("## now an exception gets raised ... but you don't see it!")
    time.sleep(3)
    print("## ... and the loop is still running so we have to kill the process")
    
    p.terminate()
    p.join(1)
    
    try:
        assert not p.is_alive()
        print("## ... done!")
        p_sub = psutil.Process(subproc_pid.value)
        if p_sub.is_running():
            print("## terminate loop process from extern ...")
            p_sub.terminate()
        
            p_sub.wait(1)
            assert not p_sub.is_running()
            print("## process with PID {} terminated!".format(subproc_pid.value))
    except:
        pass
    else:
        if p_sub.is_running():
            os.kill(subproc_pid.value, signal.SIGKILL)        
    finally:
        if p.is_alive():
            os.kill(p.pid, signal.SIGKILL)

    
    time.sleep(3)
    
    print("\n##\n## now to the same with the with statement ...")
    p = mp.Process(target=t_with, args=(subproc_pid, ))
    p.start()
    
    time.sleep(3)
    print("## no special care must be taken ... cool eh!")
    
    print("## ALL DONE! (there is no control when the exception from the loop get printed)")
    
    p.join(1)
    
    try:
        assert not p.is_alive()
    finally:
        if p.is_alive():
            # kill loop
            p_sub = psutil.Process(subproc_pid.value)
            if p_sub.is_running():
                os.kill(subproc_pid.value, signal.SIGKILL)    
            # kill sub process
            os.kill(p.pid, signal.SIGKILL)
        
    
    
def test_progress_bar():
    """
    deprecated, due to missing with
    """
    count = progress.UnsignedIntValue()
    max_count = progress.UnsignedIntValue(100)
    sb = progress.ProgressBar(count, max_count, verbose=2)
    assert not sb.is_alive()
    
    sb.start()
    time.sleep(2)
    assert sb.is_alive()
    pid = sb.getpid()
    
    # call start on already running PB
    sb.start()
    time.sleep(2)
    assert pid == sb.getpid()
    
    sb.stop()
    _safe_assert_not_loop_is_alive(sb)
    
    time.sleep(2)
    # call stop on not running PB
    sb.stop()
    time.sleep(2)
    
def test_progress_bar_with_statement():
    count = progress.UnsignedIntValue()
    max_count = progress.UnsignedIntValue(100)
    with progress.ProgressBar(count, max_count, verbose=2) as sb:
        assert not sb.is_alive()
    
        sb.start()
        time.sleep(0.2)
        assert sb.is_alive()
        pid = sb.getpid()
        
        # call start on already running PB
        sb.start()
        time.sleep(0.2)
        assert pid == sb.getpid()
    
    _safe_assert_not_loop_is_alive(sb)
    
    time.sleep(0.2)
    sb.stop()
    
def test_progress_bar_multi():
    n = 4
    max_count_value = 100
    
    count = []
    max_count = []
    prepend = []
    for i in range(n):
        count.append(progress.UnsignedIntValue(0))
        max_count.append(progress.UnsignedIntValue(max_count_value))
        prepend.append('_{}_: '.format(i))
    
    with progress.ProgressBar(count=count,
                              max_count=max_count,
                              interval=0.2,
                              speed_calc_cycles=10,
                              width='auto',
                              verbose=0,
                              sigint='stop',
                              sigterm='stop',
                              name='sb multi',
                              prepend=prepend) as sbm:
    
        sbm.start()
        
        for x in range(500):
            i = np.random.randint(low=0, high=n)
            with count[i].get_lock():
                count[i].value += 1
                
            if count[i].value > 100:
                sbm.reset(i)
                
            time.sleep(0.02)
        
           
def test_status_counter():
    c = progress.UnsignedIntValue(val=0)
    m = None
    
    with progress.ProgressBar(count=c,
                                  max_count=m,
                                  interval=0.2,
                                  speed_calc_cycles=100,
                                  verbose=2,
                                  sigint='ign',
                                  sigterm='ign',
                                  name='sc',
                                  prepend='') as sc:

        sc.start()
        while True:
            with c.get_lock():
                c.value += 1
                
            if c.value == 100:
                break
            
            time.sleep(0.01)
            
def test_status_counter_multi():
    c1 = progress.UnsignedIntValue(val=0)
    c2 = progress.UnsignedIntValue(val=0)
    
    c = [c1, c2]
    prepend = ['c1: ', 'c2: ']
    with progress.ProgressBar(count=c, prepend=prepend, verbose=2) as sc:
        sc.start()
        while True:
            i = np.random.randint(0,2)
            with c[i].get_lock():
                c[i].value += 1
                
            if c[0].value == 100:
                break
            
            time.sleep(0.01)
            
def test_intermediate_prints_while_running_progess_bar():
    c = progress.UnsignedIntValue(val=0)
    try:
        with progress.ProgressBar(count=c, verbose=2, interval=0.3) as sc:
            sc.start()
            while True:
                with c.get_lock():
                    c.value += 1
                    
                if c.value == 100:
                    print("intermediate message")
                    
                if c.value == 400:
                    break
                
                time.sleep(0.01)    
    except:
        print("IN EXCEPTION TEST")
        traceback.print_exc()
            
            
def test_intermediate_prints_while_running_progess_bar_multi():
    c1 = progress.UnsignedIntValue(val=0)
    c2 = progress.UnsignedIntValue(val=0)
    
    c = [c1,c2]
    with progress.ProgressBar(count=c, verbose=2, interval=0.3) as sc:
        sc.start()
        while True:
            i = np.random.randint(0,2)
            with c[i].get_lock():
                c[i].value += 1
                
            if c[0].value == 100:
                print("intermediate message")
                with c[i].get_lock():
                    c[i].value += 1
                
            if c[0].value == 400:
                break
            
            time.sleep(0.01)
    
def test_progress_bar_counter():
    c1 = progress.UnsignedIntValue(val=0)
    c2 = progress.UnsignedIntValue(val=0)
    
    maxc = 30
    m1 = progress.UnsignedIntValue(val=maxc)
    m2 = progress.UnsignedIntValue(val=maxc)
    
    c = [c1, c2]
    m = [m1, m2]
    
    t0 = time.time()
    
    with progress.ProgressBarCounter(count=c, max_count=m, verbose=1, interval=0.2) as sc:
        sc.start()
        while True:
            i = np.random.randint(0,2)
            with c[i].get_lock():
                c[i].value += 1
                if c[i].value > maxc:
                    sc.reset(i)
                           
            time.sleep(0.0432)
            if (time.time() - t0) > 15:
                break

def test_progress_bar_counter_non_max():
    c1 = progress.UnsignedIntValue(val=0)
    c2 = progress.UnsignedIntValue(val=0)
    
    c = [c1, c2]
    maxc = 30
    t0 = time.time()
    
    with progress.ProgressBarCounter(count=c, verbose=1, interval=0.2) as sc:
        sc.start()
        while True:
            i = np.random.randint(0,2)
            with c[i].get_lock():
                c[i].value += 1
                if c[i].value > maxc:
                    sc.reset(i)
                           
            time.sleep(0.0432)
            if (time.time() - t0) > 15:
                break
            
def test_progress_bar_counter_hide_bar():
    c1 = progress.UnsignedIntValue(val=0)
    c2 = progress.UnsignedIntValue(val=0)
    
    m1 = progress.UnsignedIntValue(val=0)
    
    c = [c1, c2]
    m = [m1, m1]
    maxc = 30
    t0 = time.time()
    
    with progress.ProgressBarCounter(count=c, max_count=m, verbose=1, interval=0.2) as sc:
        sc.start()
        while True:
            i = np.random.randint(0,2)
            with c[i].get_lock():
                c[i].value += 1
                if c[i].value > maxc:
                    sc.reset(i)
                           
            time.sleep(0.0432)
            if (time.time() - t0) > 15:
                break       
            
def test_progress_bar_slow_change():
    max_count_value = 100
    
    count = progress.UnsignedIntValue(0)
    max_count = progress.UnsignedIntValue(max_count_value)
    
    with progress.ProgressBar(count=count,
                              max_count=max_count,
                              interval=0.1,
                              speed_calc_cycles=5) as sbm:
    
        sbm.start()
        
        for i in range(max_count_value):               
            time.sleep(1)     
            count.value = i
            
def test_progress_bar_start_stop():
    max_count_value = 20
    
    count = progress.UnsignedIntValue(0)
    max_count = progress.UnsignedIntValue(max_count_value)
    
    with progress.ProgressBar(count=count,
                              max_count=max_count,
                              interval=0.5,
                              speed_calc_cycles=5,
                              verbose=1) as sbm:
    
        sbm.start()
        
        for i in range(max_count_value):
            time.sleep(0.1)
            count.value = i+1
            if i == 10:
                sbm.stop(make_sure_its_down = True)
                print("this will not overwrite the progressbar, because we stopped it explicitly")
                sbm.start()
        print("this WILL overwrite the progressbar, because we are still inside it's context (still running)")            

    print()
    print("create a progress bar, but do not start")
    with progress.ProgressBar(count=count,
                              max_count=max_count,
                              interval=0.5,
                              speed_calc_cycles=5,
                              verbose=1) as sbm:
        pass
    print("this is after progress.__exit__, there should be no prints from the progress")
            
if __name__ == "__main__":
    func = [    
#     test_loop_basic,
#     test_loop_signals,
#     test_loop_normal_stop,
#     test_loop_need_sigterm_to_stop,
#     test_loop_need_sigkill_to_stop,
#     test_why_with_statement,
#     test_progress_bar,
#     test_progress_bar_with_statement,
    test_progress_bar_multi,
#     test_status_counter,
#     test_status_counter_multi,
#     test_intermediate_prints_while_running_progess_bar,
#     test_intermediate_prints_while_running_progess_bar_multi,
#     test_progress_bar_counter,
#     test_progress_bar_counter_non_max,
#     test_progress_bar_counter_hide_bar,
#     test_progress_bar_slow_change,
#     test_progress_bar_start_stop,
    lambda: print("END")
    ]
    
    for f in func:
        print()
        print('#'*80)
        print('##  {}'.format(f.__name__))
        print()
        f()
        time.sleep(1)
    

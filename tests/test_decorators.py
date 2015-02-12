#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import os
import sys
import time

from os.path import abspath, dirname, split

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

from jobmanager import decorators, progress

@decorators.ProgressBar
def _my_func_1(arg, 
                kwarg     = "1", 
                count     = decorators.progress.UnsignedIntValue(val=0), 
                max_count = decorators.progress.UnsignedIntValue(val=1)):
    maxval = 100
    max_count.value = maxval
        
    for i in range(maxval):
        count.value += 1
        time.sleep(0.02)

    return arg+kwarg


def _my_func_2(arg, 
                c     = decorators.progress.UnsignedIntValue(val=0), 
                m     = decorators.progress.UnsignedIntValue(val=1), 
                kwarg = "2"):
    maxval = 100
    m.value += maxval
        
    for i in range(maxval):
        c.value += 1
        time.sleep(0.02)

    return arg+kwarg


def test_ProgressBar():
    result1 = _my_func_1("decorated function", kwarg=" 1")
    print(result1)
    
    wrapper = decorators.ProgressBar(_my_func_2, interval=.1)
    result2 = wrapper("wrapped function", kwarg=" 2")
    print(result2)

@decorators.ProgressBar
def my_func(c, m):
    for i in range(m.value):
        c.value = i
        time.sleep(0.02)
        
def test_decorator():
    c = progress.UnsignedIntValue(val=0)
    m = progress.UnsignedIntValue(val=100)
    my_func(c=c, m=m)
    my_func(c, m)
    

def my_func_ProgressBarOverrideCount(c = None, m = None):
    maxVal = 100
    if m is not None:
        m.value = maxVal
        
    for i in range(maxVal):
        time.sleep(0.03)
        if c is not None:
            c.value = i 
        
    
def test_ProgressBarOverrideCount():
    print("normal call -> no decoration")
    my_func_ProgressBarOverrideCount()
    print("done!")
    print()
    
    my_func_ProgressBarOverrideCount_dec = decorators.ProgressBarOverrideCount(my_func_ProgressBarOverrideCount)
    print("with decorator")
    my_func_ProgressBarOverrideCount_dec()
    print("done!")
    
def test_extended_PB_get_access_to_progress_bar():
    def my_func(c, m, **kwargs):
        for i in range(m.value):
            c.value = i+1
            time.sleep(0.1)
        
        try: 
            kwargs['progress_bar'].stop(make_sure_its_down=True) 
        except:
            pass
        
        print("let me show you something")
        
    c = progress.UnsignedIntValue(val=0)
    m = progress.UnsignedIntValue(val=20)
    
    print("call decorated func")
    my_func_dec = decorators.ProgressBarExtended(my_func)
    my_func_dec(c=c, m=m)
    
    print("call non decorated func")
    my_func(c, m)
    
def test_extended_PB_progress_bar_off():
    c = progress.UnsignedIntValue(val=0)
    m = progress.UnsignedIntValue(val=20)
    
    @decorators.ProgressBarExtended
    def my_func_kwargs(c, m, **kwargs):
        for i in range(m.value):
            c.value = i+1
            time.sleep(0.1)

    @decorators.ProgressBarExtended            
    def my_func_normal(c, m, progress_bar_off=False, **kwargs):
        for i in range(m.value):
            c.value = i+1
            time.sleep(0.1)            
        
    print("call with no kwarg -> normal progressBar")
    my_func_kwargs(c, m)
    
    print("call with kwarg 'progress_bar_off = True' -> NO progressBar")
    my_func_kwargs(c, m, progress_bar_off = True)
    
    print("call with kwarg 'progress_bar_off = False' -> normal progressBar")
    my_func_kwargs(c, m, progress_bar_off = False)
    
    print("call with argument 'progress_bar_off = False' -> normal progressBar")
    my_func_normal(c, m, progress_bar_off = False)
    
    print("call with default argument 'progress_bar_off = False' -> normal progressBar")
    my_func_normal(c, m)
    
    print("call with argument 'progress_bar_off = True' -> NO progressBar")
    my_func_normal(c, m, progress_bar_off = True)
    
    

        
if __name__ == "__main__":
#     test_ProgressBar()
#     test_decorator()
#     test_ProgressBarOverrideCount()
#     test_extended_PB_get_access_to_progress_bar()
    test_extended_PB_progress_bar_off()
    
    

        
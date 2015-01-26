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
        
if __name__ == "__main__":
    test_ProgressBar()
    test_decorator()

        

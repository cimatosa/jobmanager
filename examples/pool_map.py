#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

from os.path import split, dirname, abspath
import sys

import time
import numpy as np
import multiprocessing as mp

# Add parent directory to beginning of path variable
sys.path.insert(0, dirname(dirname(abspath(__file__))))

import jobmanager



def func(x):
    """Example function with only one argument"""
    return np.sum(x)

# Create list of parameters
a = list()
for i in range(10):
    a.append((i,2.34))

# equivalent to mp.Pool()
p = jobmanager.decorators.Pool()

b = p.map(func, a)

print(b)

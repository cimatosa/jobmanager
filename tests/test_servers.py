#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname, split

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

import jobmanager as jm

def bin_list():
    import pickle
    a =  ['cvbnm', 'qwert', 'asdfg']
    print(pickle.dumps(a))
    
def bin_dict():
    import pickle
    a =  {'cvbnm': 1, 'qwert': 2, 'asdfg': 3}
    print(pickle.dumps(a))
    
def see_bin_data():
    import multiprocessing as mp
    mp.set_start_method('spawn')
    
    for i in range(10):
        p = mp.Process(target = bin_list)
        p.start()
        p.join()
        
    for i in range(10):
        p = mp.Process(target = bin_dict)
        p.start()
        p.join()

def test_recursive_type_scan():
    d = {'a': 1, 'b':2}
    l = [2, 3]
    t = ('3','4')
    i = 1
    f = 3.4
    s = 's'
    
    a1 = [l, t, d]
    a2 = [l, t]
    
    a3 = (d, d, d)
    a4 = (i, l, a1)
    
    
    ###
    # GENERAL DICT SCAN
    ###
    assert jm.servers.recursive_scan_for_instance(obj=d, type=dict) == True
    
    assert jm.servers.recursive_scan_for_instance(obj=l, type=dict) == False
    assert jm.servers.recursive_scan_for_instance(obj=t, type=dict) == False
    assert jm.servers.recursive_scan_for_instance(obj=i, type=dict) == False
    assert jm.servers.recursive_scan_for_instance(obj=f, type=dict) == False
    assert jm.servers.recursive_scan_for_instance(obj=s, type=dict) == False
    
    assert jm.servers.recursive_scan_for_instance(obj=a1, type=dict) == True
    assert jm.servers.recursive_scan_for_instance(obj=a2, type=dict) == False
    assert jm.servers.recursive_scan_for_instance(obj=a3, type=dict) == True
    assert jm.servers.recursive_scan_for_instance(obj=a4, type=dict) == True
    
    ###
    # SPECIFIC DICT SCAN
    ###
    assert jm.servers.recursive_scan_for_dict_instance(obj=d) == True
    
    assert jm.servers.recursive_scan_for_dict_instance(obj=l) == False
    assert jm.servers.recursive_scan_for_dict_instance(obj=t) == False
    assert jm.servers.recursive_scan_for_dict_instance(obj=i) == False
    assert jm.servers.recursive_scan_for_dict_instance(obj=f) == False
    assert jm.servers.recursive_scan_for_dict_instance(obj=s) == False
    
    assert jm.servers.recursive_scan_for_dict_instance(obj=a1) == True
    assert jm.servers.recursive_scan_for_dict_instance(obj=a2) == False
    assert jm.servers.recursive_scan_for_dict_instance(obj=a3) == True
    assert jm.servers.recursive_scan_for_dict_instance(obj=a4) == True

    ###
    # INT SCAN
    ###
    
    assert jm.servers.recursive_scan_for_instance(obj = i, type=int) == True
    assert jm.servers.recursive_scan_for_instance(obj = l, type=int) == True
    assert jm.servers.recursive_scan_for_instance(obj = a1, type=int) == True
    assert jm.servers.recursive_scan_for_instance(obj = a2, type=int) == True
    assert jm.servers.recursive_scan_for_instance(obj = a4, type=int) == True
    
    print(jm.servers.recursive_scan_for_instance(obj = d, type=int))
    print(jm.servers.recursive_scan_for_instance(obj = a3, type=int))
    
    for i in d:
        print(i)
    
    
if __name__ == "__main__":
    test_recursive_type_scan()
#     see_bin_data()
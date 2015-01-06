#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import sys
from os.path import abspath, dirname, split

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

import jobmanager as jm

def test_pd():
    with jm.persistentData.PersistentDataStructure(name='test_data', verbose=2) as data:
        key = 'a'
        value = 1
        data.setData(key=key, value=value)
        assert data.getData(key) == value
        
        
        key_sub = 'zz'
        with data.getData(key_sub, create_sub_data=True) as sub_data:
            sub_data.setData(key=key, value=3)
            assert sub_data.getData(key) == 3
            assert data.getData(key) == 1
            

            with sub_data.getData(key_sub, create_sub_data=True) as sub_sub_data:
                sub_sub_data.setData(key=key, value=4)
                assert sub_sub_data.getData(key) == 4
                assert sub_data.getData(key) == 3
                assert data.getData(key) == 1
                
            with sub_data.getData(key_sub, create_sub_data=True) as sub_sub_data:
                assert sub_sub_data.getData(key) == 4
                assert sub_data.getData(key) == 3
                assert data.getData(key) == 1
                
def test_pd_bytes():
    import pickle
    
    t1 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9)
    t2 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9,1)
    
    b1 = pickle.dumps(t1)
    b2 = pickle.dumps(t2)
    
    with jm.persistentData.PersistentDataStructure(name='base') as base_data:
        with base_data.getData(key=b1, create_sub_data=True) as sub_data:
            for i in range(10):
                sub_data[i] = t2
                
        base_data[b2] = t1
        
    
    with jm.persistentData.PersistentDataStructure(name='base') as base_data:
        with base_data.getData(key=b1, create_sub_data=True) as sub_data:
            for i in range(10):
                assert sub_data[i] == t2
        
        assert base_data[b2] == t1

   
if __name__ == "__main__":
    test_pd()
    test_pd_bytes()
    

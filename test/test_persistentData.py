#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import sys
from os import rmdir, remove
from os.path import abspath, dirname, split, exists

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

from jobmanager.persistentData import PersistentDataStructure as PDS

def test_pd():
    with PDS(name='test_data', verbose=0) as data:
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

        data._consistency_check()
    
    data.erase()
    
def test_reserved_key_catch():
    with PDS(name='data', verbose=0) as data:
        # TRY TO READ RESERVED KEYS
        try:
            a = data[0]
        except RuntimeError:
            pass
        else:
            assert False
            
        try:
            b = data[1]
        except RuntimeError:
            pass
        else:
            assert False
        
        # TRY TO SET RESERVED KEYS
        try:
            data[0] = 4
        except RuntimeError:
            pass
        else:
            assert False
            
        try:
            data[1] = 4
        except RuntimeError:
            pass
        else:
            assert False

        # TRY TO REMOVE RESERVED KEYS
        try:
            del data[0]
        except RuntimeError:
            pass
        else:
            assert False
        
        try:
            del data[1]
        except RuntimeError:
            pass
        else:
            assert False            
    
        data.erase()
                
def test_pd_bytes():
    import pickle
    
    t1 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9)
    t2 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9,1)
    
    b1 = pickle.dumps(t1)
    b2 = pickle.dumps(t2)
    
    verbose = 0
    
    with PDS(name='base', verbose=verbose) as base_data:
        with base_data.getData(key=b1, create_sub_data=True) as sub_data:
            for i in range(2, 10):
                sub_data[i] = t2
                
        base_data[b2] = t1
        
    if verbose > 1:
        print("\nCHECK\n")
        
    with PDS(name='base', verbose=verbose) as base_data:
        with base_data.getData(key=b1) as sub_data:
            for i in range(2, 10):
                assert sub_data[i] == t2
        
        assert base_data[b2] == t1
        
        base_data._consistency_check()
        
    base_data.erase()

def test_directory_removal():
    with PDS(name='data', verbose=0) as data:
        with data.newSubData('s1') as s1:
            s1['bla'] = 9
            
        f = open(file=data._PersistentDataStructure__dir_name + '/other_file', mode='w')
        f.close()
        
        data.erase()
        
    assert exists(data._PersistentDataStructure__dir_name)
    remove(data._PersistentDataStructure__dir_name + '/other_file')
    rmdir(data._PersistentDataStructure__dir_name)
      
if __name__ == "__main__":
    test_reserved_key_catch()
    test_pd()
    test_pd_bytes()
    test_directory_removal()
    

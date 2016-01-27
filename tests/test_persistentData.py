#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import sys
import pickle
import os
from os.path import abspath, dirname, split, exists
from shutil import rmtree

import warnings
warnings.filterwarnings('error')
    
import numpy as np

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

import jobmanager.persistentData as pd
from jobmanager.persistentData import PersistentDataStructure as PDS

VERBOSE = 1

if sys.version_info[0] == 2:
    # fixes keyword problems with python 2.x
    old_open = open
    def new_open(file, mode):
        return old_open(name = file, mode = mode)
    open = new_open
    
rmtree('__test_data', ignore_errors=True)
rmtree('__data', ignore_errors=True)
rmtree('__base', ignore_errors=True)



def test_pd():
    try:
        with PDS(name='test_data', verbose=VERBOSE) as data:
            key = 'a'
            value = 1
            data.setData(key=key, value=value)
            assert data.getData(key) == value
            assert len(data) == 1
            
            
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
    
    finally:
        data.erase()
                   
def test_pd_bytes():
    t1 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9)
    t2 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9,1)
    
    b1 = pickle.dumps(t1)
    b2 = pickle.dumps(t2)
    
    try:
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key=b1, create_sub_data=True) as sub_data:
                for i in range(2, 10):
                    sub_data[i] = t2
                    
            base_data[b2] = t1
            
        if VERBOSE > 1:
            print("\nCHECK\n")
            
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key=b1) as sub_data:
                for i in range(2, 10):
                    assert sub_data[i] == t2
            
            assert base_data[b2] == t1
            
    finally:
        base_data.erase()

def test_directory_removal():    
    try:
        with PDS(name='data', verbose=VERBOSE) as data:
            with data.newSubData('s1') as s1:
                s1['bla'] = 9

            f = open(file=data._dirname + '/other_file', mode='w')
            f.close()
            
            print("now there should be a warning, because there is an unknown file in the directory!")
    finally:
        try:
            data.erase()
        except UserWarning:
            pass

    assert exists(data._dirname)
    os.remove(data._dirname + '/other_file')
    os.rmdir(data._dirname)
    
def test_mp_read_from_sqlite():
    import sqlitedict as sqd
    import multiprocessing as mp
    import time
    
    d = sqd.SqliteDict('test.db', autocommit = True)
    
    def write(arg):
        with sqd.SqliteDict('test.db', autocommit = True) as d:
            for i in range(100):
                d[i] = (i, arg)
    
    def read():
        with sqd.SqliteDict('test.db', autocommit = True) as d:
            for i in range(len(d)):
                print(i, d[i])
            
    p1 = mp.Process(target = write, args=('p1', ))
    time.sleep(0.1)
    p2 = mp.Process(target = read)
    
    p1.start()
    p2.start()
    
    p1.join(10)
    p2.join(10)
    
    try:
        if p1.is_alive():
            raise RuntimeError("write process did not finish on time")
        if p2.is_alive():
            raise RuntimeError("read process did not finish on time")
    finally:    
        p1.terminate()
        p2.terminate()
        d.terminate()
    
    
def test_from_existing_sub_data():
    t1 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9)
    t2 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9,1)
    
    try:
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key='sub1', create_sub_data = True) as sub_data:
                sub_data[100] = t1
                sub_data[200] = t2
                with sub_data.getData(key = 'subsub1', create_sub_data = True) as sub_sub_data:
                    sub_sub_data['t'] = 'hallo Welt'
                    
            base_data.setDataFromSubData(key='sub2', subData = sub_data)
    
            
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key='sub2', create_sub_data = False) as sub_data:
                assert sub_data[100] == t1
                assert sub_data[200] == t2
                with sub_data.getData(key = 'subsub1', create_sub_data = False) as sub_sub_data:
                    assert sub_sub_data['t'] == 'hallo Welt'
                    
    
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key='sub1', create_sub_data = True) as sub_data:
                base_data['sub2'] = sub_data
                
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key='sub2', create_sub_data = False) as sub_data:
                assert sub_data[100] == t1
                assert sub_data[200] == t2
                with sub_data.getData(key = 'subsub1', create_sub_data = False) as sub_sub_data:
                    assert sub_sub_data['t'] == 'hallo Welt'
                    sub_sub_data['t'] = 'sub2:hallo Welt'
                
                sub_data[100] = "sub2:t1"  
                sub_data[200] = "sub2:t2"
                
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key='sub1', create_sub_data = True) as sub_data:
                assert sub_data[100] == t1
                assert sub_data[200] == t2
                with sub_data.getData(key = 'subsub1', create_sub_data = True) as sub_sub_data:
                    assert sub_sub_data['t'] == 'hallo Welt'
    
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key='sub2', create_sub_data = False) as sub_data:
                with sub_data.getData(key = 'subsub1', create_sub_data = False) as sub_sub_data:
                    assert sub_sub_data['t'] == 'sub2:hallo Welt'
                
                assert sub_data[100] == "sub2:t1"  
                assert sub_data[200] == "sub2:t2"                
    
    finally:
        base_data.erase()
    
def test_remove_sub_data_and_check_len():
    try:
        with PDS(name='base', verbose=VERBOSE) as base_data:
            with base_data.getData(key='sub1', create_sub_data = True) as sub_data:
                sub_data[100] = 't1'
                sub_data[200] = 't2'
                with sub_data.getData(key = 'subsub1', create_sub_data = True) as sub_sub_data:
                    sub_sub_data['t'] = 'hallo Welt'
                    
                    
                assert len(sub_data) == 3, "len = {}".format(len(sub_data))
            
    
    
            assert len(base_data) == 1
            base_data['copy_of_sub1'] = sub_data
            assert len(base_data) == 2
            del base_data['sub1']
            assert len(base_data) == 1
            
            with base_data.getData(key='copy_of_sub1', create_sub_data = True) as sub_data:
                assert len(sub_data) == 3
                assert sub_data[100] == 't1'
                assert sub_data[200] == 't2'
                with sub_data.getData(key = 'subsub1', create_sub_data = True) as sub_sub_data:
                    assert sub_sub_data['t'] == 'hallo Welt'
                
            assert ('sub1' not in base_data)
    finally:
        base_data.erase()
    
def test_show_stat():
    try:
        with PDS(name='test_data', verbose=VERBOSE) as data:
            key = 'a'
            value = 1
            data.setData(key=key, value=value)
            assert data.getData(key) == value
            
            
            key_sub = 'zz'
            with data.getData(key_sub, create_sub_data=True) as sub_data:
                sub_data.setData(key=key, value=3)
                assert sub_data.getData(key) == 3
                assert data.getData(key) == 1
                
                
                key_sub_bin = pickle.dumps(key_sub, protocol=2)
                with sub_data.getData(key_sub_bin, create_sub_data=True) as sub_sub_data:
                    sub_sub_data.setData(key=key, value=4)
                    assert sub_sub_data.getData(key) == 4
                    assert sub_data.getData(key) == 3
                    assert data.getData(key) == 1
                    
                with sub_data.getData(key_sub_bin, create_sub_data=True) as sub_sub_data:
                    assert sub_sub_data.getData(key) == 4
                    assert sub_data.getData(key) == 3
                    assert data.getData(key) == 1
            
            data.show_stat(recursive=True)
    finally:
        data.erase()

def slow_len(pd):
    n = 0
    for k in pd:
        n += 1
    return n
    
def test_len():
    try:
        with PDS(name='data', verbose=VERBOSE) as data:
            assert len(data) == 0
            assert slow_len(data) == 0
            
            data['a'] = 1
            assert len(data) == 1
            assert slow_len(data) == 1
            
            for i in range(1, 8):
                data[i*10] = i
            assert len(data) == 8
            assert slow_len(data) == 8
            
        with PDS(name='data', verbose=VERBOSE) as data:
            assert len(data) == 8
            assert slow_len(data) == 8
            
            data.clear()
            assert len(data) == 0
            assert slow_len(data) == 0
            
        with PDS(name='data', verbose=VERBOSE) as data:
            assert len(data) == 0
            assert slow_len(data) == 0
    finally:
        data.erase()
        
    
    
def test_clear():
    try:
        with PDS(name='data', verbose=VERBOSE) as data:
            data['a'] = 1
            data['b'] = 2
            with data.newSubData('s1') as s1:
                s1['bla'] = 9
            with data.newSubData('s2') as s2:
                s2['bla2'] = 18
            
            with data['s1'] as s1:
                s1['t'] = 'tmp'
                s1.clear()
            
            with data['s1'] as s1:
                assert len(s1) == 0
                assert slow_len(s1) == 0
            
            data.clear()
            
            dir_content = os.listdir(data._dirname)
            assert len(dir_content) == 1
            assert dir_content[0] == 'data.db'
    finally:
        data.erase()

def test_not_in():
    try:
        with PDS(name='data', verbose=VERBOSE) as data:
            data['a'] = 1
            data['b'] = 2
            with data.newSubData('s1') as s1:
                s1['bla'] = 9
                
            assert ('a' in data)
            assert ('b' in data)
            assert ('s1' in data)
            
            assert ('c' not in data)
                
    finally:
        data.erase()

def test_npa():
    a = np.linspace(0, 1, 100).reshape(10,10)
    try:
        with PDS(name='data_npa', verbose=VERBOSE) as data:
            data['a'] = a
            
        with PDS(name='data_npa', verbose=VERBOSE) as data:
            b = data['a']
            assert np.all(b == a)      
            
        with PDS(name='data_npa', verbose=VERBOSE) as data:
            del data['a']
            data['a'] = a
    finally:
        data.erase()
    
def test_merge():
    
    a = np.random.rand(5)
    
    with PDS(name='d1', verbose=VERBOSE) as d1:
        d1['k1'] = 1
        d1['k2'] = 2
        d1['k3'] = 3
        d1['aa'] = a
        with d1.newSubData('sub1') as sub1:
            sub1['s1'] = 11
            sub1['s2'] = 12
            sub1['s3'] = 13
            sub1['a'] = a
            
    with PDS(name='d2', verbose=VERBOSE) as d2:
        d2['2k1'] = 1
        
        d2.mergeOtherPDS(other_db_name = "d1")

    try:
        with PDS(name='d2', verbose=VERBOSE) as d2:
            assert 'k1' in d2
            assert d2['k1'] == 1
            assert 'k2' in d2
            assert d2['k2'] == 2
            assert 'k3' in d2
            assert d2['k3'] == 3
            assert 'aa' in d2
            assert np.all(d2['aa'] == a)            
                
            assert "sub1" in d2
            assert isinstance(d2["sub1"], PDS)
            with d2["sub1"] as sub:
                assert 's1' in sub
                assert sub['s1'] == 11
                assert 's2' in sub
                assert sub['s2'] == 12
                assert 's3' in sub
                assert sub['s3'] == 13
                assert 'a' in sub
                assert np.all(sub['a'] == a)
                
        try:
            with PDS(name='d2', verbose=VERBOSE) as d2:
                d2.mergeOtherPDS(other_db_name = "d1", update='error')
        except KeyError as e:
            print(e)
            print("this is ok!")
            pass
        
        with PDS(name='d2', verbose=VERBOSE) as d2:
            d2['k1'] = 'k1'
            d2.mergeOtherPDS(other_db_name = "d1", update='ignore')
            assert d2['k1'] == 'k1'
            
        with PDS(name='d2', verbose=VERBOSE) as d2:
            d2['k1'] = 'k1'
            d2.mergeOtherPDS(other_db_name = "d1", update='update')
            assert d2['k1'] == 1        
            
    finally:
        with PDS(name='d1', verbose=VERBOSE) as d1:
            d1.erase()
            
        with PDS(name='d2', verbose=VERBOSE) as d2:
            d2.erase()        
    
      
if __name__ == "__main__":
#     test_pd()
#     test_pd_bytes()
#     test_directory_removal()
#     test_mp_read_from_sqlite()
#     test_from_existing_sub_data()
#     test_remove_sub_data_and_check_len()
#     test_show_stat()
#     test_len()
#     test_clear()
#     test_not_in()
#     test_npa()
    test_merge()
    pass

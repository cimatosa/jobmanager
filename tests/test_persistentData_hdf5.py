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

from jobmanager.persistentData import PersistentDataStructure_HDF5 as PDS
from jobmanager.persistentData import PersistentDataStructure as PDS_SQL

VERBOSE = 1

def test_md5_clash():
    with PDS(name='test_data', verbose=VERBOSE) as data:
        data.clear()
        data._md5 = lambda key: str(np.random.randint(0,2))
        for i in range(100):
            data['a{}'.format(i)] = i
        assert len(data) == 100
        
        for i in range(100):
            data.newSubData('s{}'.format(i))
        assert len(data) == 200
        
        n = 0
        for k in data:
            n += 1
        assert n == 200

def test_pd():   
    with PDS(name='test_data', verbose=VERBOSE) as data:
        data.clear()
        key = 'a'
        value = 1
        try:
            data.getData(key)
        except KeyError as e:
            pass
        
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

    with PDS(name='test_data', verbose=VERBOSE) as data:
        data['d1'] = ('ö', 4, [0])
    with PDS(name='test_data', verbose=VERBOSE) as data:
        d1 = data['d1']
        assert d1[0] == 'ö'
        assert d1[1] == 4
        assert d1[2] == [0]
        
    with PDS(name='test_data', verbose=VERBOSE) as data:
        data.clear()
        data.newSubData(key='sub_1', overwrite = False)
        
    with PDS(name='test_data', verbose=VERBOSE) as data:
        try:
            data.newSubData(key='sub_1', overwrite = False)
        except KeyError:
            pass        
    
        data.newSubData(key='sub_1', overwrite = True)
                   
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
                    assert np.all(sub_data[i] == t2)
            
            assert np.all(base_data[b2] == t1)
            
    finally:
        print()
        base_data.erase()

   
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
    print()
    print('test_from_existing_sub_data')
    t1 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9)
    t2 = (3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9,1)
    

    with PDS(name='base', verbose=VERBOSE) as base_data:
        base_data.clear()
        with base_data.getData(key='s1', create_sub_data = True) as s1:
            s1['d1'] = 1
            s1['d2'] = 'b'
             
    with PDS(name='base', verbose=VERBOSE) as base_data:
        assert base_data['s1']['d1'] == 1
        assert base_data['s1']['d2'] == 'b'
        base_data.setDataFromSubData('s2', base_data['s1'])
        assert base_data['s2']['d1'] == 1
        assert base_data['s2']['d2'] == 'b'
        
        del base_data['s1']
        assert base_data['s2']['d1'] == 1
        assert base_data['s2']['d2'] == 'b'            
    
    with PDS(name='base', verbose=VERBOSE) as base_data:
        with base_data.getData(key='sub1', create_sub_data = True) as sub_data:
            sub_data[100] = t1
            sub_data[200] = t2
            with sub_data.getData(key = 'subsub1', create_sub_data = True) as sub_sub_data:
                sub_sub_data['t'] = 'hallo Welt'
                
        base_data.setDataFromSubData(key='sub2', subData = sub_data)

        
    with PDS(name='base', verbose=VERBOSE) as base_data:
        with base_data.getData(key='sub2', create_sub_data = False) as sub_data:
            assert np.all(sub_data[100] == t1)
            assert np.all(sub_data[200] == t2)
            with sub_data.getData(key = 'subsub1', create_sub_data = False) as sub_sub_data:
                assert sub_sub_data['t'] == 'hallo Welt'
                

    with PDS(name='base', verbose=VERBOSE) as base_data:
        with base_data.getData(key='sub1', create_sub_data = True) as sub_data:
            base_data['sub2'] = sub_data
            
    with PDS(name='base', verbose=VERBOSE) as base_data:
        with base_data.getData(key='sub2', create_sub_data = False) as sub_data:
            assert np.all(sub_data[100] == t1)
            assert np.all(sub_data[200] == t2)
            with sub_data.getData(key = 'subsub1', create_sub_data = False) as sub_sub_data:
                assert sub_sub_data['t'] == 'hallo Welt'
                sub_sub_data['t'] = 'sub2:hallo Welt'
            
            sub_data[100] = "sub2:t1"  
            sub_data[200] = "sub2:t2"
            

    with PDS(name='base', verbose=VERBOSE) as base_data:
        base_data.clear()
        with base_data.getData(key = 'sub1', create_sub_data = True) as sub1:
            sub1['npa'] = np.linspace(0,1,10)
            sub1['val'] = 'hallo ich bin sub1'
            
        base_data['sub2'] = sub1
        
    with PDS(name='base', verbose=VERBOSE) as base_data:
        npa1 = base_data['sub1']['npa']
        npa2 = base_data['sub1']['npa']
        
        assert type(npa1) == np.ndarray
        assert type(npa2) == np.ndarray
             
    
def test_remove_sub_data_and_check_len():

    with PDS(name='base', verbose=VERBOSE) as base_data:
        base_data.clear()
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
    

def slow_len(pd):
    n = 0
    for k in pd:
        n += 1
    return n
    
def test_len():

    with PDS(name='data', verbose=VERBOSE) as data:
        data.clear()
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
       
    
    
def test_clear():
    with PDS(name='data', verbose=VERBOSE) as data:
        data.clear()
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
        d1.clear()
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
        d2.clear()
        d2['2k1'] = 1
        with PDS(name='d1', verbose=VERBOSE) as d1:
            d2.mergeOtherPDS(other_db = d1, status_interval=0)

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
            with PDS(name='d1', verbose=VERBOSE) as d1:
                d2.mergeOtherPDS(other_db = d1, update='error', status_interval=0)
    except KeyError as e:
        print(e)
        print("this is ok!")
        pass
    
    with PDS(name='d2', verbose=VERBOSE) as d2:
        d2['k1'] = 'k1'
        with PDS(name='d1', verbose=VERBOSE) as d1:
            d2.mergeOtherPDS(other_db = d1, update='ignore', status_interval=0)
        assert d2['k1'] == 'k1'
        
    with PDS(name='d2', verbose=VERBOSE) as d2:
        d2['k1'] = 'k1'
        with PDS(name='d1', verbose=VERBOSE) as d1:
            d2.mergeOtherPDS(other_db = d1, update='update', status_interval=0)
        assert d2['k1'] == 1        

def test_link_vs_copy():
    data = np.arange(0,5)
    with PDS(name='d', verbose=VERBOSE) as d:
        d.clear()
        gr1 = d.getData('gr1', create_sub_data=True)
        gr1['data'] = data
        gr1['str'] = 'gr1'
        keys = [k for k in d['gr1']]
        assert keys[0] == 'str'
        assert keys[1] == 'data'

                
        d.setDataFromSubData('gr1_link', gr1, copy=False)
        
        keys = [k for k in d['gr1']]
        assert keys[0] == 'str'
        assert keys[1] == 'data'
        keys = [k for k in d['gr1_link']]
        assert keys[0] == 'str'
        assert keys[1] == 'data'
        
        
        d.setDataFromSubData('gr1_copy', gr1, copy=True)
        keys = [k for k in d['gr1']]
        assert keys[0] == 'str'
        assert keys[1] == 'data'
        keys = [k for k in d['gr1_copy']]
        assert keys[0] == 'str'
        assert keys[1] == 'data'
    
    with PDS(name='d', verbose=VERBOSE) as d:
        assert np.all(d['gr1']['data'] == d['gr1_link']['data'])
        assert np.all(d['gr1']['data'] == d['gr1_copy']['data'])
        
        d['gr1'].getH5Object('data')[0] = -3
        assert d['gr1_link']['data'][0] == -3
        assert d['gr1_copy']['data'][0] == 0
        
        keys = [k for k in d['gr1']]
        assert keys[0] == 'str'
        assert keys[1] == 'data'
     
    
        

def test_merge_fname_conflict():
    a = np.random.rand(5)
    b = np.random.rand(5)
    
    with PDS(name='d1', verbose=VERBOSE) as d1:
        d1.clear()
        d1['aa'] = a
        with d1.newSubData('sub1') as sub1:
            sub1['s1'] = 11
            sub1['a'] = a
            
    with PDS(name='d2', verbose=VERBOSE) as d2:
        d2.clear()
        d2['2k1'] = 1
        d2['2aa'] = b
        with d2.newSubData('sub2') as sub2:
            sub2['s2'] = 22
            sub2['a2'] = b
            
        assert np.all(d2['2aa'] == b)
        with PDS(name='d1', verbose=VERBOSE) as d1:
            d2.mergeOtherPDS(other_db = d1, update='error', status_interval=0)
        assert np.all(d2['2aa'] == b)
        
    with PDS(name='d2', verbose=VERBOSE) as d2:
        assert d2['2k1'] == 1
        assert np.all(d2['2aa'] == b)
        
        assert np.all(d2['aa'] == a)
        
        assert d2.has_key('sub1')
        with d2['sub1'] as sub1:
            assert sub1['s1'] == 11
            assert np.all(sub1['a'] == a)
        
        assert d2.has_key('sub2')
        with d2['sub2'] as sub2:
            assert sub2['s2'] == 22
            assert np.all(sub2['a2'] == b)
                
     
def test_convert_SQL_TO_H5():
    data = np.empty((10,), dtype='<U2')
    data[0] = 'd\U00008000' 
    data[1] = 'ha'
            
    with PDS_SQL(name='pds_sql') as db:
        db.clear()
        db['a'] = 5
        db[4] = (3, 's', [0])
        db['uni'] = data 
        db[b'\xff\xee'] = np.arange(4)
        
    
    with PDS(name='pds_h5') as db_h5:
        db_h5.clear()
        db_h5['datautest'] = data
        with PDS_SQL(name='pds_sql') as db_sql:
            db_h5.mergeOtherPDS(db_sql, status_interval=0)
            
    with PDS(name='pds_h5') as db_h5:
        assert db_h5['a'] == 5
        assert db_h5['4'] == (3, 's', [0])
        assert np.all(db_h5[b'\xff\xee'] == np.arange(4))
        assert np.all(db_h5['uni'] == data)
      
if __name__ == "__main__":
    test_clear()
    test_pd()
    test_md5_clash()
    test_pd_bytes()
 
    test_mp_read_from_sqlite()
    test_from_existing_sub_data()
    test_remove_sub_data_and_check_len()
    test_len()
 
    test_not_in()
    test_npa()
    test_merge()
    test_merge_fname_conflict()
    test_link_vs_copy()
    test_convert_SQL_TO_H5()
    pass

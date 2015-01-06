#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlitedict
from .jobmanager import JobManager_Server
from collections import namedtuple

Data_Type = namedtuple('Data_Type', ['key', 'data'])

class PersistentDataBase(object):
    def __init__(self, fname, table_name):
        self.fname = fname
        self.table_name = table_name
    
    def has_key(self, key):
        hash_key = hash(key)
        with sqlitedict.SqliteDict(filename = self.fname, tablename=self.table_name) as data: 
            if hash_key in data:
                # hash already exists -> key might exists
                for i, d in enumerate(data[hash_key]):
                    if d.key == key:
                        return i, hash_key
        return -1, hash_key
        
    def dump(self, key, data, overwrite=False):
        key_idx, hash_key = self.has_key(key)
        
        if key_idx >= 0:
            print("key {} already exists!")
            if overwrite:
                print("overwrite data")
                with sqlitedict.SqliteDict(filename = self.fname, tablename=self.table_name) as data_dict: 
                    data_dict[hash_key][key_idx].data = data
                    data_dict.commit()
                    return True
            else:
                print("not NOT overwrite data")
                return False
        else:
            # hash does not exists -> create new item (list of Data_Type elements)
            with sqlitedict.SqliteDict(filename = self.fname, tablename=self.table_name) as data_dict:
                data_dict[hash_key] = []
                data_dict[hash_key].append(Data_Type(key = key, data = data))
                data_dict.commit()
                return True

class PersistentDataBase_byte_keys(object):
    def __init__(self, fname, table_name):
        self.fname = fname
        self.table_name = table_name
    
    def dump(self, key_as_bytes, data, overwrite=False):
        with sqlitedict.SqliteDict(filename = self.fname, tablename=self.table_name) as data_dict:
            if overwrite or (not key_as_bytes in data_dict):
                data_dict[key_as_bytes] = data
                data_dict.commit()
                return True
        
        return False

            
class PersistentData_Server(JobManager_Server, PersistentDataBase):
    def __init__(self, 
                 fname_persistent_data,
                 table_key,
                 authkey, 
                 const_arg=None, 
                 port=42524, 
                 verbose=1, 
                 msg_interval=1, 
                 fname_dump=None, 
                 speed_calc_cycles=50):
        
        JobManager_Server.__init__(self, authkey, const_arg=const_arg, port=port, verbose=verbose, msg_interval=msg_interval, fname_dump=fname_dump, speed_calc_cycles=speed_calc_cycles)
        PersistentDataBase.__init__(self, fname_persistent_data)
        
    def process_new_result(self, arg, result):
        self.dump(key = arg['key'], data = result, overwrite = True)
    
    def put_arg(self, a):
        try:
            a['key']
        except:
            raise RuntimeError("PersistentData_Server excepts only argument which provide a key such that arg['key'] exists")

        if self.has_key(a['key']) >= 0:
            print("key {} found -> put_arg will be SKIPPED".format(a['key']))
        else:
            JobManager_Server.put_arg(self, a)
        
        

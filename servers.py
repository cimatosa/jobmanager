import sqlitedict
from .jobmanager import JobManager_Server
from collections import namedtuple

Data_Type = namedtuple('Data_Type', ['key', 'data'])

class PersistentDataBase(object):
    def __init__(self, fname):
        self.fname = fname
    
    def has_key(self, key):
        hash_key = hash(key)
        with sqlitedict.SqliteDict(self.fname) as data: 
            if hash_key in data:
                # hash already exists -> key might exists
                for i, d in enumerate(data[hash_key]):
                    if d.key == key:
                        return i
        return -1
        
    def dump(self, key, data, overwrite=False):
        hash_key = hash(key)
        
        key_idx = self.has_key(key)
        
        if self.has_key(key) >= 0:
            print("key {} already exists!")
            if overwrite:
                print("overwrite data")
                with sqlitedict.SqliteDict(self.fname) as data_dict: 
                    data_dict[hash_key][key_idx].data = data
                    data_dict.commit()
                    return True
            else:
                print("not NOT overwrite data")
                return False
        else:
            # hash does not exists -> create new item (list of Data_Type elements)
            with sqlitedict.SqliteDict(self.fname) as data_dict:
                data_dict[hash_key] = []
                data_dict[hash_key].append(Data_Type(key = key, data = data))
                data_dict.commit()
                return True
            
class PersistentData_Server(JobManager_Server, PersistentDataBase):
    def __init__(self, 
                 fname_persistent_data,
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
        
        
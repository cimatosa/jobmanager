import sqlitedict as sqd
from os.path import abspath, join, exists
import os
import sys
import shutil
import traceback
import pickle
import warnings

if sys.version_info[0] == 2:
    # fixes keyword problems with python 2.x
    os_remove = os.remove
    def new_remove(path):
        os_remove(path)
    os.remove = new_remove
    
    os_rmdir = os.rmdir
    def new_rmdir(path):
        os_rmdir(path)
    os.rmdir = new_rmdir

MAGIC_SIGN = 0xff4a87

KEY_COUNTER = '0'
KEY_SUB_DATA_KEYS = '1'

RESERVED_KEYS = (KEY_COUNTER, KEY_SUB_DATA_KEYS)

class PersistentDataStructure(object):
    """
        Note: avoid using pickled dictionaries as binary keys! The problem with dicts is
        that the order of the keys, when returned as list, depends on the hash value of
        the keys. If the keys are strings, the hash value will be randomly seeded for
        each python session, which may lead to different binary representations of the
        same dict. Therefore the same dict may actually be considered as distinct keys.
        
        The same hold true when using classes with default pickler routine as binary keys
        (because the pickler will essentially pickle the dictionary self.__dict__).
        If you want to use "complicated" python objects as binary keys make sure you
        implement your own pickle behavior without the need of dictionaries.   
    """
    def __init__(self, name, path="./", verbose=1):
        self._open = False
        self._name = name
        self._path = abspath(path)
        if not exists(self._path):
            raise RuntimeError("given path does not exists ({} -> {})".format(path, self._path))
        
        self.verbose = verbose
        
        # create directory to hold sub structures
        self._dirname = join(self._path, "__" + self._name)
        if not exists(self._dirname):
            os.mkdir(self._dirname)
        
        # open actual sqltedict
        self._filename = join(self._dirname, self._name + '.db')
        self.open()
        
        
        
        if KEY_COUNTER not in self.db:
            self.db[KEY_COUNTER] = 0 
           
        if KEY_SUB_DATA_KEYS not in self.db:
            self.db[KEY_SUB_DATA_KEYS] = set()
            
        self.db.commit()
            
        self.counter = self.db[KEY_COUNTER]
        self.sub_data_keys = self.db[KEY_SUB_DATA_KEYS]

            
    def _consistency_check(self):
        self.need_open()
        
        c = 0
        
        for key in self.db:
            value = self.db[key]
            if self.__is_sub_data(value):
                c += 1
                assert key in self.sub_data_keys
        
        assert len(self.sub_data_keys) == c
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.verbose > 1:
            print("exit called for        {} in {}".format(self._name, self._dirname))
        self.close()
        
    def open(self):
        """
            open the SQL database at self._filename = <path>/__<name>/<name>.db
            as sqlitedict
        """
        if self.verbose > 1:
            print("open db                {} in {}".format(self._name, self._dirname))             
        self.db = sqd.SqliteDict(filename = self._filename, autocommit=False)
        self._open = True
        
    def is_open(self):
        return self._open
        
    def is_closed(self):
        return not self._open
    
    def need_open(self):
        if self.is_closed():
            raise RuntimeError("PersistentDataStructure needs to be open")
        
    def close(self):
        """
            close the sqligtedict ans therefore the SQL database
        """
        try:
            self.db.close()
            self._open = False
            if self.verbose > 1:
                print("closed db              {} in {}".format(self._name, self._dirname))
        except:
            if self.verbose > 1:
                print("db seem already closed {} in {}".format(self._name, self._dirname))
            
    def erase(self):
        """
            removed the database file from the disk
            
            this is called recursively for all sub PersistentDataStructure
        """
        if self.verbose > 1:
            print("erase db               {} in {}".format(self._name, self._dirname))        

        if self.is_closed():
            self.open()
            
        try:

            if self.verbose > 1:
                print("sub_data_keys:", self.sub_data_keys)
            for key in self.sub_data_keys:
                if self.verbose > 1:
                    print("call erase for key:", key, "on file", self._filename)
                sub_data = self.getData(key)
                sub_data.erase()
        except:
            traceback.print_exc()
        finally:
            self.close()

        os.remove(path = self._filename)
        try:
            os.rmdir(path = self._dirname)
        except OSError as e:
            if self.verbose > 0:
                warnings.warn("directory structure can not be deleted\n{}".format(e))
                
    def clear(self):
        """
            delete all entries from the db
        """
        self.need_open()
        
        # delete all sub data
        for k in self.sub_data_keys:
            with self[k] as sub_data:
                sub_data.erase()
                        
        self.db.clear()
        
        self.db[KEY_COUNTER] = 0 
        self.db[KEY_SUB_DATA_KEYS] = set()
        self.db.commit()
        
        self.sub_data_keys = set()
        self.counter = 0        

    def show_stat(self, recursive = False, prepend = ""):
        prepend += self._name
        print("{}: I'm a pds called {}".format(prepend, self._name))
        print("{}:     dirname  :{}".format(prepend, self._dirname))
        print("{}:     filename :{}".format(prepend, self._filename))
        
        self.need_open()
        
        str_key = 0
        bin_key = 0
        oth_key = 0
        
        for k in self:
            if isinstance(k, str):
                str_key += 1
            elif isinstance(k, bytes):
                bin_key += 1
            else:
                oth_key += 1
                
        print("{}:     number of string keys: {}".format(prepend, str_key))
        print("{}:     number of byte   keys: {}".format(prepend, bin_key))
        if oth_key > 0:
            print("{}:     number of other  keys: {}".format(prepend, oth_key))
        print("{}:     number of subdata: {}".format(prepend, len(self.sub_data_keys)))
        print()
        sys.stdout.flush()
        if recursive:
            for k in self.sub_data_keys:
                if isinstance(k, bytes):
                    k_from_bytes = pickle.loads(k)
                    print("show stat for subdata with key (from bytes) {}".format(k_from_bytes))
                else:
                    print("show stat for subdata with key {}".format(k))
                sys.stdout.flush()
                    
                            
                with self.getData(k) as subdata:
                    subdata.show_stat(recursive = recursive,
                                      prepend = prepend + "->")
            
       
    def __check_key(self, key):
        """
            returns True if the key does NOT collide with some reserved keys
            
            otherwise a RuntimeError will be raised
        """
        if str(key) in RESERVED_KEYS:
            raise RuntimeError("key must not be in {} (reserved key)".format(RESERVED_KEYS))
        
        return True
    
    def __is_sub_data(self, value):
        """
            determine if the value gotten from the sqlitedict refers
            to a sub PersistentDataStructure
            
            this is considered the case if the value itself has an index 'magic'
            whose value matches a magic sign defined by MAGIC_SIGN 
        """
        try:
            assert value['magic'] == MAGIC_SIGN
            value.pop('magic')
            return True
        except:
            return False
    
    def has_key(self, key):
        self.need_open()
        return (key in self.db)
    
    def is_subdata(self, key):
        return key in self.sub_data_keys
        # return self.__is_sub_data(self.db[key])
        
    def setData(self, key, value, overwrite=False):
        """
            write the key value pair to the data base
            
            if the key already exists, overwrite must be
            set True in oder to update the data for
            that key in the database 
        """
        self.need_open()
        if not self.__check_key(key):
            return False
        
        if overwrite or (not key in self.db):
            self.db[key] = value
            self.db.commit()
            return True
        
        return False
            
    def newSubData(self, key):
        """
            if key is not in database
            create a new database (sqlitedict)
            which can be queried from this one
            via the key specified 
            
            this will automatically create a new
            file where the filename is internally
            managed (simple increasing number)   
        """
        self.need_open()
        if not key in self.db:
            self.counter += 1
            self.sub_data_keys.add(key)
            if self.verbose > 1:
                print("new sub_data with key", key)
                print("sub_data_keys are now", self.sub_data_keys)

            new_name = "{}".format(self.counter)
            kwargs = {'name': new_name, 'magic': MAGIC_SIGN}
            
            self.db[KEY_COUNTER] = self.counter
            self.db[KEY_SUB_DATA_KEYS] = self.sub_data_keys
            self.db[key] = kwargs
            self.db.commit()

            kwargs.pop('magic')
            return PersistentDataStructure(name = new_name, path = os.path.join(self._dirname) , verbose = self.verbose)
        else:
            raise RuntimeError("can NOT create new SubData, key already found!")
        
    def getData(self, key, create_sub_data = False):
        self.need_open()
        if key in self.db:
            if self.verbose > 1:
                print("getData key exists")
 
            if self.is_subdata(key): 
                sub_db_name = self.db[key]['name']
            
                if self.verbose > 1:
                    print("return subData stored as key", key, "using name", sub_db_name)
                return PersistentDataStructure(name = sub_db_name, path = os.path.join(self._dirname) , verbose = self.verbose)
            else:
                if self.verbose > 1:
                    print("return normal value")
                return self.db[key] 
        else:
            if not create_sub_data:
                if isinstance(key, bytes):
                    key = pickle.loads(key)
                print("KEY NOT FOUND:")
                print(key)
                raise KeyError("key not found")
            else:
                if self.verbose > 1:
                    print("getData key does NOT exists -> create subData")
                return self.newSubData(key)
            
    def setDataFromSubData(self, key, subData):
        """
            set an entry of the PDS with data from an other PDS
            
            this means copying the appropriate file to the right place
            and rename them
        """
        self.need_open()
        self.__check_key(key)                                       # see if key is valid
        if self.is_subdata(key):                                    # check if key points to existing PDS 
            with self[key] as pds:                                  #
                name = pds._name                                    #    remember its name
                dir_name = pds._dirname                             #    and the directory where it's in     
                pds.erase()                                         #    remove the existing subData from hdd  
        else:
            with self.newSubData(key) as new_sub_data:              #    create a new subData
                name = new_sub_data._name                           #    and remember name and directory
                dir_name = new_sub_data._dirname
                new_sub_data.erase()
        
        shutil.copytree(src=subData._dirname, dst=dir_name)
        os.rename(src=os.path.join(dir_name, subData._name+'.db'), dst=os.path.join(dir_name, name+'.db'))

    def __len__(self):
        self.need_open()
        return len(self.db) - 2
            
    # implements the iterator
    def __iter__(self):
        self.need_open()
        db_iter = self.db.__iter__()
        while True:
            next_item = db_iter.__next__()
            while next_item in RESERVED_KEYS: 
                next_item = db_iter.__next__()
            yield next_item 
    
    # implements the 'in' statement 
    def __contains__(self, key):
        self.need_open()
        return (key in self.db)
            
    # implements '[]' operator getter
    def __getitem__(self, key):
        self.need_open()
        self.__check_key(key)
        return self.getData(key, create_sub_data=False)
    
    # implements '[]' operator setter
    def __setitem__(self, key, value):
        self.need_open()
        self.__check_key(key)
#         if key in self.db:
#             if self.__is_sub_data(self.db[key]):
#                 raise RuntimeWarning("values which hold sub_data structures can not be overwritten!")
#                 return None
        
        if self.verbose > 1:
            print("set", key, "to", value, "in", self._filename)
            
        if isinstance(value, PersistentDataStructure):
            self.setDataFromSubData(key, value)
        else:
            self.db[key] = value
            self.db.commit()
        
        
    # implements '[]' operator deletion
    def __delitem__(self, key):
        self.need_open()
        self.__check_key(key)
        
        if self.is_subdata(key):
            with self[key] as pds:
                pds.erase()
            
            self.sub_data_keys.remove(key)
            self.db[KEY_SUB_DATA_KEYS] = self.sub_data_keys
                
        del self.db[key]
        self.db.commit()
            
        

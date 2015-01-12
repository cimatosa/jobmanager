import sqlitedict as sqd
from os.path import abspath, join, exists
import os
import traceback

MAGIC_SIGN = 0xff4a87
RESERVED_KEYS = (0, 1)

class PersistentDataStructure(object):
    def __init__(self, name, path="./", verbose=1):
        self.__name = name
        self.__path = abspath(path)
        if not exists(self.__path):
            raise RuntimeError("given path does not exists ({} -> {})".format(path, self.__path))
        
        self.verbose = verbose
        
        # create directory to hold sub structures
        self.__dir_name = join(self.__path, "__" + self.__name)
        if not exists(self.__dir_name):
            os.mkdir(self.__dir_name)
        
        # open actual sqltedict
        self.__filename = join(self.__dir_name, self.__name + '.db')
        self.open()
        
        if 0 in self.db:
           self.counter = self.db[0] 
        else:
            self.counter = 0
            
        if 1 in self.db:
            self.sub_data_keys = self.db[1]
        else:
            self.sub_data_keys = set()
            
    def _consistency_check(self):
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
            print("exit called for        {} in {}".format(self.__name, self.__dir_name))
        self.close()
        
    def open(self):
        """
            open the SQL database at self.__filename = <path>/__<name>/<name>.db
            as sqlitedict
        """
        if self.verbose > 1:
            print("open db                {} in {}".format(self.__name, self.__dir_name))             
        self.db = sqd.SqliteDict(filename = self.__filename, autocommit=False)
        
    def is_open(self):
        """
            assume the sqligtedict and therefore the SQL database
             to be opened when self.db[0] is accessible
        """
        try:
            self.db[0]
            return True
        except:
            return False
        
    def is_closed(self):
        return not self.is_open()
        
    def close(self):
        """
            close the sqligtedict ans therefore the SQL database
        """
        try:
            self.db.close()
            if self.verbose > 1:
                print("closed db              {} in {}".format(self.__name, self.__dir_name))
        except:
            if self.verbose > 1:
                print("db seem already closed {} in {}".format(self.__name, self.__dir_name))
            
    def erase(self):
        """
            removed the database file from the disk
            
            this is called recursively for all sub PersistentDataStructure
        """
        if self.verbose > 1:
            print("erase db               {} in {}".format(self.__name, self.__dir_name))        

        if self.is_closed():
            self.open()
            
        try:

            if self.verbose > 1:
                print("sub_data_keys:", self.sub_data_keys)
            for key in self.sub_data_keys:
                if self.verbose > 1:
                    print("call erase for key:", key, "on file", self.__filename)
                sub_data = self.getData(key)
                sub_data.erase()
        except:
            traceback.print_exc()
        finally:
            self.close()

        os.remove(path = self.__filename)
        try:
            os.rmdir(path = self.__dir_name)
        except OSError as e:
            if self.verbose > 0:
                print("Warning: directory structure can not be deleted")
                print("         {}".format(e))
       
    def __check_key(self, key):
        """
            returns True if the key does NOT collide with some reserved keys
            
            otherwise a RuntimeError will be raised
        """
        if key in RESERVED_KEYS:
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
        return (key in self.db)
        
    def setData(self, key, value, overwrite=False):
        """
            write the key value pair to the data base
            
            if the key already exists, overwrite must be
            set True in oder to update the data for
            that key in the database 
        """
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
        if not key in self.db:
            self.counter += 1
            self.sub_data_keys.add(key)
            if self.verbose > 1:
                print("new sub_data with key", key)
                print("sub_data_keys are now", self.sub_data_keys)

            new_name = "{}".format(self.counter)
            kwargs = {'name': new_name, 'path': self.__dir_name, 'magic': MAGIC_SIGN}
            
            self.db[0] = self.counter
            self.db[1] = self.sub_data_keys
            self.db[key] = kwargs
            self.db.commit()

            kwargs.pop('magic')
            return PersistentDataStructure(verbose = self.verbose, **kwargs)
        else:
            raise RuntimeError("can NOT create new SubData, key already found!")
        
    def getData(self, key, create_sub_data = False):
        if key in self.db:
            if self.verbose > 1:
                print("getData key exists")
            value = self.db[key]
            if self.__is_sub_data(value):
                if self.verbose > 1:
                    print("return subData")
                return PersistentDataStructure(verbose = self.verbose, **value)
            else:
                if self.verbose > 1:
                    print("return normal value")
                return value 
        else:
            if not create_sub_data:
                raise KeyError("key '{}' not found".format(key))
            else:
                if self.verbose > 1:
                    print("getData key does NOT exists -> create subData")
                return self.newSubData(key)
            
    def __contains__(self, key):
        return (key in self.db)
            
    def __getitem__(self, key):
        self.__check_key(key)
        return self.getData(key, create_sub_data=False)
    
    def __setitem__(self, key, value):
        self.__check_key(key)
        if key in self.db:
            if self.__is_sub_data(self.db[key]):
                raise RuntimeWarning("values which hold sub_data structures can not be overwritten!")
                return None
        
        if self.verbose > 1:
            print("set", key, "to", value, "in", self.__filename)
        self.db[key] = value
        self.db.commit()
        
    def __delitem__(self, key):
        self.__check_key(key)
        value = self.db[key]
        if self.__is_sub_data(value):
            with PersistentDataStructure(verbose = self.verbose, **value) as pds:
                pds.erase()
            
            del self.sub_data_keys[key]
            self.db[1] = sub_data_keys
                
        del self.db[key]
        self.db.commit()
            
        

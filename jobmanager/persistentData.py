import sqlitedict as sqd
from os.path import abspath, join, exists
import os

MAGIC_SIGN = 0xff4a87

class PersistentDataStructure(object):
    def __init__(self, name, path="./", verbose=0):
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
        self.db = sqd.SqliteDict(filename = self.__filename, autocommit=True)
        
        if not 0 in self.db:
            self.db[0] = 1
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self.verbose > 1:
            print("exit called for '{}' in '{}'".format(self.__name, self.__dir_name))
        self.close()
        
    def close(self):
        self.db.close()
        if self.verbose > 1:
            print("closed db '{}' in '{}'".format(self.__name, self.__dir_name))
            
       
    def __check_key(self, key):
        if key == 0:
            raise RuntimeError("key must not be 0 (reserved key)")
        
        return True
    
    def __is_sub_data(self, value):
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
            db0 = self.db[0]
            new_name = "{}".format(db0)
            self.db[0] = db0+1
            kwargs = {'name': new_name, 'path': self.__dir_name, 'magic': MAGIC_SIGN}
            self.db[key] = kwargs
            kwargs.pop('magic')
            return PersistentDataStructure(verbose = self.verbose, **kwargs)
        else:
            raise RuntimeError("can NOT create new SubData, key already found!")
        
    def getData(self, key, create_sub_data = False):
        if key in self.db:
            value = self.db[key]
            if self.__is_sub_data(value):
                return PersistentDataStructure(**value)
            else:
                return value 
        else:
            if not create_sub_data:
                raise KeyError
            else:
                return self.newSubData(key)
            
    def __getitem__(self, key):
        return self.db[key]
    
    def __setitem__(self, key, value):
        if key in self.db:
            if self.__is_sub_data(self.db[key]):
                raise RuntimeWarning("values which hold sub_data structures can not be overwritten!")
                return None
            
        self.db[key] = value
        

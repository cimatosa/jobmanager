import sqlitedict as sqd
import os

MAGIC_SIGN = 0xff4a87

class PersistentDataStructure(object):
    def __init__(self, name, path=""):
        self.__name = name
        self.__path = path
        
        # create directory to hold sub structures
        dir_name = os.path.join(path, name)
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)
        
        # open actual sqltedict
        file_name = os.path.join(path, name + '.db')
        self.db = sqd.SqliteDict(filename = file_name, autocommit=True)
        
        if not 0 in self.db:
            self.db[0] = 1
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        print("exit called for '{}' in '{}'".format(self.__name, self.__path))
        self.close()
        
    def close(self):
        self.db.close()
        print("closed db '{}' in '{}'".format(self.__name, self.__path))
        
    def __check_key(self, key):
        if key == 0:
            raise RuntimeError("key must not be 0 (reserved key)")
        
        return True
    
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
            kwargs = {'name': new_name, 'path': os.path.join(self.__path,self.__name), 'magic': MAGIC_SIGN}
            self.db[key] = kwargs
            kwargs.pop('magic')
            return PersistentDataStructure(**kwargs)
        else:
            raise RuntimeError("can NOT create new SubData, key already found!")
        
    def getData(self, key, create_sub_data = False):
        if key in self.db:
            value = self.db[key]
            try:
                assert value['magic'] == MAGIC_SIGN
                value.pop('magic')
                return PersistentDataStructure(**value)
            except: 
                return value 
        else:
            if not create_sub_data:
                raise KeyError
            else:
                return self.newSubData(key)
            
            

        
 
        

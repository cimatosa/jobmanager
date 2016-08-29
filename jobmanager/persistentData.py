from __future__ import division, print_function

import sqlitedict as sqd
import h5py
import hashlib
from os.path import abspath, join, exists
import os
import sys
import shutil
import traceback
import pickle
import warnings
import random

import progress

try:
    import numpy as np
    _NP = True
except ImportError:
    warnings.warn("could not import 'numpy', I can not treat np.ndarray separately!")
    _NP = False
    

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
MAGIC_SIGN_NPARRAY = 0xee4a87

TYPE_ORD = 0x00
TYPE_SUB = 0x01
TYPE_NPA = 0x02

def key_to_str(key, max_len = 255):
    if isinstance(key, (bytearray, bytes)):
        return "<binary key>"
    s = str(key)
    if len(s) > max_len:
        return s[:max_len] + ' ...'
    else:
        return s
    
RAND_STR_ASCII_IDX_LIST = list(range(48,58)) + list(range(65,91)) + list(range(97,123)) 
def rand_str(l = 8):
    s = ''
    for i in range(l):
        s += chr(random.choice(RAND_STR_ASCII_IDX_LIST))
    return s
   
    

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
            print("given path does not exists ({} -> {})".format(path, self._path))
            print("create path")
            os.makedirs(self._path)
            
        
        self.verbose = verbose
        
        # create directory to hold sub structures
        self._dirname = join(self._path, "__" + self._name)
        if not exists(self._dirname):
            os.mkdir(self._dirname)
        
        # open actual sqltedict
        self._filename = join(self._dirname, self._name + '.db')
        self._l = 8
        self.open()
        
        
                
    def _repair(self):
        raise DeprecationWarning        
            
    def _consistency_check(self):
        raise DeprecationWarning
        
    def _new_rand_file_name(self, make_dir = False, end=''):
        c = 0
        while True:
            fname = rand_str(self._l) + end
            if not make_dir:
                full_name = join(self._dirname, fname)
                if not os.path.exists(full_name):
                    #open(full_name, 'a').close()
                    return fname
            else:
                full_name = join(self._dirname, '__'+fname)
                if not os.path.exists(full_name):
                    os.mkdir(full_name)
                    return fname
                
            c += 1
            if c > 10:
                self._l += 2
                c = 0
                print("INFO: increase random file name length to", self._l)
        
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

            for key in self:
                t, v = self.get_value_and_value_type(key)
                if t == TYPE_SUB:
                    with self.getData(key) as sub_data:
                        sub_data.erase()
                elif t == TYPE_NPA:
                    os.remove(os.path.join(self._dirname, v['fname']))

        except:
            traceback.print_exc()
        finally:
            self.close()

        if self.verbose > 1:
            print("remove", self._filename)
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
               
        for key in self:
            t, v = self.get_value_and_value_type(key)
            if t == TYPE_SUB:
                with self.getData(key) as sub_data:
                    sub_data.erase()
            elif t == TYPE_NPA:
                os.remove(os.path.join(self._dirname, v['fname']))
                        
        self.db.clear()        

    def show_stat(self, recursive = False, prepend = ""):
        prepend += self._name
        print("{}: I'm a pds called {}".format(prepend, self._name))
        print("{}:     dirname  :{}".format(prepend, self._dirname))
        print("{}:     filename :{}".format(prepend, self._filename))
        
        self.need_open()
        
        str_key = 0
        bin_key = 0
        oth_key = 0
        
        sub_c = 0
        npa_c = 0
        
        sub_data_keys = set()
        
        for k in self:
            if isinstance(k, str):
                str_key += 1
            elif isinstance(k, bytes):
                bin_key += 1
            else:
                oth_key += 1
                
            t, v = self.get_value_and_value_type(k)
            if t == TYPE_NPA:
                npa_c += 1
            elif t == TYPE_SUB:
                sub_c += 1
                sub_data_keys.add(k)
                
        print("{}:     number of string keys: {}".format(prepend, str_key))
        print("{}:     number of byte   keys: {}".format(prepend, bin_key))
        if oth_key > 0:
            print("{}:     number of other  keys: {}".format(prepend, oth_key))
        print("{}:     number of subdata: {}".format(prepend, sub_c))
        print("{}:     nparray counter: {}".format(prepend, npa_c))
        print()
        sys.stdout.flush()
        if recursive:
            for k in sub_data_keys:
                print("show stat for subdata with key {}".format(key_to_str(k)))
                sys.stdout.flush()
                with self.getData(k) as subdata:
                    subdata.show_stat(recursive = recursive,
                                      prepend = prepend + "->")
            
    
    def __is_sub_data(self, value):
        """
            determine if the value gotten from the sqlitedict refers
            to a sub PersistentDataStructure
            
            this is considered the case if the value itself has an index 'magic'
            whose value matches a magic sign defined by MAGIC_SIGN 
        """
        try:
            assert value['magic'] == MAGIC_SIGN
            return True
        except:
            return False

    def __is_nparray(self, value):
        """
            determine if the value gotten from the sqlitedict refers
            to a numpy array which is stored in a seperate file
            
            this is considered the case if the value itself has an index 'magic'
            whose value matches a magic sign defined by MAGIC_SIGN_NPARRAY 
        """
        try:
            assert value['magic'] == MAGIC_SIGN_NPARRAY
            return True
        except:
            return False        
    
    def has_key(self, key):
        self.need_open()
        return (key in self.db)
    
    def is_subdata(self, key):
        return (key in self) and self.__is_sub_data(self.db[key])
        
    def is_NPA(self, key):
        return (key in self) and self.__is_nparray(self.db[key])
    
    def get_value_and_value_type(self, key):
        v = self.db[key]
        if self.__is_nparray(v):
            return TYPE_NPA, v
        elif self.__is_sub_data(v):
            return TYPE_SUB, v
        else:
            return TYPE_ORD, v
        
    def setData(self, key, value, overwrite=False):
        """
            write the key value pair to the data base
            
            if the key already exists, overwrite must be
            set True in oder to update the data for
            that key in the database 
        """
        self.need_open()
        
        if (overwrite) and (key in self.db):
            if self.verbose > 1:
                print("overwrite True: del key")
            self.__delitem__(key)         
         
        if not key in self.db:
            if _NP and isinstance(value, np.ndarray):
                if self.verbose > 1:
                    print("set nparray")
                return self._setNPA(key, nparray=value)
            else:
                if self.verbose > 1:
                    print("set normal value")
                self.db[key] = value
                self.db.commit()
                return True
        else:
            if overwrite:
                raise RuntimeError("this can not happen -> if so, pls check code!")
            raise KeyError("could not set data, key exists, and overwrite is False")
            


    
    def _setNPA(self, key, nparray):
        d = {'fname': self._new_rand_file_name(end='.npy'),
             'magic': MAGIC_SIGN_NPARRAY}
        if self.verbose > 1:
            print("set NPA (key)", key, " (fname)", d['fname'])
        self.db[key] = d
        self.db.commit()

        full_name = os.path.join(self._dirname, d['fname'])
        np.save(full_name, nparray)
        return True
    
    def _loadNPA(self, fname):
        return np.load(os.path.join(self._dirname, fname))
    
    def _getNPA(self, key):
        d = self.db[key]
        assert d['magic'] == MAGIC_SIGN_NPARRAY
        fname = d['fname']
        if self.verbose > 1:
            print("load NPA (key)", key, " (fname)", fname)
        return self._loadNPA(fname)
        
        
            
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
            d = {'name': self._new_rand_file_name(make_dir=True),
                 'magic': MAGIC_SIGN}
            
            if self.verbose > 1:
                print("newSubData (key)", key, " (name)", d['name'])
            
            self.db[key] = d
            self.db.commit()
            return self.__class__(name = d['name'], path = os.path.join(self._dirname) , verbose = self.verbose)
        else:
            raise RuntimeError("can NOT create new SubData, key already found!")
        
    def getData(self, key, create_sub_data = False):
        self.need_open()
        if key in self.db:
            if self.verbose > 1:
                print("getData key exists")
                
            t, v = self.get_value_and_value_type(key)
 
            if t == TYPE_SUB: 
                sub_db_name = v['name']
            
                if self.verbose > 1:
                    print("return subData stored as key", key, "using name", sub_db_name)
                return self.__class__(name = sub_db_name, path = os.path.join(self._dirname) , verbose = self.verbose)
            elif t == TYPE_NPA:
                if self.verbose > 1:
                    print("return nparray value")
                return self._loadNPA(v['fname'])
            else:
                if self.verbose > 1:
                    print("return normal value")
                return v 
        else:
            if not create_sub_data:
                raise KeyError("key not found\n{}".format(key_to_str(key)))
            else:
                if self.verbose > 1:
                    print("getData key does NOT exists -> create subData")
                return self.newSubData(key)
            
    def setDataFromSubData(self, key, subData, overwrite=False):
        """
            set an entry of the PDS with data from an other PDS
            
            this means copying the appropriate file to the right place
            and rename them
        """
        self.need_open()
        
        if key in self.db:
            if overwrite:
                if self.verbose > 1:
                    print("overwrite True: del key")
                self.__delitem__(key) 
            else:
                raise RuntimeError("can NOT create new SubData from Data, key already found!") 
                
        
        d = {'name': self._new_rand_file_name(make_dir=True),
             'magic': MAGIC_SIGN}
        self.db[key] = d
        self.db.commit()
        
        if self.verbose > 1:
            print("")
            print("setDataFromSubData: orig SubData (name)", subData._name, "new SubData (key)", key, " (name)", d['name'])
        
        dest_dir = os.path.join(self._dirname, '__'+d['name'])
        os.removedirs(dest_dir)

        shutil.copytree(src=subData._dirname, dst=dest_dir)
        os.rename(src=os.path.join(dest_dir, subData._name+'.db'), 
                  dst=os.path.join(dest_dir, d['name']+'.db'))
        
    def mergeOtherPDS(self, other_db_name, other_db_path = './', update = 'error', status_interval=5):
        """
            update determines the update scheme
                error : raise error when key exists
                ignore: do nothing when key exists, keep old value
                update: update value when key exists with value from otherData 
        """
        error = False
        if update == 'error':
            error = True
        elif update == 'ignore':
            ignore = True
        elif update == 'update':
            ignore = False
        else:
            raise TypeError("update must be one of the following: 'error', 'ignore', 'update'")
        
        transfered = 0
        ignored = 0
        
        if status_interval == 0:
            PB = progress.ProgressSilentDummy
        else:
            PB = progress.ProgressBarFancy   
        
        with self.__class__(name = other_db_name, 
                            path = other_db_path, 
                            verbose = self.verbose) as otherData:

            c = progress.UnsignedIntValue(val=0)
            m = progress.UnsignedIntValue(val=len(otherData))
            
            with PB(count=c, max_count=m, verbose=self.verbose, interval=status_interval) as pb:
                pb.start()
                for k in otherData:
        
                    if k in self:
                        if error:
                            raise KeyError("merge error, key already found in PDS")
                        else:
                            if ignore:
                                if self.verbose > 1:
                                    print("ignore key", k)
                                ignored += 1
                                continue
                            else:
                                if self.verbose > 1:
                                    print("replace key", k)
                                del self[k]
        
                    value = otherData[k]
                    try:
                        self[k] = value
                        transfered += 1
                    finally:
                        if isinstance(value, self.__class__):
                            value.close()
                    
                    with c.get_lock():
                        c.value += 1
                    sys.stdout.flush()
            
            print("merge summary:")
            print("   transfered values:", transfered)
            print("      ignored values:", ignored)

    def __len__(self):
        self.need_open()
        return len(self.db)
            
    # implements the iterator
    def __iter__(self):
        self.need_open()
        db_iter = self.db.__iter__()
        while True:
            next_item = db_iter.__next__()
            yield next_item 
    
    # implements the 'in' statement 
    def __contains__(self, key):
        self.need_open()
        return (key in self.db)
            
    # implements '[]' operator getter
    def __getitem__(self, key):
        self.need_open()
        return self.getData(key, create_sub_data=False)
    
    # implements '[]' operator setter
    def __setitem__(self, key, value):
        if isinstance(value, self.__class__):
            self.setDataFromSubData(key, value, overwrite=True)
        else:
            self.setData(key, value, overwrite=True)
            
        if self.verbose > 1:
            print("set", key, "to", value, "in", self._filename)            
        
        
    # implements '[]' operator deletion
    def __delitem__(self, key):
        self.need_open()
        t, v = self.get_value_and_value_type(key)
        if t == TYPE_SUB:
            with self[key] as pds:
                pds.erase()
        elif t == TYPE_NPA:
            fname = v['fname']
            os.remove(os.path.join(self._dirname, fname))
                
        del self.db[key]
        self.db.commit()
            
        
class PersistentDataStructure_HDF5(object):
    def __init__(self, name='', path="./", gr=None, verbose=1):
        self.__classname = self.__class__.__name__
        self.verbose = verbose
        if gr is None:
            self._open = False
            self._name = name
            self._path = abspath(path)
            if not exists(self._path):
                print("given path does not exists ({} -> {})".format(path, self._path))
                print("create path")
                os.makedirs(self._path)
            self._filename = join(self._path, self._name + '.hdf5')
            self.open()
        else:
            self._open = True
            self._name = None
            self._path = None
            self._filename = None
            self.db = gr
        
    def _md5(self, key):
        if isinstance(key, str):
            key = key.encode('utf8')
        return hashlib.md5(key).hexdigest()        
    
    def _convkey(self, key):
        if isinstance(key, str):
            return key
        elif isinstance(key, (bytearray, bytes)):
            return np.void(key)
        else:
            raise TypeError("bad key type")
                      
                       
    # implements '[]' operator getter
    def __getitem__(self, key):
        _md5 = self._md5(key)       
        if self.verbose > 2:
            print("__getitem__")
            print("key : ", key)
            print("md5 : ", _md5)        
        try:
            gr_md5 = self.db[_md5]
            if self.verbose > 2:
                print("gr_md5 found")            
        except KeyError:
            raise KeyError("key not found in {}".format(self.__classname))
                           
        for k in gr_md5:
            test_key = gr_md5[k].attrs['key']
            if isinstance(test_key, np.void):
                test_key = test_key.tostring()
            if self.verbose > 2:
                print("test against key", test_key)

            if key == test_key:
                dat = gr_md5[k]
                print(type(dat))
                print()
                if isinstance(dat, h5py.Dataset):
                    return gr_md5[k].value
                else:
                    return PersistentDataStructure_HDF5(gr=gr_md5[k], verbose=self.verbose)
        
        raise KeyError("key not found in {}".format(self.__classnme))

    # implements the 'in' statement
    def __contains__(self, key):
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False
        
    def __create_group(self, key):
        _md5 = self._md5(key)
        if self.verbose > 2:
            print("__create_group")
            print("key : ", key)
            print("md5 : ", _md5)
        try:
            gr_md5 = self.db[_md5]
            if self.verbose > 2:
                print("found md5 group")            
        except KeyError:
            gr_md5 = self.db.create_group(_md5)
            if self.verbose > 2:
                print("create md5 group")
            
        for dat in gr_md5:
            test_key = dat.attrs['key']
            if isinstance(test_key, np.void):
                test_key = test_key.tostring()
            if self.verbose > 2:
                print("compare with test_key in md5_group: ", format(test_key))                    
            if key == test_key:
                raise RuntimeError("key must not exist when creating a group")

        n = len(gr_md5)            
        gr = gr_md5.create_group('gr{}'.format(n))
        gr.attrs['key'] = self._convkey(key)
        if self.verbose > 2:
            print("create group as {}th object in gr_md5".format(n))        
        return gr
    
    def __set_dataset(self, key, data):
        _md5 = self._md5(key)
        if self.verbose > 2:
            print("__set_dataset")
            print("key : ", key)
            print("md5 : ", _md5)
            print("data: ", data)
        try:
            gr_md5 = self.db[_md5]
            if self.verbose > 2:
                print("found md5 group")
        except KeyError:
            gr_md5 = self.db.create_group(_md5)
            print("create md5 group")
            
        for k in gr_md5:
            test_key = gr_md5[k].attrs['key']
            if isinstance(test_key, np.void):
                test_key = test_key.tostring()
            if self.verbose > 2:
                print("compare with test_key in md5_group: ", format(test_key))                
                
            if key == test_key:
                raise RuntimeError("key must not exist when creating a dataset")

        n = len(gr_md5)
        try:
            dat = gr_md5.create_dataset('dat{}'.format(n), data=data)
            if self.verbose > 2:
                print("set dataset from pure data")
        except TypeError:
            dat = gr_md5.create_dataset('dat{}'.format(n), data=np.void(pickle.dumps(data)))
            if self.verbose > 2:
                print("set dataset from binary data")
            
        
        dat.attrs['key'] = self._convkey(key)
        return dat
    
    # implements '[]' operator setter
    def __setitem__(self, key, value):
        if isinstance(value, self.__class__):
            raise NotImplementedError
        else:
            self.__set_dataset(key, value)              
        
    def __len__(self):
        l = 0
        for gr_md5 in self.db:
            l += len(self.db[gr_md5])
        return l
        
    # implements '[]' operator deletion
    def __delitem__(self, key):
        _md5 = self._md5(key)
        try:
            gr_md5 = self.db[_md5]
        except KeyError:
            return
            
        for k in gr_md5:
            test_key = gr_md5[k].attrs['key']
            if isinstance(test_key, np.void):
                test_key = test_key.tostring()
                
            if key == test_key:
                del gr_md5[k]
        
        if len(gr_md5) == 0:
            del self.db[_md5]

    def open(self):
        if self._filename is None:
            raise RuntimeError("can not open a group")            
        self.db = h5py.File(self._filename)
        self._open = True
        
    def is_open(self):
        return self._open
        
    def is_closed(self):
        return not self._open
    
    def need_open(self):
        if self.is_closed():
            raise RuntimeError("{} needs to be open".format(self.__classname))
        
    def close(self):
        if self._filename is None:
            raise RuntimeError("can not close as group")
        
        self.db.close()
        self._open = False
                               
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self._filename is not None:
            self.close()
        
                
    def clear(self):
        self.need_open()
               
        for k in self.db:
            del self.db[k]
        for k in self.db.attrs:
            del self.db.attrs[k]
            
    def erase(self):
        if self.verbose > 1:
            print("remove", self._filename)
        os.remove(path = self._filename)                           
              
    
    def has_key(self, key):
        return self.__contains__(key)
        
    def setData(self, key, value, overwrite=False):
        if overwrite:
            self.__delitem__(key)
        self.__setitem__(key, value)
            
            
            
    def newSubData(self, key):
        pass
        
    def getData(self, key, create_sub_data = False):
        try:
            return self.__getitem__(key)
        except KeyError:
            if create_sub_data:
                return PersistentDataStructure_HDF5(gr = self.__create_group(key), verbose = self.verbose)
            else:
                raise
            
    def setDataFromSubData(self, key, subData, overwrite=False):
        pass
        
    def mergeOtherPDS(self, other_db_name, other_db_path = './', update = 'error', status_interval=5):
        pass

       
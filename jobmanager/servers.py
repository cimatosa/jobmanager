#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .jobmanager import JobManager_Server
import pickle 

def recursive_scan_for_instance(obj, type, explicit_exclude = None ):
    """
        try to do some recursive check to see whether 'obj' is of type
        'type' or contains items of 'type' type.
        
        if obj is a mapping (like dict) this will only check
        for item iterated over via
        
            for item in obj
            
        which corresponds to the keys in the dict case.
        
        The explicit_exclude argument may be a tuple of types for
        some explicit checking in the sense that if obj is an
        instance of one of the type given by explicit_exclude
        we know it is NOT an instance of type. 
    """
    
    # check this object for type
    if isinstance(obj, type):
        return True

    # check for some explicit types in order to conclude
    # that obj is not of type
    # see dict example
    if explicit_exclude is not None:
        if isinstance(obj, explicit_exclude):
            return False


    # if not of desired type, try to iterate and check each item for type
    try:
        for i in obj:
            # return True, if object is of type or contains type 
            if recursive_scan_for_instance(i, type) == True:
                return True
    except:
        pass

    # either object is not iterable and not of type, or each item is not of type -> return False    
    return False

def recursive_scan_for_dict_instance(obj):
    # here we explicitly check against the 'str' class
    # as it is iterable, but can not contain an dict as item, only characters
    return recursive_scan_for_instance(obj, type=dict, explicit_exclude=(str, ))
            
def data_as_binary_key(data):
    # since the hash value of a string is randomly seeded each time python
    # is started -> the order of the entries in a dict is not guaranteed
    # and therefore its binary representation may vary
    # this forbids to use dicts as a key for persistent data structures
    if recursive_scan_for_dict_instance(data):
        raise RuntimeError("data used as 'key' must not include dictionaries!")
    
    # protocol 2 ensures backwards compatibility 
    # (at least here) down to Python 2.3
    return pickle.dumps(data, protocol=2)

class PersistentData_Server(JobManager_Server):
    def __init__(self, 
                 persistent_data_structure,
                 authkey, 
                 const_arg=None, 
                 port=42524, 
                 verbose=1, 
                 msg_interval=1, 
                 fname_dump=None,
                 speed_calc_cycles=50,
                 overwrite=False):
         
        JobManager_Server.__init__(self, authkey, const_arg=const_arg, port=port, verbose=verbose, msg_interval=msg_interval, fname_dump=fname_dump, speed_calc_cycles=speed_calc_cycles)
        self.pds = persistent_data_structure
        self.overwrite = overwrite
        if self.verbose > 1:
            if self.overwrite:
                print("{}: overwriting existing data is ENABLED".format(self._identifier))
            else:
                print("{}: overwriting existing data is DISABLED".format(self._identifier))
         
    def process_new_result(self, arg, result):
        """
            use arg.id as key to store the pair (arg, result) in the data base
            
            return True, if a new data set was added (key not already in pds)
            otherwise false
        """
        key = data_as_binary_key(arg.id)
        has_key = key in self.pds
        self.pds[key] = (arg, result)
        return not has_key
        
    def put_arg(self, a):
        a_bin = data_as_binary_key(a.id)
        if self.overwrite or (not a_bin in self.pds):
            JobManager_Server.put_arg(self, a)
            return True
        
        return False
        

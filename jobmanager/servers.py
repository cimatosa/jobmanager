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
            
def as_binary_data(a):
    if isinstance(a, dict):
        raise RuntimeError()
    
    return pickle.dumps(a)

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
         
    def process_new_result(self, arg, result):
        self.pds[as_binary_data(arg)] = result
        self.pds.commit()
        
    def put_arg(self, a):
        a_bin = as_binary_data(a)
        if overwrite or (not a_bin in self.pds):
            JobManager_Server.put_arg(self, a)
        

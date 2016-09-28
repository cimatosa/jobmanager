#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binfootprint

from .jobmanager import JobManager_Server

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
        key = binfootprint.dump(arg.id)
        has_key = key in self.pds
        self.pds[key] = (arg, result)
        return not has_key
        
    def put_arg(self, a):
        a_bin = binfootprint.dump(a.id)
        if self.overwrite or (not a_bin in self.pds):
            JobManager_Server.put_arg(self, a)
            return True
        
        return False
        

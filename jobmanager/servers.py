#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
                 speed_calc_cycles=50):
         
        JobManager_Server.__init__(self, authkey, const_arg=const_arg, port=port, verbose=verbose, msg_interval=msg_interval, fname_dump=fname_dump, speed_calc_cycles=speed_calc_cycles)
        self.pds = persistent_data_structure
         
    def process_new_result(self, arg, result):
        self.pds[arg] = result
        self.pds.commit()

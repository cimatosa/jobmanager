#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import copy
import functools
import inspect
import multiprocessing as mp
from multiprocessing.managers import SyncManager
import numpy as np
import os
import pickle
import signal
import socket
import sys
import time
import traceback

# This is a list of all python objects that will be imported upon
# initialization during module import (see __init__.py)
__all__ = ["JobManager_Client",
           "JobManager_Local",
           "JobManager_Server",
           "hashDict",
          ]
           

# Magic conversion from 3 to 2
if sys.version_info[0] == 2:
    # Python 2
    import Queue as queue
    
    class getfullargspec(object):
        "A quick and dirty replacement for getfullargspec for Python 2"
        def __init__(self, f):
            self.args, self.varargs, self.varkw, self.defaults = \
                inspect.getargspec(f)
            self.annotations = getattr(f, '__annotations__', {})
    inspect.getfullargspec = getfullargspec
    
else:
    # Python 3
    import queue

sys.path.append(os.path.dirname(__file__))
import progress

myQueue = mp.Queue


"""jobmanager module

Richard Hartmann 2014


This module provides an easy way to implement distributed computing
based on the python class SyncManager for remote communication
and the python module multiprocessing for local parallelism.

class SIG_handler_Loop

The class Loop provides as mechanism to spawn a process repeating to 
call a certain function as well as a StatusBar class for the terminal.

class StatusBar

The class JobManager_Server will provide a server process handling the
following tasks:
    - providing a list (queue) of arguments to be processed by client processes 
    (see put_arg and args_from_list)
    - handling the results of the calculations done by the client processes
    (see process_new_result)
    - when finished (all provided arguments have been processed and returned its result)
    process the obtained results (see process_final_result)
    
The class JobManager_Client
  
"""



# a list of all names of the implemented python signals
all_signals = [s for s in dir(signal) if (s.startswith('SIG') and s[3] != '_')]

# keyword arguments that define counting in wrapped functions
validCountKwargs = [
                    [ "count", "count_max"],
                    [ "count", "max_count"],
                    [ "c", "m"],
                    [ "jmc", "jmm"],
                   ]
                     
                     
def getDateForFileName(includePID = False):
    """returns the current date-time and optionally the process id in the format
    YYYY_MM_DD_hh_mm_ss_pid
    """
    date = time.localtime()
    name = '{:d}_{:02d}_{:02d}_{:02d}_{:02d}_{:02d}'.format(date.tm_year, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min, date.tm_sec)
    if includePID:
        name += "_{}".format(os.getpid()) 
    return name


def getCountKwargs(func):
    """ Returns a list ["count kwarg", "count_max kwarg"] for a
    given function. Valid combinations are defined in 
    `jobmanager.jobmanager.validCountKwargs`.
    
    Returns None if no keyword arguments are found.
    """
    # Get all arguments of the function
    func_args = func.__code__.co_varnames[:func.__code__.co_argcount]
    for pair in validCountKwargs:
        if ( pair[0] in func_args and pair[1] in func_args ):
            return pair
    # else
    return None
            

def copyQueueToList(q):
    res_list = []
    res_q = myQueue()
    
    try:
        while True:
            res_list.append(q.get_nowait())
            res_q.put(res_list[-1])
    except queue.Empty:
        pass

    return res_q, res_list

def try_pickle(obj, show_exception=False):
    blackhole = open(os.devnull, 'wb')
    try:
        pickle.dump(obj, blackhole)
        return True
    except:
        if show_exception:
            traceback.print_exc()
        return False
        
        
class hashDict(dict):
    def __hash__(self):
        try:
            return hash(tuple(sorted(self.items())))
        except:
            for i in self.items():
                try:
                    hash(i)
                except Exception as e:
                    print("item '{}' of dict is not hashable".format(i))
                    raise e
                    
    
class hashableCopyOfNumpyArray(np.ndarray):
    def __new__(self, other):
        return np.ndarray.__new__(self, shape=other.shape, dtype=other.dtype)

    def __init__(self, other):
        self[:] = other[:]
    
    def __hash__(self):
        return hash(self.shape + tuple(self.flat))

    def __eq__(self, other):
        return np.all(np.equal(self, other))






def setup_SIG_handler_manager():
    """
    When a process calls this functions, it's signal handler
    will be set to ignore the signals given by the list signals.
    
    This functions is passed to the SyncManager start routine (used
    by JobManager_Server) to enable graceful termination when received
    SIGINT or SIGTERM.
    
    The problem is, that the SyncManager start routine triggers a new 
    process to provide shared memory object even over network communication.
    Since the SIGINT signal will be passed to all child processes, the default
    handling would make the SyncManger halt on KeyboardInterrupt Exception.
    
    As we want to shout down the SyncManager at the last stage of cleanup
    we have to prevent this default signal handling by passing this functions
    to the SyncManager start routine.
    """
    Signal_to_SIG_IGN(signals=[signal.SIGINT, signal.SIGTERM], verbose=0)

class Signal_to_SIG_IGN(object):
    def __init__(self, signals=[signal.SIGINT, signal.SIGTERM], verbose=0):
        self.verbose = verbose
        for s in signals:
            signal.signal(s, self._handler)
    
    def _handler(self, sig, frame):
        if self.verbose > 0:
            print("PID {}: received signal {} -> will be ignored".format(os.getpid(), progress.signal_dict[sig]))
        
class Signal_to_sys_exit(object):
    def __init__(self, signals=[signal.SIGINT, signal.SIGTERM], verbose=0):
        self.verbose = verbose
        for s in signals:
            signal.signal(s, self._handler)
    def _handler(self, signal, frame):
        if self.verbose > 0:
            print("PID {}: received signal {} -> call sys.exit -> raise SystemExit".format(os.getpid(), progress.signal_dict[signal]))
        sys.exit('exit due to signal {}'.format(progress.signal_dict[signal]))
        
class Signal_handler_for_Jobmanager_client(object):
    def __init__(self, client_object, exit_handler, signals=[signal.SIGINT], verbose=0):
        self.client_object = client_object
        self.exit_handler = exit_handler
        self.verbose = verbose
        for s in signals:
            signal.signal(s, self._handler)
            
    def _handler(self, sig, frame):
        if self.verbose > 0:
            print("PID {}: received signal {}".format(os.getpid(), progress.signal_dict[sig]))
        
        if self.client_object.pbc is not None:
            self.client_object.pbc.pause()
        
        try:
            r = input("<q> - quit, <i> - server info: ")
        except:
            r = 'q'
            
            
        if r == 'i':
            self._show_server_info()
        elif r == 'q':
            print('PID {}: terminate worker functions'.format(os.getpid()))
            self.exit_handler._handler(sig, frame)
            print('PID {}: call sys.exit -> raise SystemExit'.format(os.getpid()))
            sys.exit('exit due to user')
        else:
            print("input '{}' ignored".format(r))
        
        if self.client_object.pbc is not None:
            self.client_object.pbc.resume()
        
    def _show_server_info(self):
        self.client_object.server
        self.client_object.authkey
        print("{}: connected to {} using authkey {}".format(self.client_object._identifier,
                                                            self.client_object.server,
                                                            self.client_object.authkey))    
        
class Signal_to_terminate_process_list(object):
    """
    SIGINT and SIGTERM will call terminate for process given in process_list
    """
    def __init__(self, process_list, signals = [signal.SIGINT, signal.SIGTERM], verbose=0, name='process', timeout=2):
        self.process_list = process_list
        self.verbose = verbose
        self.name = name
        self.timeout = timeout
        for s in signals:
            signal.signal(s, self._handler)
            
    def _handler(self, signal, frame):
        if self.verbose > 0:
            print("PID {}: received sig {} -> terminate all given subprocesses".format(os.getpid(), progress.signal_dict[signal]))
        for i, p in enumerate(self.process_list):
            p.terminate()
            progress.check_process_termination(proc=p, 
                                               identifier='{} {}'.format(self.name, i), 
                                               timeout=self.timeout,
                                               verbose=self.verbose,
                                               auto_kill_on_last_resort=False)
            

class JobManager_Server(object):
    """general usage:
    
        - init the JobManager_Server, start SyncManager server process
        
        - pass the arguments to be processed to the JobManager_Server
        (put_arg, args_from_list)
        
        - start the JobManager_Server (start), which means to wait for incoming 
        results and to process them. Afterwards process all obtained data.
        
    The default behavior of handling each incoming new result is to simply
    add the pair (arg, result) to the final_result list.
    
    When finished the default final processing is to dump the
    final_result list to fname_for_final_result_dump
    
    To change this behavior you may subclass the JobManager_Server
    and implement
        - an extended __init__ to change the type of the final_result attribute
        - process_new_result
        - process_final_result(self)
        
    In case of any exceptions the JobManager_Server will call process_final_result
    and dump the unprocessed job_q as a list to fname_for_job_q_dump.
    
    Also the signal SIGTERM is caught. In such a case it will raise SystemExit exception
    will will then be handle in the try except clause.
    
    SystemExit and KeyboardInterrupt exceptions are not considered as failure. They are
    rather methods to shout down the Server gracefully. Therefore in such cases no
    traceback will be printed.
    
    All other exceptions are probably due to some failure in function. A traceback
    it printed to stderr.
        
    notes:
        - when the JobManager_Server gets killed (SIGKILL) and the SyncManager still
        lives, the port used will occupied. considere sudo natstat -pna | grep 42524 to
        find the process still using the port
        
        - also the SyncManager ignores SIGTERM and SIGINT signals so you have to send
        a SIGKILL.  
    """
    def __init__(self, 
                  authkey,
                  const_arg=None, 
                  port=42524, 
                  verbose=1, 
                  msg_interval=1,
                  fname_dump='auto',
                  speed_calc_cycles=50):
        """
        authkey [string] - authentication key used by the SyncManager. 
        Server and Client must have the same authkey.
        
        const_arg [dict] - some constant keyword arguments additionally passed to the
        worker function (see JobManager_Client).
        
        port [int] - network port to use
        
        verbose [int] - 0: quiet, 1: status only, 2: debug messages
        
        msg_interval [int] - number of second for the status bar to update
        
        fname_for_final_result_dump [string/None] - sets the file name used to dump the the
        final_result. (None: do not dump, 'auto' choose filename 
        'YYYY_MM_DD_hh_mm_ss_final_result.dump')
        
        fname_for_args_dump [string/None] - sets the file name used to dump the list
        of unprocessed arguments, if there are any. (None: do not dump at all, 
        'auto' choose filename 'YYYY_MM_DD_hh_mm_ss_args.dump')
        
        fname_for_fail_dump [string/None] - sets the file name used to dump the list 
        of not successfully processed arguments, if there are any. 
        (None: do not dump, 'auto' choose filename 'YYYY_MM_DD_hh_mm_ss_fail.dump')
        
        This init actually starts the SyncManager as a new process. As a next step
        the job_q has to be filled, see put_arg().
        """
        self.verbose = verbose        
        self._pid = os.getpid()
        self._pid_start = None
        self._identifier = progress.get_identifier(name=self.__class__.__name__, pid=self._pid)
        if self.verbose > 1:
            print("{}: I'm the JobManager_Server main process".format(self._identifier))
        
        self.__wait_before_stop = 2
        
        self.port = port

        if isinstance(authkey, bytearray):
            self.authkey = authkey
        else: 
            self.authkey = bytearray(authkey, encoding='utf8')
            
        if not isinstance(const_arg, dict):
            raise RuntimeError('const_arg must be an instance of dict!')
        
#         for k in const_arg.keys():
#             if not try_pickle(const_arg[k], show_exception=True):
#                 raise RuntimeError("key '{}' of const_arg is not pickable!\n{}={}".format(k, k, const_arg[k]))
        
        self.const_arg = copy.copy(const_arg)
        
        
        self.fname_dump = fname_dump        
        self.msg_interval = msg_interval
        self.speed_calc_cycles = speed_calc_cycles

        # to do some redundant checking, might be removed
        # the args_set holds all arguments to be processed
        # in contrast to the job_q, an argument will only be removed
        # from the set if it was caught by the result_q
        # so iff all results have been processed successfully,
        # the args_set will be empty
        self.args_set = set()
        
        # thread safe integer values  
        self._numresults = mp.Value('i', 0)  # count the successfully processed jobs
        self._numjobs = mp.Value('i', 0)     # overall number of jobs
        
        # final result as list, other types can be achieved by subclassing 
        self.final_result = []
        
        # NOTE: it only works using multiprocessing.Queue()
        # the Queue class from the module queue does NOT work  
        self.job_q = myQueue()    # queue holding args to process
        self.result_q = myQueue() # queue holding returned results
        self.fail_q = myQueue()   # queue holding args where processing failed
        self.manager = None
        
        self.__start_SyncManager()
        
    def __stop_SyncManager(self):
        if self.manager == None:
            return
        
        manager_proc = self.manager._process
        manager_identifier = progress.get_identifier(name='SyncManager')
        
        # stop SyncManager
        self.manager.shutdown()
        
        progress.check_process_termination(proc=manager_proc, 
                                  identifier=manager_identifier, 
                                  timeout=2, 
                                  verbose=self.verbose, 
                                  auto_kill_on_last_resort=True)
                
    def __start_SyncManager(self):
        class JobQueueManager(SyncManager):
            pass
    
        # make job_q, result_q, fail_q, const_arg available via network
        JobQueueManager.register('get_job_q', callable=lambda: self.job_q)
        JobQueueManager.register('get_result_q', callable=lambda: self.result_q)
        JobQueueManager.register('get_fail_q', callable=lambda: self.fail_q)
        JobQueueManager.register('get_const_arg', callable=lambda: self.const_arg, proxytype=mp.managers.DictProxy)
    
        address=('', self.port)   #ip='' means local
        authkey=self.authkey
    
        self.manager = JobQueueManager(address, authkey)
        
        # start manager with non default signal handling given by
        # the additional init function setup_SIG_handler_manager
        self.manager.start(setup_SIG_handler_manager)
        self.hostname = socket.gethostname()
        
        if self.verbose > 1:
            print("{}: started on {}:{} with authkey '{}'".format(progress.get_identifier('SyncManager', self.manager._process.pid), 
                                                                  self.hostname, 
                                                                  self.port,  
                                                                  authkey))
    
    def __restart_SyncManager(self):
        self.__stop_SyncManager()
        self.__start_SyncManager()
        
    def __enter__(self):
        return self
        
    def __exit__(self, err, val, trb):
        # KeyboardInterrupt via SIGINT will be mapped to SystemExit  
        # SystemExit is considered non erroneous
        if err == SystemExit:
            if self.verbose > 0:
                print("{}: normal shutdown caused by SystemExit".format(self._identifier))
            # no exception traceback will be printed
        elif err != None:
            # causes exception traceback to be printed
            traceback.print_exception(err, val, trb)
             
        # bring everything down, dump status to file 
        self.shoutdown()
        return True
    
    @property    
    def numjobs(self):
        return self._numjobs.value
    @numjobs.setter
    def numjobs(self, numjobs):
        self._numjobs.value = numjobs
        
    @property    
    def numresults(self):
        return self._numresults.value
    @numresults.setter
    def numresults(self, numresults):
        self._numresults.value = numresults

    def shoutdown(self):
        """"stop all spawned processes and clean up
        
        - call process_final_result to handle all collected result
        - if job_q is not empty dump remaining job_q
        """
        # will only be False when _shutdown was started in subprocess
        
        # start also makes sure that it was not started as subprocess
        # so at default behavior this assertion will allays be True
        assert self._pid == os.getpid()
        
        self.__stop_SyncManager()
        
        self.show_statistics()
       
        # do user defined final processing
        self.process_final_result()
        
        
        print(self.fname_dump)
        if self.fname_dump is not None:
            if self.fname_dump == 'auto':
                fname = "{}_{}.dump".format(self.authkey.decode('utf8'), getDateForFileName(includePID=False))
            else:
                fname = self.fname_dump

            if self.verbose > 0:
                print("{}: dump current state to '{}'".format(self._identifier, fname))    
            with open(fname, 'wb') as f:
                self.__dump(f)
                

        else:
            if self.verbose > 0:
                print("{}: fname_dump == None, ignore dumping current state!".format(self._identifier))
        
        print("{}: JobManager_Server was successfully shout down".format(self._identifier))
        
    def show_statistics(self):
        if self.verbose > 0:
            all_jobs = self.numjobs
            succeeded = self.numresults
            failed = self.fail_q.qsize()
            all_processed = succeeded + failed
            
            print("total number of jobs  : {}".format(all_jobs))
            print("  processed   : {}".format(all_processed))
            print("    succeeded : {}".format(succeeded))
            print("    failed    : {}".format(failed))
            
            all_not_processed = all_jobs - all_processed
            not_queried = self.job_q.qsize()
            queried_but_not_processed = all_not_processed - not_queried  
            
            print("  not processed     : {}".format(all_not_processed))
            print("    queried         : {}".format(queried_but_not_processed))
            print("    not queried yet : {}".format(not_queried))
            print("len(args_set) : {}".format(len(self.args_set)))
            if (all_not_processed + failed) != len(self.args_set):
                raise RuntimeWarning("'all_not_processed != len(self.args_set)' something is inconsistent!")
            
            

    @staticmethod
    def static_load(f):
        data = {}
        data['numjobs'] = pickle.load(f)
#         print(data['numjobs'])
        
        data['numresults'] = pickle.load(f)
#         print(data['numresults'])
        
        data['final_result'] = pickle.load(f)
        
        data['args_set'] = pickle.load(f)
#         print(len(data['args_set']))
        
        fail_list = pickle.load(f)
        data['fail_set'] = {fail_item[0] for fail_item in fail_list}
        
        data['fail_q'] = myQueue()
        data['job_q'] = myQueue()
        
        for fail_item in fail_list:
            data['fail_q'].put_nowait(fail_item)
        for arg in (data['args_set'] - data['fail_set']):
            data['job_q'].put_nowait(arg)

        return data

    def __load(self, f):
        data = JobManager_Server.static_load(f)
        for key in ['numjobs', 'numresults', 'final_result',
                    'args_set', 'fail_q','job_q']:
            self.__setattr__(key, data[key])
        
    def __dump(self, f):
        pickle.dump(self.numjobs, f, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(self.numresults, f, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(self.final_result, f, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(self.args_set, f, protocol=pickle.HIGHEST_PROTOCOL)
        fail_list = []
        try:
            while True:
                fail_list.append(self.fail_q.get_nowait())
        except queue.Empty:
            pass
        pickle.dump(fail_list, f, protocol=pickle.HIGHEST_PROTOCOL)
        
#         print('numjobs', self.numjobs)
#         print('numresults', self.numresults)
#         print('final_result', self.final_result)
#         print('args_set', self.args_set)
#         print('fail_list', fail_list)
        

    def read_old_state(self, fname_dump=None):
        
        if fname_dump == None:
            fname_dump = self.fname_dump
        if fname_dump == 'auto':
            raise RuntimeError("fname_dump must not be 'auto' when reading old state")
        
        if not os.path.isfile(fname_dump):
            raise RuntimeError("file '{}' to read old state from not found".format(fname_dump))

        if self.verbose > 0:
            print("{}: load state from file '{}'".format(self._identifier, fname_dump))
        
        with open(fname_dump, 'rb') as f:
            self.__load(f)
        self.__restart_SyncManager()
        
        self.show_statistics()
            

    def put_arg(self, a):
        """add argument a to the job_q
        """
        if (not hasattr(a, '__hash__')) or (a.__hash__ == None):
            # try to add hashability
            if isinstance(a, dict):
                a = hashDict(a)
            elif isinstance(a, np.ndarray):
                a = hashableCopyOfNumpyArray(a)
            else:
                raise AttributeError("'{}' is not hashable".format(type(a)))
        
        self.args_set.add(copy.copy(a))
        self.job_q.put(copy.copy(a))
        
        with self._numjobs.get_lock():
            self._numjobs.value += 1
        
    def args_from_list(self, args):
        """serialize a list of arguments to the job_q
        """
        for a in args:
            self.put_arg(a)

    def process_new_result(self, arg, result):
        """Will be called when the result_q has data available.      
        result is the computed result to the argument arg.
        
        Should be overwritten by subclassing!
        """
        self.final_result.append((arg, result))
    
    def process_final_result(self):
        """to implement user defined final processing"""
        pass

    def start(self):
        """
        starts to loop over incoming results
        
        When finished, or on exception call stop() afterwards to shout down gracefully.
        """
        if self._pid != os.getpid():
            raise RuntimeError("do not run JobManager_Server.start() in a subprocess")

        if (self.numjobs - self.numresults) != len(self.args_set):
            if self.verbose > 1:
                print("numjobs: {}".format(self.numjobs))
                print("numresults: {}".format(self.numresults))
                print("len(self.args_set): {}".format(len(self.args_set)))
                
            raise RuntimeError("inconsistency detected! (self.numjobs - self.numresults) != len(self.args_set)! use JobManager_Server.put_arg to put arguments to the job_q")
        
        if self.numjobs == 0:
            raise RuntimeError("no jobs to process! use JobManager_Server.put_arg to put arguments to the job_q")
        
        Signal_to_sys_exit(signals=[signal.SIGTERM, signal.SIGINT], verbose = self.verbose)
        pid = os.getpid()
        
        if self.verbose > 1:
            print("{}: start processing incoming results".format(self._identifier))
        
        if self.verbose > 0:
            Progress = progress.ProgressBar
        else:
            Progress = progress.ProgressSilentDummy
  
        with Progress(count = self._numresults,
                       max_count = self._numjobs, 
                       interval = self.msg_interval,
                       speed_calc_cycles=self.speed_calc_cycles,
                       verbose = self.verbose,
                       sigint='ign',
                       sigterm='ign') as stat:

            stat.start()
        
            while (len(self.args_set) - self.fail_q.qsize()) > 0:
                try:
                    arg, result = self.result_q.get(timeout=1)
                    self.args_set.remove(arg)
                    self.numresults = self.numjobs - (len(self.args_set) - self.fail_q.qsize())
                    self.process_new_result(arg, result)
                except queue.Empty:
                    pass
        
        if self.verbose > 1:
            print("{}: wait {}s before trigger clean up".format(self._identifier, self.__wait_before_stop))
        time.sleep(self.__wait_before_stop)


class JobManager_Client(object):
    """
    Calls the functions self.func with arguments fetched from the job_q. You should
    subclass this class and overwrite func to handle your own function.
    
    The job_q is provided by the SycnManager who connects to a SyncManager setup
    by the JobManager_Server. 
    
    Spawns nproc subprocesses (__worker_func) to process arguments. 
    Each subprocess gets an argument from the job_q, processes it 
    and puts the result to the result_q.
    
    If the job_q is empty, terminate the subprocess.
    
    In case of any failure detected within the try except clause
    the argument, which just failed to process, the error and the
    hostname are put to the fail_q so the JobManager_Server can take care of that.
    
    After that the traceback is written to a file with name 
    traceback_args_<args>_err_<err>_<YYYY>_<MM>_<DD>_<hh>_<mm>_<ss>_<PID>.trb.
    
    Then the process will terminate.
    """
    
    def __init__(self, 
                  server, 
                  authkey, 
                  port = 42524, 
                  nproc = 0, 
                  nice=19, 
                  no_warnings=False, 
                  verbose=1,
                  show_statusbar_for_jobs=True,
                  show_counter_only=False,
                  interval=0.3):
        """
        server [string] - ip address or hostname where the JobManager_Server is running
        
        authkey [string] - authentication key used by the SyncManager. 
        Server and Client must have the same authkey.
        
        port [int] - network port to use
        
        nproc [integer] - number of subprocesses to start
            
            positive integer: number of processes to spawn
            
            zero: number of spawned processes == number cpu cores
            
            negative integer: number of spawned processes == number cpu cores - |nproc|
        
        nice [integer] - niceness of the subprocesses
        
        no_warnings [bool] - call warnings.filterwarnings("ignore") -> all warnings are ignored
        
        verbose [int] - 0: quiet, 1: status only, 2: debug messages
        """
        
        self.show_statusbar_for_jobs = show_statusbar_for_jobs
        self.show_counter_only = show_counter_only
        self.interval = interval
        self.verbose = verbose
        
        self._pid = os.getpid()
        self._identifier = progress.get_identifier(name=self.__class__.__name__, pid=self._pid) 
        if self.verbose > 1:
            print("{}: init".format(self._identifier))
       
        if no_warnings:
            import warnings
            warnings.filterwarnings("ignore")
            if self.verbose > 1:
                print("{}: ignore all warnings".format(self._identifier))
        self.server = server
        if isinstance(authkey, bytearray):
            self.authkey = authkey
        else: 
            self.authkey = bytearray(authkey, encoding='utf8')
        self.port = port
        self.nice = nice
        if nproc > 0:
            self.nproc = nproc
        else:
            self.nproc = mp.cpu_count() + nproc
            assert self.nproc > 0

        self.procs = []
        
        self.manager_objects = self.get_manager_objects()
        
        self.pbc = None
        
       
    def get_manager_objects(self):
        return JobManager_Client._get_manager_objects(self.server, 
                                                      self.port, 
                                                      self.authkey, 
                                                      self._identifier,
                                                      self.verbose)
       
    @staticmethod
    def _get_manager_objects(server, port, authkey, identifier, verbose=0):
        """
        connects to the server and get registered shared objects such as
        job_q, result_q, fail_q, const_arg
        """
        class ServerQueueManager(SyncManager):
            pass
        
        ServerQueueManager.register('get_job_q')
        ServerQueueManager.register('get_result_q')
        ServerQueueManager.register('get_fail_q')
        ServerQueueManager.register('get_const_arg', exposed="__iter__")
    
        manager = ServerQueueManager(address=(server, port), authkey=authkey)

            
        try:
            manager.connect()
        except:
            if verbose > 0:
                print("{}: connecting to {}:{} authkey '{}' FAILED!".format(identifier, server, port, authkey.decode('utf8')))    
                
                err, val, trb = sys.exc_info()
                print("caught exception {}: {}".format(err.__name__, val))
                
                if err == ConnectionRefusedError:
                    print("make sure the server is up!")
            
            if verbose > 1:
                traceback.print_exception(err, val, trb)
            return None
        else:
            if verbose > 1:    
                print("{}: connecting to {}:{} authkey '{}' SUCCEEDED!".format(identifier, server, port, authkey.decode('utf8')))
            
        
        job_q = manager.get_job_q()
        if verbose > 1:
            print("{}: found job_q with {} jobs".format(identifier, job_q.qsize()))
            
        result_q = manager.get_result_q()
        fail_q = manager.get_fail_q()
        const_arg = manager.get_const_arg()
        return job_q, result_q, fail_q, const_arg
        
    @staticmethod
    def func(arg, const_arg):
        """
        function to be called by the worker processes
        
        arg - provided by the job_q of the JobManager_Server
        
        const_arg - tuple of constant arguments also provided by the JobManager_Server
        
        to give status information to the Client class, use the variables
        (c, m) as additional parameters. c and m will be 
        multiprocessing.sharedctypes.Synchronized objects with an underlying
        unsigned int. so set c.value to the current status of the operation
        ans m.value to the final status. So at the end of the operation c.value should
        be m.value.
        
        NOTE: This is just some dummy implementation to be used for test reasons only!
        Subclass and overwrite this function to implement your own function.  
        """
        time.sleep(0.1)
        return os.getpid()
    
    @staticmethod
    def _handle_unexpected_queue_error(verbose, identifier):
        if verbose > 0:
                print("{}: unexpected Error, I guess the server went down, can't do anything, terminate now!".format(identifier))
        if verbose > 1:
            traceback.print_exc()


    @staticmethod
    def __worker_func(func, nice, verbose, server, port, authkey, i, manager_objects, c, m, reset_pbc):
        """
        the wrapper spawned nproc trimes calling and handling self.func
        """
        identifier = progress.get_identifier(name='worker{}'.format(i+1))
        Signal_to_sys_exit(signals=[signal.SIGTERM])
        Signal_to_SIG_IGN(signals=[signal.SIGINT])

        if manager_objects is None:        
            manager_objects = JobManager_Client._get_manager_objects(server, port, authkey, identifier, verbose)
            if manager_objects == None:
                if verbose > 1:
                    print("{}: no shared object recieved, terminate!".format(identifier))
                sys.exit(1)

        job_q, result_q, fail_q, const_arg = manager_objects 
        
        n = os.nice(0)
        n = os.nice(nice - n)

        if verbose > 1:
            print("{}: now alive, niceness {}".format(identifier, n))
        cnt = 0
        time_queue = 0.
        time_calc = 0.
        
        tg_1 = tg_0 = tp_1 = tp_0 = tf_1 = tf_0 = 0
        
        

        # check for func definition without status members count, max_count
        #args_of_func = inspect.getfullargspec(func).args
        #if len(args_of_func) == 2:
        count_args = getCountKwargs(func)
        if count_args is None:
            if verbose > 1:
                print("{}: found function without status information".format(identifier))
            m.value = 0  # setting max_count to -1 will hide the progress bar 
            _func = lambda arg, const_arg, c, m : func(arg, const_arg)
        elif count_args != ["c", "m"]:
            if verbose > 1:
                print("{}: found counter keyword arguments: {}".format(identifier, count_args))
            # Allow other arguments, such as ["jmc", "jmm"] as defined
            # in `validCountKwargs`.
            # Here we translate to "c" and "m".
            def _func(arg, const_arg, c, m):
                arg[count_args[0]] = c
                arg[count_args[1]] = m
        else:
            if verbose > 1:
                print("{}: found standard keyword arguments: [c, m]".format(identifier))
            _func = func
            
        
        
        # supposed to catch SystemExit, which will shout the client down quietly 
        try:
            
            # the main loop, exit loop when: 
            #    a) job_q is empty
            #    b) SystemExit is caught
            #    c) any queue operation (get, put) fails for what ever reason
            while True:

                # try to get an item from the job_q                
                try:
                    tg_0 = time.time()
                    arg = job_q.get(block = True, timeout = 0.1)
                    tg_1 = time.time()
                 
                # regular case, just stop working when empty job_q was found
                except queue.Empty:
                    if verbose > 1:
                        print("{}: finds empty job queue, processed {} jobs".format(identifier, cnt))
                    break
                # handle SystemExit in outer try ... except
                except SystemExit as e:
                    raise e
                # job_q.get failed -> server down?             
                except:
                    JobManager_Client._handle_unexpected_queue_error(verbose, identifier)
                    break
                
                # try to process the retrieved argument
                try:
                    tf_0 = time.time()
                    res = _func(arg, const_arg, c, m)
                    tf_1 = time.time()
                # handle SystemExit in outer try ... except
                except SystemExit as e:
                    raise e
                # something went wrong while doing the actual calculation
                # - write traceback to file
                # - try to inform the server of the failure
                except:
                    err, val, trb = sys.exc_info()
                    if verbose > 0:
                        print("{}: caught exception '{}'".format(identifier, err.__name__))
                    
                    if verbose > 1:
                        traceback.print_exc()
                
                    # write traceback to file
                    hostname = socket.gethostname()
                    fname = 'traceback_err_{}_{}.trb'.format(err.__name__, getDateForFileName(includePID=True))
                        
                    if verbose > 0:
                        print("        write exception to file {} ... ".format(fname), end='')
                        sys.stdout.flush()
                    with open(fname, 'w') as f:
                        traceback.print_exception(etype=err, value=val, tb=trb, file=f)
                    if verbose > 0:
                        print("done")
                        print("        continue processing next argument.")
                        
                    # try to inform the server of the failure
                    if verbose > 1:
                        print("{}: try to send send failed arg to fail_q ...".format(identifier), end='')
                        sys.stdout.flush()
                    try:
                        fail_q.put((arg, err.__name__, hostname), timeout=10)
                    # handle SystemExit in outer try ... except                        
                    except SystemExit as e:
                        if verbose > 1:
                            print(" FAILED!")
                        raise e
                    # fail_q.put failed -> server down?             
                    except:
                        if verbose > 1:
                            print(" FAILED!")
                        JobManager_Client._handle_unexpected_queue_error(verbose, identifier)
                        break
                    else:
                        if verbose > 1:
                            print(" done!")
                            
                # processing the retrieved arguments succeeded
                # - try to send the result back to the server                        
                else:
                    try:
                        tp_0 = time.time()
                        result_q.put((arg, res))
                        tp_1 = time.time()
                    # handle SystemExit in outer try ... except
                    except SystemExit as e:
                        raise e
                    # job_q.get failed -> server down?             
                    except:
                        JobManager_Client._handle_unexpected_queue_error(verbose, identifier)
                        break
                    
                cnt += 1
                
                time_queue += (tg_1-tg_0 + tp_1-tp_0)
                time_calc += (tf_1-tf_0)
                
                reset_pbc()
             
        # considered as normal exit caused by some user interaction, SIGINT, SIGTERM
        # note SIGINT, SIGTERM -> SystemExit is achieved by overwriting the
        # default signal handlers
        except SystemExit:
            if verbose > 0:
                print("{}: SystemExit, quit processing, reinsert current argument".format(identifier))

            if verbose > 1:
                print("{}: try to put arg back to job_q ...".format(identifier), end='')
                sys.stdout.flush()
            try:
                job_q.put(arg, timeout=10)
            # handle SystemExit in outer try ... except                        
            except SystemExit as e:
                if verbose > 1:
                    print(" FAILED!")
                raise e
            # fail_q.put failed -> server down?             
            except:
                if verbose > 1:
                    print(" FAILED!")
                JobManager_Client._handle_unexpected_queue_error(verbose, identifier)
            else:
                if verbose > 1:
                    print(" done!")
                
        if verbose > 0:
            try:
                print("{}: pure calculation time: {}".format(identifier, progress.humanize_time(time_calc) ))
                print("{}: calculation:{:.2%} communication:{:.2%}".format(identifier, time_calc/(time_calc+time_queue), time_queue/(time_calc+time_queue)))
            except:
                pass
        if verbose > 1:
            print("{}: JobManager_Client.__worker_func terminates".format(identifier))
        

    def start(self):
        """
        starts a number of nproc subprocess to work on the job_q
        
        SIGTERM and SIGINT are managed to terminate all subprocesses
        
        retruns when all subprocesses have terminated
        """
        if self.verbose > 1:
            print("{}: start {} processes to work on the remote queue".format(self._identifier, self.nproc))
            
        c = []
        for i in range(self.nproc):
            c.append(progress.UnsignedIntValue())
        
        m_progress = []
        for i in range(self.nproc):
            m_progress.append(progress.UnsignedIntValue(0))
            
        m_set_by_function = []
        for i in range(self.nproc):
            m_set_by_function.append(progress.UnsignedIntValue(0))
            
        if not self.show_counter_only:
            m_set_by_function = m_progress
            
        if (self.show_statusbar_for_jobs) and (self.verbose > 0):
            Progress = progress.ProgressBarCounter
        else:
            Progress = progress.ProgressSilentDummy
            
        with Progress(count=c, 
                      max_count=m_progress, 
                      interval=self.interval, 
                      verbose=self.verbose,
                      sigint='ign',
                      sigterm='ign') as self.pbc :
            self.pbc.start()
            for i in range(self.nproc):
                reset_pbc = lambda: self.pbc.reset(i)
                p = mp.Process(target=self.__worker_func, args=(self.func, 
                                                                self.nice, 
                                                                self.verbose, 
                                                                self.server, 
                                                                self.port,
                                                                self.authkey,
                                                                i,
                                                                self.manager_objects,
                                                                c[i],
                                                                m_set_by_function[i],
                                                                reset_pbc))
                self.procs.append(p)
                p.start()
                time.sleep(0.3)
            
            exit_handler = Signal_to_terminate_process_list(process_list=self.procs,
                                                            signals=[signal.SIGTERM], 
                                                            verbose=self.verbose,
                                                            name='worker',
                                                            timeout=2)
            
            interrupt_handler = Signal_handler_for_Jobmanager_client(client_object = self,
                                                                     exit_handler=exit_handler,
                                                                     signals=[signal.SIGINT],
                                                                     verbose=self.verbose)
            
            
        
            for p in self.procs:
                p.join()

class JobManager_Local(JobManager_Server):
    def __init__(self,
                  client_class,
                  authkey='local_jobmanager',
                  nproc=-1,
                  delay=1,
                  const_arg=None, 
                  port=42524, 
                  verbose=1,
                  verbose_client=0, 
                  show_statusbar_for_jobs=False,
                  show_counter_only=False,
                  niceness_clients=19,
                  msg_interval=1,
                  fname_dump='auto',
                  speed_calc_cycles=50):
        
        super(JobManager_Local, self).__init__(authkey=authkey,
                         const_arg=const_arg, 
                         port=port, 
                         verbose=verbose, 
                         msg_interval=msg_interval,
                         fname_dump=fname_dump,
                         speed_calc_cycles=speed_calc_cycles)
        
        self.client_class = client_class
        self.nproc = nproc
        self.delay = delay
        self.verbose_client=verbose_client
        self.show_statusbar_for_jobs = show_statusbar_for_jobs
        self.show_counter_only = show_counter_only
        self.niceness_clients = niceness_clients

    @staticmethod 
    def _start_client(authkey, 
                        client_class,
                        nproc=0, 
                        nice=19, 
                        delay=1, 
                        verbose=0, 
                        show_statusbar_for_jobs=False,
                        show_counter_only=False):        # ignore signal, because any signal bringing the server down
        # will cause an error in the client server communication
        # therefore the clients will also quit 
        Signal_to_SIG_IGN(verbose=verbose)
        time.sleep(delay)
        client = client_class(server='localhost',
                              authkey=authkey,
                              nproc=nproc, 
                              nice=nice,
                              verbose=verbose,
                              show_statusbar_for_jobs=show_statusbar_for_jobs,
                              show_counter_only=show_counter_only)
        
        client.start()
        
        
    def start(self):
        p_client = mp.Process(target=JobManager_Local._start_client,
                              args=(self.authkey, 
                                    self.client_class, 
                                    self.nproc,
                                    self.niceness_clients, 
                                    self.delay,
                                    self.verbose_client,
                                    self.show_statusbar_for_jobs,
                                    self.show_counter_only))
        p_client.start()
        super(JobManager_Local, self).start()
        
        progress.check_process_termination(p_client, 
                                           identifier='local_client',
                                           timeout=2,
                                           verbose=self.verbose_client)


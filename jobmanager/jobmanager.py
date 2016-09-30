#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from __future__ import division, print_function

import copy
import inspect
import multiprocessing as mp
from multiprocessing.managers import BaseManager, RemoteError
import subprocess
import os
import pickle
import signal
import socket
import sys
import time
import traceback
import warnings
import binfootprint as bf
import progression as progress
import logging
import threading
import ctypes

# taken from here: https://mail.python.org/pipermail/python-list/2010-November/591474.html
class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        _str = logging.Formatter.format(self, record)
        header = _str.split(record.message)[0]
        _str = _str.replace('\n', '\n' + ' '*len(header))
        return _str
   

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
console_hand = logging.StreamHandler(stream = sys.stderr)
console_hand.setLevel(logging.DEBUG)
fmt = MultiLineFormatter('%(asctime)s %(name)s %(levelname)s : %(message)s')
console_hand.setFormatter(fmt)
log.addHandler(console_hand)


from datetime import datetime

# This is a list of all python objects that will be imported upon
# initialization during module import (see __init__.py)
__all__ = ["JobManager_Client",
           "JobManager_Local",
           "JobManager_Server",
           "getDateForFileName"
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
    
    # IOError (socket.error) expection handling 
    import errno
    
    class JMConnectionError(Exception):
        pass
    
    class JMConnectionRefusedError(JMConnectionError):
        pass
    
    class JMConnectionResetError(JMConnectionError):
        pass
    
else:
    # Python 3
    import queue
    
    JMConnectionError = ConnectionError
    JMConnectionRefusedError = ConnectionRefusedError
    JMConnectionResetError = ConnectionResetError
    
class JMHostNotReachableError(JMConnectionError):
    pass

myQueue = mp.Queue
AuthenticationError = mp.AuthenticationError

def humanize_size(size_in_bytes):
    """convert a speed in counts per second to counts per [s, min, h, d], choosing the smallest value greater zero.
    """
    thr = 99
    scales = [1024, 1024, 1024]
    units = ['k', 'M', 'G', 'T']
    i = 0
    while (size_in_bytes > thr) and (i < len(scales)):
        size_in_bytes = size_in_bytes / scales[i]
        i += 1
    return "{:.2f}{}B".format(size_in_bytes, units[i]) 

def get_user():
    out = subprocess.check_output('id -un', shell=True).decode().strip()
    return out
    
def get_user_process_limit():
    out = subprocess.check_output('ulimit -u', shell=True).decode().strip()
    return int(out)
    
def get_user_num_process():
    out = subprocess.check_output('ps ut | wc -l', shell=True).decode().strip()
    return int(out)-2

class JobManager_Client(object):
    """
    Calls the functions self.func with arguments fetched from the job_q.
    You should subclass this class and overwrite func to handle your own
    function.
    
    The job_q is provided by the SyncManager who connects to a 
    SyncManager setup by the JobManager_Server. 
    
    Spawns nproc subprocesses (__worker_func) to process arguments. 
    Each subprocess gets an argument from the job_q, processes it 
    and puts the result to the result_q.
    
    If the job_q is empty, terminate the subprocess.
    
    In case of any failure detected within the try except clause
    the argument, which just failed to process, the error and the
    hostname are put to the fail_q so the JobManager_Server can take
    care of that.
    
    After that the traceback is written to a file with name 
    traceback_args_<args>_err_<err>_<YYYY>_<MM>_<DD>_<hh>_<mm>_<ss>_<PID>.trb.
    
    Then the process will terminate.
    """
    
    def __init__(self, 
                 server, 
                 authkey, 
                 port                    = 42524, 
                 nproc                   = 0,
                 njobs                   = 0,
                 nice                    = 19, 
                 no_warnings             = False, 
                 verbose                 = None,
                 show_statusbar_for_jobs = True,
                 show_counter_only       = False,
                 interval                = 0.3,
                 emergency_dump_path     = '.',
                 job_q_get_timeout       = 1,
                 job_q_put_timeout       = 10,
                 result_q_put_timeout   = 300,
                 fail_q_put_timeout      = 10,
                 reconnect_wait          = 2,
                 reconnect_tries         = 3,
                 ping_timeout            = 2,
                 ping_retry              = 3,
                 hide_progress           = False):
        """
        server [string] - ip address or hostname where the JobManager_Server is running
        
        authkey [string] - authentication key used by the SyncManager. 
        Server and Client must have the same authkey.
        
        port [int] - network port to use
        
        nproc [integer] - number of subprocesses to start
            
            positive integer: number of processes to spawn
            
            zero: number of spawned processes == number cpu cores
            
            negative integer: number of spawned processes == number cpu cores - |nproc|
        
        njobs [integer] - total number of jobs to run per process
        
            negative integer or zero: run until there are no more jobs
            
            positive integer: run only njobs number of jobs per nproc
                              The total number of jobs this client will
                              run is njobs*nproc.
        
        nice [integer] - niceness of the subprocesses
        
        no_warnings [bool] - call warnings.filterwarnings("ignore") -> all warnings are ignored
        
        verbose [int] - 0: quiet, 1: status only, 2: debug messages
        
        DO NOT SIGTERM CLIENT TOO ERLY, MAKE SURE THAT ALL SIGNAL HANDLERS ARE UP (see log at debug level)
        """

        global log
        log = logging.getLogger(__name__+'.'+self.__class__.__name__)

        self._pid = os.getpid()
        self._sid = os.getsid(self._pid)

        if verbose is not None:
            log.warning("verbose is deprecated, only allowed for compatibility")
            warnings.warn("verbose is deprecated", DeprecationWarning)

        self.hide_progress = hide_progress
               
        log.info("init JobManager Client instance (pid %s)", os.getpid())
        
        self.show_statusbar_for_jobs = show_statusbar_for_jobs
        log.debug("show_statusbar_for_jobs:%s", self.show_statusbar_for_jobs)
        self.show_counter_only = show_counter_only
        log.debug("show_counter_only:%s", self.show_counter_only)
        self.interval = interval
        log.debug("interval:%s", self.interval)

        self._job_q_get_timeout = job_q_get_timeout
        log.debug("_job_q_get_timeout:%s", self._job_q_get_timeout)
        self._job_q_put_timeout = job_q_put_timeout
        log.debug("_job_q_put_timeout:%s", self._job_q_put_timeout)
        self._result_q_put_timeout = result_q_put_timeout
        log.debug("_result_q_put_timeout:%s", self._result_q_put_timeout)
        self._fail_q_put_timeout = fail_q_put_timeout
        log.debug("_fail_q_put_timeout:%s", self._fail_q_put_timeout)
        self.reconnect_wait = reconnect_wait
        log.debug("reconnect_wait:%s", self.reconnect_wait)
        self.reconnect_tries = reconnect_tries
        log.debug("reconnect_tries:%s", self.reconnect_tries)
        self.ping_timeout = ping_timeout
        log.debug("ping_timeout:%s", self.ping_timeout)
        self.ping_retry = ping_retry
        log.debug("ping_retry:%s", self.ping_retry)
              
        if no_warnings:
            warnings.filterwarnings("ignore")
            log.info("ignore all warnings")

        self.server = server
        log.debug("server:%s", self.server)
        if isinstance(authkey, bytearray):
            self.authkey = authkey
        else: 
            self.authkey = bytearray(authkey, encoding='utf8')
        log.debug("authkey:%s", self.authkey)
        self.port = port
        log.debug("port:%s", self.port)
        self.nice = nice
        log.debug("nice:%s", self.nice)
        if nproc > 0:
            self.nproc = nproc
        else:
            self.nproc = mp.cpu_count() + nproc
            if self.nproc <= 0:
                raise RuntimeError("Invalid Number of Processes\ncan not spawn {} processes (cores found: {}, cores NOT to use: {} = -nproc)".format(self.nproc, mp.cpu_count(), abs(nproc)))
        log.debug("nproc:%s", self.nproc)
        if njobs == 0:        # internally, njobs must be negative for infinite jobs
            njobs -= 1
        self.njobs = njobs
        log.debug("njobs:%s", self.njobs)
        self.emergency_dump_path = emergency_dump_path
        log.debug("emergency_dump_path:%s", self.emergency_dump_path)

        self.pbc = None
        
        self.procs = []        
        self.manager_objects = None  # will be set via connect()
        self.connect()               # get shared objects from server
        
    def connect(self):
        if self.manager_objects is None:
            try:
                self.manager_objects = self.create_manager_objects()
            except Exception as e:
                log.critical("creating manager objects failed due to {}".format(type(e)))
                log.info(traceback.format_exc())
                raise
                
        else:
            log.info("already connected (at least shared object are available)")

    @property
    def connected(self):
        return self.manager_objects is not None
    
    def _dump_result_to_local_storage(self, res):
        pass
       
    def create_manager_objects(self):
        """
            connects to the server and get registered shared objects such as
            job_q, result_q, fail_q
            
            const_arg will be deep copied from the manager and therefore live
            as non shared object in local memory
        """
        class ServerQueueManager(BaseManager):
            pass
        
        ServerQueueManager.register('get_job_q')
        ServerQueueManager.register('get_result_q')
        ServerQueueManager.register('get_fail_q')
        ServerQueueManager.register('get_const_arg')
    
        manager = ServerQueueManager(address=(self.server, self.port), authkey=self.authkey)

        try:
            call_connect(connect         = manager.connect,
                         dest            = address_authkey_from_manager(manager),
                         reconnect_wait  = self.reconnect_wait, 
                         reconnect_tries = self.reconnect_tries)
        except:
            log.warning("FAILED to connect to %s", address_authkey_from_manager(manager))
            log.info(traceback.format_exc())
            return None
            
        job_q = manager.get_job_q()
        log.info("found job_q with %s jobs", job_q.qsize())
        
        result_q = manager.get_result_q()
        fail_q = manager.get_fail_q()
        # deep copy const_arg from manager -> non shared object in local memory
        const_arg = copy.deepcopy(manager.get_const_arg())
            
        return job_q, result_q, fail_q, const_arg, manager
    
        
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
        pid = os.getpid()
        #print("{} sleeps for {}s".format(pid, const_arg))
        time.sleep(const_arg)
        return pid
    
    @staticmethod
    def __worker_func(func,
                      nice,
                      loglevel,
                      i,
                      job_q_get,
                      local_job_q,
                      local_result_q,
                      local_fail_q,
                      const_arg,
                      c,
                      m,
                      reset_pbc,
                      njobs,
                      emergency_dump_path,
                      job_q_get_timeout):
        """
        the wrapper spawned nproc times calling and handling self.func
        """
        global log
        log = logging.getLogger(__name__+'.'+progress.get_identifier(name='worker{}'.format(i+1), bold=False))
        log.setLevel(loglevel)

        Signal_to_sys_exit(signals=[signal.SIGTERM])
        Signal_to_SIG_IGN(signals=[signal.SIGINT])

        n = os.nice(0)
        try:
            n = os.nice(nice - n)
        except PermissionError:
            log.warning("changing niceness not permitted! run with niceness %s", n)

        log.debug("worker function now alive, niceness %s", n)
        cnt = 0
        time_queue = 0.
        time_calc = 0.

        # check for func definition without status members count, max_count
        #args_of_func = inspect.getfullargspec(func).args
        #if len(args_of_func) == 2:
        count_args = progress.getCountKwargs(func)

        if count_args is None:
            log.warning("found function without status information (progress will not work)")
            m.value = 0  # setting max_count to -1 will hide the progress bar 
            _func = lambda arg, const_arg, c, m : func(arg, const_arg)
        elif count_args != ["c", "m"]:
            log.debug("found counter keyword arguments: %s", count_args)
            # Allow other arguments, such as ["jmc", "jmm"] as defined
            # in `validCountKwargs`.
            # Here we translate to "c" and "m".
            def _func(arg, const_arg, c, m):
                kwargs = {count_args[0]: c,
                          count_args[1]: m}
                return func(arg, const_arg, **kwargs)
        else:
            log.debug("found standard keyword arguments: [c, m]")
            _func = func
            

               
        # supposed to catch SystemExit, which will shut the client down quietly 
        try:
            
            # the main loop, exit loop when: 
            #    a) job_q is empty
            #    b) SystemExit is caught
            #    c) any queue operation (get, put) fails for what ever reason
            #    d) njobs becomes zero
            while njobs != 0:
                njobs -= 1

                # try to get an item from the job_q                
                try:
                    tg_0 = time.time()
                    arg = job_q_get(block = True, timeout = job_q_get_timeout)
                    tg_1 = time.time()
                    time_queue += (tg_1-tg_0)
                 
                # regular case, just stop working when empty job_q was found
                except queue.Empty:
                    log.info("finds empty job queue, processed %s jobs", cnt)
                    break
                # handle SystemExit in outer try ... except
                except SystemExit as e:
                    log.warning('getting arg from job_q failed due to SystemExit')
                    raise e
                # job_q.get failed -> server down?             
                except Exception as e:
                    log.error("Error when calling 'job_q_get'") 
                    handle_unexpected_queue_error(e)
                    break
                
                # try to process the retrieved argument
                try:
                    tf_0 = time.time()
                    log.debug("START crunching _func")
                    res = _func(arg, const_arg, c, m)
                    log.debug("DONE crunching _func")
                    tf_1 = time.time()
                    time_calc += (tf_1-tf_0)
                # handle SystemExit in outer try ... except
                except SystemExit as e:
                    raise e
                # something went wrong while doing the actual calculation
                # - write traceback to file
                # - try to inform the server of the failure
                except:
                    err, val, trb = sys.exc_info()
                    log.error("caught exception '%s' when crunching 'func'\n%s", err.__name__, traceback.print_exc())
                
                    # write traceback to file
                    hostname = socket.gethostname()
                    fname = 'traceback_err_{}_{}.trb'.format(err.__name__, getDateForFileName(includePID=True))
                    
                    log.info("write exception to file %s", fname)
                    with open(fname, 'w') as f:
                        traceback.print_exception(etype=err, value=val, tb=trb, file=f)

                    log.debug("try to send send failed arg to fail_q")
                    try:
                        local_fail_q.put((arg, err.__name__, hostname))
                    # handle SystemExit in outer try ... except                        
                    except SystemExit as e:
                        log.warning('sending arg to fail_q failed due to SystemExit')
                        raise e
                    # fail_q.put failed -> server down?             
                    except Exception as e:
                        log.error('sending arg to fail_q failed')
                        handle_unexpected_queue_error(e)
                        break
                    else:
                        log.debug('sending arg to fail_q was successful')
                            
                # processing the retrieved arguments succeeded
                # - try to send the result back to the server                        
                else:
                    try:
                        tp_0 = time.time()
                        local_result_q.put((arg, res))
                        tp_1 = time.time()
                        time_queue += (tp_1-tp_0)
                        
                    # handle SystemExit in outer try ... except
                    except SystemExit as e:
                        log.warning('sending result to result_q failed due to SystemExit')
                        raise e
                    
                    except Exception as e:
                        log.error('sending result to result_q failed due to %s', type(e))
                        emergency_dump(arg, res, emergency_dump_path)
                        handle_unexpected_queue_error(e)
                        break
                    
                    del res
                    
                cnt += 1
                reset_pbc()
                log.debug("continue with next arg")
             
        # considered as normal exit caused by some user interaction, SIGINT, SIGTERM
        # note SIGINT, SIGTERM -> SystemExit is achieved by overwriting the
        # default signal handlers
        except SystemExit:
            log.warning("SystemExit, quit processing, reinsert current argument, please wait")
            log.debug("try to put arg back to job_q")
            try:
                local_job_q.put(arg)
            # handle SystemExit in outer try ... except                        
            except SystemExit as e:
                log.error("put arg back to job_q failed due to SystemExit")
                raise e
            # fail_q.put failed -> server down?             
            except Exception as e:
                log.error("put arg back to job_q failed due to %s", type(e))
                handle_unexpected_queue_error(e)
            else:
                log.debug("putting arg back to job_q was successful")

        try:                
            sta = progress.humanize_time(time_calc / cnt)
        except:
            sta = 'invalid'

        stat = "pure calculation time: {} single task average: {}".format(progress.humanize_time(time_calc), sta)
        try:
            stat += "\ncalculation:{:.2%} communication:{:.2%}".format(time_calc/(time_calc+time_queue), time_queue/(time_calc+time_queue))
        except ZeroDivisionError:
            pass
            
        log.info(stat)

        log.debug("JobManager_Client.__worker_func at end (PID %s)", os.getpid())
        
    def start(self):
        """
        starts a number of nproc subprocess to work on the job_q
        
        SIGTERM and SIGINT are managed to terminate all subprocesses
        
        retruns when all subprocesses have terminated
        """
        
        if not self.connected:
            raise JMConnectionError("Can not start Client with no connection to server (shared objetcs are not available)")

        
        log.info("STARTING CLIENT\nserver:%s authkey:%s port:%s num proc:%s", self.server, self.authkey.decode(), self.port, self.nproc)
            
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
                      
        prepend = []
        infoline = progress.StringValue(num_of_bytes=12)
        infoline = None

        # try:
        #     worker_stdout_queue = mp.Queue(-1)
        #     listener = QueueListener(worker_stdout_queue, console_hand)
        #     listener.start()
        # except NameError:
        #     log.error("QueueListener not available in this python version (need at least 3.2)\n"
        #               "this may resault in incoheerent logging")
        #     worker_stdout_queue = None

        # worker_stdout_queue = None

        l = len(str(self.nproc))
        for i in range(self.nproc):
            prepend.append("w{0:0{1}}:".format(i+1, l))

        job_q, result_q, fail_q, const_arg, manager = self.manager_objects

        local_job_q = mp.Queue()
        local_result_q = mp.Queue()
        local_fail_q = mp.Queue()

        kwargs = {'reconnect_wait': self.reconnect_wait,
                  'reconnect_tries': self.reconnect_tries,
                  'ping_timeout': self.ping_timeout,
                  'ping_retry': self.ping_retry}

        job_q_get = proxy_operation_decorator(proxy=job_q, operation='get', **kwargs)
        job_q_put = proxy_operation_decorator(proxy=job_q, operation='put', **kwargs)
        result_q_put = proxy_operation_decorator(proxy=result_q, operation='put', **kwargs)
        fail_q_put = proxy_operation_decorator(proxy=fail_q, operation='put', **kwargs)
        
        def pass_job_q_put(job_q_put, local_job_q, timeout):
#             log.debug("this is thread thr_job_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))
            while True:
                data = local_job_q.get()
                job_q_put(data, timeout=timeout)
#             log.debug("stopped thread thr_job_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))

 
        def pass_result_q_put(result_q_put, local_result_q, timeout):
            log.debug("this is thread thr_result_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))
            try:
                while True:
                    data = local_result_q.get()
                    result_q_put(data, timeout=timeout)
            except Exception as e:
                log.error("thr_result_q_put caught error %s", type(e))
                log.info(traceback.format_exc())
            log.debug("stopped thread thr_result_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))
 
        def pass_fail_q_put(fail_q_put, local_fail_q, timeout):
#             log.debug("this is thread thr_fail_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))
            while True:
                data = local_fail_q.get()
                fail_q_put(data, timeout=timeout)  
#             log.debug("stopped thread thr_fail_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))

        thr_job_q_put           = threading.Thread(target=pass_job_q_put   , args=(job_q_put   , local_job_q   , self._job_q_put_timeout))
        thr_job_q_put.daemon    = True
        thr_result_q_put        = threading.Thread(target=pass_result_q_put, args=(result_q_put, local_result_q, self._result_q_put_timeout))
        thr_result_q_put.daemon = True
        thr_fail_q_put          = threading.Thread(target=pass_fail_q_put  , args=(fail_q_put  , local_fail_q  , self._fail_q_put_timeout))
        thr_fail_q_put.daemon   = True
        
        

        thr_job_q_put.start()       
        thr_result_q_put.start()
        thr_fail_q_put.start()
        
        with progress.ProgressBarCounterFancy(count         = c, 
                                              max_count     = m_progress, 
                                              interval      = self.interval,
                                              prepend       = prepend,
                                              sigint        = 'ign',
                                              sigterm       = 'ign',
                                              info_line     = infoline) as self.pbc :
            
            if (not self.hide_progress) and self.show_statusbar_for_jobs:
                self.pbc.start()

            for i in range(self.nproc):
                reset_pbc = lambda: self.pbc.reset(i)
                p = mp.Process(target=self.__worker_func, args=(self.func,                # func
                                                                self.nice,                # nice
                                                                log.level,                # loglevel
                                                                i,                        # i
                                                                job_q_get,                # job_q_get
                                                                local_job_q,              # local_job_q
                                                                local_result_q,           # local_result_q
                                                                local_fail_q,             # local_fail_q
                                                                const_arg,                # const_arg
                                                                c[i],                     # c
                                                                m_set_by_function[i],     # m
                                                                reset_pbc,                # reset_pbc
                                                                self.njobs,               # njobs
                                                                self.emergency_dump_path, # emergency_dump_path
                                                                self._job_q_get_timeout)) # job_q_get_timeout



                self.procs.append(p)
                p.start()
                log.debug("started new worker with pid %s", p.pid)
                time.sleep(0.1)

            log.debug("all worker processes startes")

            #time.sleep(self.interval/2)
            log.debug("setup Signal_to_terminate_process_list handler")
            exit_handler = Signal_to_terminate_process_list(process_list    = self.procs,
                                                            identifier_list = [progress.get_identifier(name = "worker{}".format(i+1),
                                                                                                       pid  = p.pid,
                                                                                                       bold = True) for i, p in enumerate(self.procs)],
                                                            signals         = [signal.SIGTERM],                                                            
                                                            timeout         = 2)

            log.debug("setup Signal_handler_for_Jobmanager_client handler")
            Signal_handler_for_Jobmanager_client(client_object = self,
                                                 exit_handler=exit_handler,
                                                 signals=[signal.SIGINT])
        
            for p in self.procs:

                p.join()
                log.debug("worker process %s exitcode %s", p.pid, p.exitcode)
                log.debug("worker process %s was joined", p.pid)

            log.debug("all workers joind")
            log.debug("still in progressBar context")

        log.debug("progressBar context has been left")
        
        while (not local_job_q.empty()):
            log.debug("still data in local_job_q (%s)", local_job_q.qsize())
            if thr_job_q_put.is_alive():
                log.debug("allow the thread thr_job_q_put to process items")
                time.sleep(1)
            else:
                log.warning("the thread thr_job_q_put has died, can not process remaining items")
                break

        while (not local_result_q.empty()):
            log.debug("still data in local_result_q (%s)", local_result_q.qsize())
            if thr_result_q_put.is_alive():
                log.debug("allow the thread thr_result_q_put to process items")
                time.sleep(1)
            else:
                log.warning("the thread thr_result_q_put has died, can not process remaining items")
                break

        while (not local_fail_q.empty()):
            log.debug("still data in local_fail_q (%s)", local_fail_q.qsize())
            if thr_fail_q_put.is_alive():
                log.debug("allow the thread thr_fail_q_put to process items")
                time.sleep(1)
            else:
                log.warning("the thread thr_fail_q_put has died, can not process remaining items")
                break
            
        log.info("client stopped")                    

# 
# 
#         log.debug("wait for local_result_q to empty or thread to stop...")
#         while (not local_result_q.empty()) and thr_result_q_put.is_alive():
#             time.sleep(1)
#         log.debug("join thread (local_result_q empty:%s)", local_result_q.empty())
#         thr_result_q_put.join(0)
# 
# 
#         log.debug("wait for local_fail_q to empty or thread to stop...")
#         while (not local_fail_q.empty()) and thr_fail_q_put.is_alive():
#             time.sleep(1)
#         log.debug("join thread (local_fail_q empty:%s)", local_fail_q.empty())
#         thr_fail_q_put.join(0)





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
    rather methods to shut down the Server gracefully. Therefore in such cases no
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
                 const_arg                 = None,
                 port                      = 42524,
                 verbose                   = None,
                 msg_interval              = 1,
                 fname_dump                = 'auto',
                 speed_calc_cycles         = 50,
                 keep_new_result_in_memory = False,
                 hide_progress             = False):
        """
        authkey [string] - authentication key used by the SyncManager. 
        Server and Client must have the same authkey.
        
        const_arg [dict] - some constant keyword arguments additionally passed to the
        worker function (see JobManager_Client).
        
        port [int] - network port to use
        
        verbose deprecates, use log.setLevel to change verbosity
        
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
        global log
        log = logging.getLogger(__name__+'.'+self.__class__.__name__)

        self._pid = os.getpid()
        self._pid_start = None

        if verbose is not None:
            log.warning("verbose is deprecated, only allowed for compatibility")
            warnings.warn("verbose is deprecated", DeprecationWarning)

        self.hide_progress = hide_progress
        
        log.debug("I'm the JobManager_Server main process (pid %s)", os.getpid())
        
        self.__wait_before_stop = 2
        self.port = port

        if isinstance(authkey, bytearray):
            self.authkey = authkey
        else: 
            self.authkey = bytearray(authkey, encoding='utf8')
            
      
        self.const_arg = const_arg
                
        self.fname_dump = fname_dump        
        self.msg_interval = msg_interval
        self.speed_calc_cycles = speed_calc_cycles
        self.keep_new_result_in_memory = keep_new_result_in_memory

        # to do some redundant checking, might be removed
        # the args_dict holds all arguments to be processed
        # in contrast to the job_q, an argument will only be removed
        # from the set if it was caught by the result_q
        # so iff all results have been processed successfully,
        # the args_dict will be empty
        self.args_dict = dict()   # has the bin footprint in it
        self.args_list = []       # has the actual args object
        
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
        self.hostname = socket.gethostname()
        
    def __stop_SyncManager(self):
        if self.manager == None:
            return
        
        manager_proc = self.manager._process        
        # stop SyncManager
        self.manager.shutdown()
        
        progress.check_process_termination(proc                     = manager_proc, 
                                           prefix                   = 'SyncManager: ', 
                                           timeout                  = 2,
                                           auto_kill_on_last_resort = True)

    def __check_bind(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((self.hostname, self.port))
        except:
            log.critical("test bind to %s:%s failed", self.hostname, self.port)
            raise
        finally:
            s.close()
                
    def __start_SyncManager(self):
        self.__check_bind()
        
        class JobQueueManager(BaseManager):
            pass

        # make job_q, result_q, fail_q, const_arg available via network
        JobQueueManager.register('get_job_q', callable=lambda: self.job_q)
        JobQueueManager.register('get_result_q', callable=lambda: self.result_q)
        JobQueueManager.register('get_fail_q', callable=lambda: self.fail_q)
        JobQueueManager.register('get_const_arg', callable=lambda: self.const_arg)
    
        address=('', self.port)   #ip='' means local
        authkey=self.authkey
    
        self.manager = JobQueueManager(address, authkey)
            
        # start manager with non default signal handling given by
        # the additional init function setup_SIG_handler_manager

        try:
            self.manager.start(setup_SIG_handler_manager)
        except EOFError as e:
            log.error("can not start SyncManager on %s:%s\n"+
                      "this is usually the case when the port used is not available!", self.hostname, self.port)
            
            manager_proc = self.manager._process
            manager_identifier = progress.get_identifier(name='SyncManager')
            progress.check_process_termination(proc                     = manager_proc, 
                                               prefix                   = manager_identifier, 
                                               timeout                  = 0.3,
                                               auto_kill_on_last_resort = True)
            
            self.manager = None
            return False
        
        log.info("SyncManager with PID %s started on %s:%s (%s)", self.manager._process.pid, self.hostname, self.port, authkey)
        return True
    
    def __restart_SyncManager(self):
        self.__stop_SyncManager()
        if not self.__start_SyncManager():
            log.critical("faild to restart SyncManager")
            raise RuntimeError("could not start server")
        
    def __enter__(self):
        return self
        
    def __exit__(self, err, val, trb):
        # KeyboardInterrupt via SIGINT will be mapped to SystemExit  
        # SystemExit is considered non erroneous
        if (err is None) or (err == SystemExit):
            log.debug("normal shutdown")
            # bring everything down, dump status to file 
            self.shutdown()
            # no exception traceback will be printed
            return True
        else:
            log.debug("shutdown due to exception '%s'", err.__name__)
            # bring everything down, dump status to file 
            self.shutdown()
            # causes exception traceback to be printed
            return False
             
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

    def shutdown(self):
        """"stop all spawned processes and clean up
        
        - call process_final_result to handle all collected result
        - if job_q is not empty dump remaining job_q
        """
        # will only be False when _shutdown was started in subprocess
        
        self.__stop_SyncManager()
        log.debug("SyncManager stopped!")        
        
        # do user defined final processing
        self.process_final_result()
        log.debug("process_final_result done!")
        
        # print(self.fname_dump)
        if self.fname_dump is not None:
            if self.fname_dump == 'auto':
                fname = "{}_{}.dump".format(self.authkey.decode('utf8'), getDateForFileName(includePID=False))
            else:
                fname = self.fname_dump
            
            log.info("dump current state to '%s'", fname)    
            with open(fname, 'wb') as f:
                self.__dump(f)

            log.debug("dump state done!")

        else:
            log.info("fname_dump == None, ignore dumping current state!")
        
        # start also makes sure that it was not started as subprocess
        # so at default behavior this assertion will allays be True
        assert self._pid == os.getpid()
        
        self.show_statistics()
        

        log.info("JobManager_Server was successfully shut down")
        
    def show_statistics(self):
        all_jobs = self.numjobs
        succeeded = self.numresults
        failed = self.fail_q.qsize()
        all_processed = succeeded + failed
        
        id1 = self.__class__.__name__+" "
        l = len(id1)
        id2 = ' '*l + "| " 
        
        print("{}total number of jobs  : {}".format(id1, all_jobs))
        print("{}  processed   : {}".format(id2, all_processed))
        print("{}    succeeded : {}".format(id2, succeeded))
        print("{}    failed    : {}".format(id2, failed))
        
        all_not_processed = all_jobs - all_processed
        not_queried = self.job_q.qsize()
        queried_but_not_processed = all_not_processed - not_queried  
        
        print("{}  not processed     : {}".format(id2, all_not_processed))
        print("{}    queried         : {}".format(id2, queried_but_not_processed))
        print("{}    not queried yet : {}".format(id2, not_queried))
        print("{}len(args_dict) : {}".format(id2, len(self.args_dict)))
        if (all_not_processed + failed) != len(self.args_dict):
            log.error("'all_not_processed != len(self.args_dict)' something is inconsistent!")
            
    def all_successfully_processed(self):
        return self.numjobs == self.numresults

    @staticmethod
    def static_load(f):
        data = {}
        data['numjobs'] = pickle.load(f)
        data['numresults'] = pickle.load(f)        
        data['final_result'] = pickle.load(f)        
        data['args_dict'] = pickle.load(f)
        data['args_list'] = pickle.load(f)
        
        fail_list = pickle.load(f)
        data['fail_set'] = {bf.dump(fail_item[0]) for fail_item in fail_list}
        
        data['fail_q'] = myQueue()
        data['job_q'] = myQueue()
        
        for fail_item in fail_list:
            data['fail_q'].put_nowait(fail_item)
        for arg in data['args_dict']:
            if arg not in data['fail_set']:
                arg_idx = data['args_dict'][arg] 
                data['job_q'].put_nowait(data['args_list'][arg_idx])

        return data

    def __load(self, f):
        data = JobManager_Server.static_load(f)
        for key in ['numjobs', 'numresults', 'final_result',
                    'args_dict', 'args_list', 'fail_q','job_q']:
            self.__setattr__(key, data[key])
        
    def __dump(self, f):
        pickle.dump(self.numjobs, f, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(self.numresults, f, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(self.final_result, f, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(self.args_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(self.args_list, f, protocol=pickle.HIGHEST_PROTOCOL)
        fail_list = []
        try:
            while True:
                fail_list.append(self.fail_q.get_nowait())
        except queue.Empty:
            pass
        pickle.dump(fail_list, f, protocol=pickle.HIGHEST_PROTOCOL)

        
    def read_old_state(self, fname_dump=None):
        
        if fname_dump == None:
            fname_dump = self.fname_dump
        if fname_dump == 'auto':
            log.critical("fname_dump must not be 'auto' when reading old state")
            raise RuntimeError("fname_dump must not be 'auto' when reading old state")
        
        if not os.path.isfile(fname_dump):
            log.critical("file '%s' to read old state from not found", fname_dump)
            raise RuntimeError("file '{}' to read old state from not found".format(fname_dump))

        log.info("load state from file '%s'", fname_dump)
        
        with open(fname_dump, 'rb') as f:
            self.__load(f)
        
        self.show_statistics()
            
    def put_arg(self, a):
        """add argument a to the job_q
        """
        bfa = bf.dump(a)
        if bfa in self.args_dict:
            log.critical("do not add the same argument twice! If you are sure, they are not the same, there might be an error with the binfootprint mehtods!")
            raise ValueError("do not add the same argument twice! If you are sure, they are not the same, there might be an error with the binfootprint mehtods!")
        
        # this dict associates an unique index with each argument 'a'
        # or better with its binary footprint
        self.args_dict[bfa] = len(self.args_list)
        
        self.args_list.append(a)
        # the actual shared queue
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

    def print_jm_ready(self):
        # please overwrite for individual hooks to notify that the server process runs
        print("jobmanager awaits client results")

    def start(self):
        """
        starts to loop over incoming results
        
        When finished, or on exception call stop() afterwards to shut down gracefully.
        """
        
        if not self.__start_SyncManager():
            log.critical("could not start server")
            raise RuntimeError("could not start server")
        
        if self._pid != os.getpid():
            log.critical("do not run JobManager_Server.start() in a subprocess")
            raise RuntimeError("do not run JobManager_Server.start() in a subprocess")

        if (self.numjobs - self.numresults) != len(self.args_dict):
            log.debug("numjobs:             %s\n"+
                      "numresults:          %s\n"+
                      "len(self.args_dict): %s", self.numjobs, self.numresults, len(self.args_dict))
                
            log.critical("inconsistency detected! (self.numjobs - self.numresults) != len(self.args_dict)! use JobManager_Server.put_arg to put arguments to the job_q")
            raise RuntimeError("inconsistency detected! (self.numjobs - self.numresults) != len(self.args_dict)! use JobManager_Server.put_arg to put arguments to the job_q")
        
        if self.numjobs == 0:
            log.warning("no jobs to process! use JobManager_Server.put_arg to put arguments to the job_q")
            return
        else:
            log.info("started (host:%s authkey:%s port:%s jobs:%s)", self.hostname, self.authkey.decode(), self.port, self.numjobs)
        
        Signal_to_sys_exit(signals=[signal.SIGTERM, signal.SIGINT])
        
        log.debug("start processing incoming results")
        info_line = progress.StringValue(num_of_bytes=100)
        self.print_jm_ready()
        with progress.ProgressBarFancy(count             = self._numresults,
                                       max_count         = self._numjobs, 
                                       interval          = self.msg_interval,
                                       speed_calc_cycles = self.speed_calc_cycles,                                       
                                       sigint            = 'ign',
                                       sigterm           = 'ign',
                                       info_line         = info_line) as stat:
            if not self.hide_progress:
                stat.start()
        
            while (len(self.args_dict) - self.fail_q.qsize()) > 0:
                info_line.value = "result_q size:{}, job_q size:{}, recieved results:{}".format(self.result_q.qsize(), 
                                                                                                self.job_q.qsize(), 
                                                                                                self.numresults).encode('utf-8')
        
                # allows for update of the info line
                try:
                    arg, result = self.result_q.get(timeout = self.msg_interval)
                except queue.Empty:
                    continue
                
                bf_arg = bf.dump(arg)
                if bf_arg not in self.args_dict:
                    log.warning("got an argument that is not listed in the args_dict (probably crunshed twice, uups) -> will be skipped")
                    del arg
                    del result
                    continue
                
                del self.args_dict[bf_arg]
                self.numresults += 1
                self.process_new_result(arg, result)
                if not self.keep_new_result_in_memory:
                    del arg
                    del result
        
        log.debug("wait %ss before trigger clean up", self.__wait_before_stop)
        time.sleep(self.__wait_before_stop)
        

class JobManager_Local(JobManager_Server):
    def __init__(self,
                 client_class,
                 authkey                 = 'local_jobmanager',
                 nproc                   = -1,
                 delay                   = 1,
                 const_arg               = None, 
                 port                    = 42524, 
                 verbose                 = None,
                 verbose_client          = None,
                 show_statusbar_for_jobs = False,
                 show_counter_only       = False,
                 niceness_clients        = 19,
                 msg_interval            = 1,
                 fname_dump              = 'auto',
                 speed_calc_cycles       = 50):
        
        super(JobManager_Local, self).__init__(authkey           = authkey,
                                               const_arg         = const_arg, 
                                               port              = port, 
                                               verbose           = verbose, 
                                               msg_interval      = msg_interval,
                                               fname_dump        = fname_dump,
                                               speed_calc_cycles = speed_calc_cycles)
        
        self.client_class = client_class
        self.port = port
        self.nproc = nproc
        self.delay = delay
        self.verbose_client=verbose_client
        self.show_statusbar_for_jobs = show_statusbar_for_jobs
        self.show_counter_only = show_counter_only
        self.niceness_clients = niceness_clients

    @staticmethod 
    def _start_client(authkey,
                      port, 
                      client_class,
                      nproc                   = 0, 
                      nice                    = 19, 
                      delay                   = 1, 
                      verbose                 = None,
                      show_statusbar_for_jobs = False,
                      show_counter_only       = False):        # ignore signal, because any signal bringing the server down
        # will cause an error in the client server communication
        # therefore the clients will also quit 
        Signal_to_SIG_IGN(signals=[signal.SIGINT, signal.SIGTERM])
        time.sleep(delay)
        client = client_class(server='localhost',
                              authkey                 = authkey,
                              port                    = port,
                              nproc                   = nproc, 
                              nice                    = nice,
                              verbose                 = verbose,
                              show_statusbar_for_jobs = show_statusbar_for_jobs,
                              show_counter_only       = show_counter_only)
        
        client.start()
        
        
    def start(self):
        p_client = mp.Process(target=JobManager_Local._start_client,
                              args=(self.authkey,
                                    self.port, 
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
                                           prefix  = 'local_client',
                                           timeout = 2)

class RemoteKeyError(RemoteError):
    pass

class RemoteValueError(RemoteError):
    pass

class Signal_handler_for_Jobmanager_client(object):
    def __init__(self, client_object, exit_handler, signals=[signal.SIGINT]):
        self.client_object = client_object
        self.exit_handler = exit_handler
        for s in signals:
            log.debug("setup Signal_handler_for_Jobmanager_client for signal %s", progress.signal_dict[s])
            signal.signal(s, self._handler)
            
    def _handler(self, sig, frame):
        log.info("received signal %s", progress.signal_dict[sig])
        
        if self.client_object.pbc is not None:
            self.client_object.pbc.pause()
        
        try:
            r = input(progress.ESC_BOLD + progress.ESC_LIGHT_RED+"<q> - quit, <i> - server info: " + progress.ESC_NO_CHAR_ATTR)
        except:
            r = 'q'

        if r == 'i':
            self._show_server_info()
        elif r == 'q':
            log.info("terminate worker functions")
            self.exit_handler._handler(sig, frame)
            log.info("call sys.exit -> raise SystemExit")
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


class Signal_to_SIG_IGN(object):
    def __init__(self, signals=[signal.SIGINT, signal.SIGTERM]):
        for s in signals:
            signal.signal(s, self._handler)
    
    def _handler(self, sig, frame):
        log.info("PID %s: received signal %s -> will be ignored", os.getpid(), progress.signal_dict[sig])


class Signal_to_sys_exit(object):
    def __init__(self, signals=[signal.SIGINT, signal.SIGTERM]):
        for s in signals:
            signal.signal(s, self._handler)
    def _handler(self, signal, frame):
        log.info("PID %s: received signal %s -> call sys.exit -> raise SystemExit", os.getpid(), progress.signal_dict[signal])
        sys.exit('exit due to signal {}'.format(progress.signal_dict[signal]))
        
 
class Signal_to_terminate_process_list(object):
    """
    SIGINT and SIGTERM will call terminate for process given in process_list
    """
    def __init__(self, process_list, identifier_list, signals = [signal.SIGINT, signal.SIGTERM], timeout=2):
        self.process_list = process_list
        self.identifier_list = identifier_list
        self.timeout = timeout
        
        for s in signals:
            log.debug("setup Signal_to_terminate_process_list for signal %s", progress.signal_dict[s])
            signal.signal(s, self._handler)
            
    def _handler(self, signal, frame):
        log.debug("received sig %s -> terminate all given subprocesses", progress.signal_dict[signal])
        for i, p in enumerate(self.process_list):
            p.terminate()
            
        for i, p in enumerate(self.process_list):            
            progress.check_process_termination(proc       = p, 
                                               prefix     = self.identifier_list[i], 
                                               timeout    = self.timeout,
                                               auto_kill_on_last_resort=False)

def address_authkey_from_proxy(proxy):
    return proxy._token.address, proxy._authkey.decode()

def address_authkey_from_manager(manager):
    return manager.address, manager._authkey.decode()

def call_connect_python3(connect, dest, reconnect_wait=2, reconnect_tries=3):
    c = 0
    while True:
        try:                                # here we try re establish the connection
            log.debug("try connecting to %s", dest)
            connect()
    
        except Exception as e:
            log.error("connection to %s could not be established due to '%s'", dest, type(e))
            log.error(traceback.format_stack()[-3].strip())             
            
            if type(e) is ConnectionResetError:           # ... when the destination hangs up on us     
                c = handler_connection_reset(dest, c, reconnect_wait, reconnect_tries)
            elif type(e) is ConnectionRefusedError:       # ... when the destination refuses our connection
                handler_connection_refused(e, dest)
            elif type(e) is AuthenticationError :         # ... when the destination refuses our connection due authkey missmatch
                handler_authentication_error(e, dest)
            elif type(e) is RemoteError:                  # ... when the destination send us an error message
                if 'KeyError' in e.args[0]:
                    handler_remote_key_error(e, dest)
                elif 'ValueError: unsupported pickle protocol:' in e.args[0]:
                    handler_remote_value_error(e, dest)
                else:
                    handler_remote_error(e, dest)
            elif type(e) is ValueError:
                handler_value_error(e)
            else:                                   # any other exception
                handler_unexpected_error(e)
        
        else:                               # no exception
            log.debug("connection to %s successfully established".format(dest))
            return True      

def call_connect_python2(connect, dest, reconnect_wait=2, reconnect_tries=3):
    c = 0
    while True:
        try:                                # here we try re establish the connection
            log.debug("try connecting to %s", dest)
            connect()
        
        except Exception as e:
            log.error("connection to %s could not be established due to '%s'", dest, type(e))
            log.info(traceback.format_stack()[-3].strip())
            
            if type(e) is socket.error:                    # error in socket communication
                log.error("caught %s with args %s", type(e), e.args) 
                err_code = e.args[0]
                if err_code == errno.ECONNRESET:     # ... when the destination hangs up on us
                    c = handler_connection_reset(dest, c, reconnect_wait, reconnect_tries)
                elif err_code == errno.ECONNREFUSED: # ... when the destination refuses our connection
                    handler_connection_refused(e, dest)
                else:
                    handler_unexpected_error(e)
            elif type(e) is AuthenticationError :       # ... when the destination refuses our connection due authkey missmatch
                handler_authentication_error(e, dest)
            elif type(e) is RemoteError:                   # ... when the destination send us an error message
                if 'KeyError' in e.args[0]:
                    handler_remote_key_error(e, dest)
                elif 'ValueError: unsupported pickle protocol:' in e.args[0]:
                    handler_remote_value_error(e, dest)
                else:
                    handler_remote_error(e, dest)
            elif type(e) is ValueError:
                handler_value_error(e)
            else:                                    # any other exception
                handler_unexpected_error(e)            
        
        else:                               # no exception
            log.debug("connection to %s successfully established", dest)
            return True                     # SUCCESS -> return True            

call_connect = call_connect_python2 if sys.version_info[0] == 2 else call_connect_python3

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
     
def getDateForFileName(includePID = False):
    """returns the current date-time and optionally the process id in the format
    YYYY_MM_DD_hh_mm_ss_pid
    """
    date = time.localtime()
    name = '{:d}_{:02d}_{:02d}_{:02d}_{:02d}_{:02d}'.format(date.tm_year, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min, date.tm_sec)
    if includePID:
        name += "_{}".format(os.getpid()) 
    return name

def handler_authentication_error(e, dest):
    log.error("authentication error")
    log.info("Authkey specified does not match the authkey at destination side!")
    raise e

def handler_broken_pipe_error(e):
    log.error("broken pip error")
    log.info("This usually means that an established connection was closed\n")
    log.info("does not exists anymore, probably the server went down")
    raise e   

def handler_connection_refused(e, dest):
    log.error("connection refused error")
    log.info("This usually means that no matching Manager object was instanciated at destination side!")
    log.info("Either there is no Manager running at all, or it is listening to another port.")
    raise JMConnectionRefusedError(e)

def handler_connection_reset(dest, c, reconnect_wait, reconnect_tries):
    log.error("connection reset error")
    log.info("During 'connect' this error might be due to firewall settings"+
             "or other TPC connections controlling mechanisms!")
    c += 1
    if c > reconnect_tries:
        log.error("maximium number of reconnects %s was reached", reconnect_tries)
        raise JMConnectionError("connection to %s FAILED, ".format(dest)+
                                "{} retries were NOT successfull".format(reconnect_tries))
    log.debug("try connecting to %s again in %s seconds", dest, reconnect_wait)
    time.sleep(reconnect_wait)
    return c                

def handler_eof_error(e):
    log.error("EOF error")
    log.info("This usually means that server did not replay, although the connection is still there.\n"+
             "This is due to the fact that the connection is in 'timewait' status for about 60s\n"+
             "after the server went down inappropriately.")
    raise e

def handler_remote_error(e, dest):
    log.error("remote error")
    log.info("The server %s send an RemoteError message!\n%s", dest, e.args[0])
    raise RemoteError(e.args[0])

def handler_remote_key_error(e, dest):
    log.error("remote key error")
    log.info("'KeyError' detected in RemoteError message from server %s!\n"+
             "This hints to the fact that the actual instace of the shared object on the server side has changed,\n"+
             "for example due to a server restart you need to reinstanciate the proxy object.", dest)
    raise RemoteKeyError(e.args[0])
    
def handler_remote_value_error(e, dest):
    log.error("remote value error")
    log.info("'ValueError' due to 'unsupported pickle protocol' detected in RemoteError from server %s!\n"+
             "You might have tried to connect to a SERVER running with an OLDER python version.\n"+
             "At this stage (and probably for ever) this should be avoided!", dest)        
    raise RemoteValueError(e.args[0])

def handler_value_error(e):
    log.error("value error")
    if 'unsupported pickle protocol' in e.args[0]:
        log.info("'ValueError' due to 'unsupported pickle protocol'!\n"
                 "You might have tried to connect to a SERVER running with an NEWER python version.\n"
                 "At this stage (and probably for ever) this should be avoided.\n")  
    raise e

def handler_unexpected_error(e):
    log.error("unexpected error of type %s and args %s", type(e), e.args)
    raise e

def handle_unexpected_queue_error(e):
    log.error("unexpected error of type %s and args %s\n"+
              "I guess the server went down, can't do anything, terminate now!", type(e), e.args)
    log.debug(traceback.print_exc())

def emergency_dump(arg, res, path):
    now = datetime.now().isoformat()
    pid = os.getpid()
    fname = "{}_pid_{}".format(now, pid)
    full_path = os.path.join(path, fname)
    log.warning("emergency dump (arg, res) to %s", full_path)
    with open(full_path, 'wb') as f:
        pickle.dump(arg, f)
        pickle.dump(res, f)

def check_if_host_is_reachable_unix_ping(adr, timeout=2, retry=5):
    for i in range(retry):
        try:
            cmd = 'ping -c 1 -W {} {}    '.format(int(timeout), adr)
            log.debug("[%s/%s]call: %s", i+1, retry, cmd)
            subprocess.check_output(cmd, shell = True)
        except subprocess.CalledProcessError as e:
            # on exception, resume with loop
            log.warning("CalledProcessError on ping with message: %s", e)
            continue
        else:
            # no exception, ping was successful, return without error
            log.debug("ping was succesfull")
            return
        
    # no early return happend, ping was never successful, raise error
    log.error("ping failed after %s retries", retry)
    raise JMHostNotReachableError("could not reach host '{}'\nping error reads: {}".format(adr, e.output))
        

def proxy_operation_decorator_python3(proxy, operation, reconnect_wait=2, reconnect_tries=3, ping_timeout=2, ping_retry=5):
    o = getattr(proxy, operation)
    dest = address_authkey_from_proxy(proxy)
    
    def _operation(*args, **kwargs):
        c = 0
        while True:
            check_if_host_is_reachable_unix_ping(adr     = dest[0][0],
                                                 timeout = ping_timeout,
                                                 retry   = ping_retry)
            log.debug("establish connection to %s", dest)
            try: 
                proxy._connect()
            except Exception as e:
                log.warning("establishing connection to %s FAILED due to '%s'", dest, type(e))
                log.debug("show traceback.format_stack()[-3]\n%s", traceback.format_stack()[-3].strip())
                c += 1
                if c > reconnect_tries:
                    log.error("reached maximum number of reconnect tries %s, raise exception", reconnect_tries)
                    raise e
                log.info("wait %s seconds and retry", reconnect_wait)                
                time.sleep(reconnect_wait)
                continue
                
            log.debug("execute operation '%s' -> %s", operation, dest)
            
            try:
                res = o(*args, **kwargs)
            except queue.Empty as e:
                log.info("operation '%s' -> %s FAILED due to '%s'", operation, dest, type(e))
                raise e
            except Exception as e:
                log.warning("operation '%s' -> %s FAILED due to '%s'", operation, dest, type(e))
                log.debug("show traceback.format_stack()[-3]\n%s", traceback.format_stack()[-3].strip())
                if type(e) is ConnectionResetError:
                    log.debug("show traceback.print_exc(limit=1))")
                    log.debug(traceback.print_exc(limit=1))

                    c += 1
                    if c > reconnect_tries:
                        log.error("reached maximum number of reconnect tries %s", reconnect_tries)
                        raise e

                    log.info("wait %s seconds and retry", reconnect_wait)
                    time.sleep(reconnect_wait)
                    continue
                elif type(e) is BrokenPipeError:
                    handler_broken_pipe_error(e)
                elif type(e) is EOFError:
                    handler_eof_error(e)
                else:
                    handler_unexpected_error(e)
            else:                               # SUCCESS -> return True
                log.debug("operation '%s' successfully executed", operation)
                return res
        
            log.debug("close connection to %s", dest)
            try:
                proxy._tls.connection.close()
            except Exception as e:
                log.error("closeing connection to %s FAILED due to %s", dest, type(e))
                log.info("show traceback.format_stack()[-3]\n%s", traceback.format_stack()[-3].strip())
            
    return _operation

def proxy_operation_decorator_python2(proxy, operation, reconnect_wait=2, reconnect_tries=3, ping_timeout=2, ping_retry=5):
    o = getattr(proxy, operation)
    dest = address_authkey_from_proxy(proxy)
    
    def _operation(*args, **kwargs):
        c = 0
        while True:
            check_if_host_is_reachable_unix_ping(adr     = dest[0][0],
                                                 timeout = ping_timeout,
                                                 retry   = ping_retry)
            log.debug("establishing connection to %s ...", dest)
            try: 
                proxy._connect()
            except Exception as e:
                log.warning("establishing connection to %s FAILED due to '%s'", dest, type(e))
                log.debug("show traceback.format_stack()[-3]\n%s", traceback.format_stack()[-3].strip())
                c += 1
                if c > reconnect_tries:
                    log.error("reached maximum number of reconnect tries %s, raise exception", reconnect_tries)
                    raise e
                log.info("wait %s seconds and retry", reconnect_wait)                
                time.sleep(reconnect_wait)
                continue
                 
            log.debug("execute operation '%s' -> %s", operation, dest)
            
            try:
                res = o(*args, **kwargs)
            except queue.Empty as e:
                log.info("operation '%s' -> %s FAILED due to '%s'", operation, dest, type(e))
                raise e                
            except Exception as e:
                log.warning("operation '%s' -> %s FAILED due to '%s'", operation, dest, type(e))
                log.debug("show traceback.format_stack()[-3]\n%s", traceback.format_stack()[-3].strip())
                
                if type(e) is IOError:
                    log.debug("%s with args %s", type(e), e.args)
                    err_code = e.args[0]
                    if err_code == errno.ECONNRESET:     # ... when the destination hangs up on us
                        log.debug("show traceback.print_exc(limit=1))")
                        log.debug(traceback.print_exc(limit=1))
                        
                        c += 1
                        if c > reconnect_tries:
                            log.error("reached maximum number of reconnect tries %s", reconnect_tries)
                            raise e
                        
                        log.info("wait %s seconds and retry", reconnect_wait)
                        time.sleep(reconnect_wait)
                        continue
                    else:
                        handler_unexpected_error(e) 
                elif type(e) is EOFError:
                    handler_eof_error(e)
                else:
                    handler_unexpected_error(e)
            else: # SUCCESS -> return True
                log.debug("operation '%s' successfully executed", operation)
                return res
            
            log.debug("close connection to %s", dest)
            try:
                proxy._tls.connection.close()
            except Exception as e:
                log.error("closeing connection to %s FAILED due to %s", dest, type(e))
                log.info("show traceback.format_stack()[-3]\n%s", traceback.format_stack()[-3].strip())
            
    return _operation

proxy_operation_decorator = proxy_operation_decorator_python2 if sys.version_info[0] == 2 else proxy_operation_decorator_python3

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
    
    As we want to shut down the SyncManager at the last stage of cleanup
    we have to prevent this default signal handling by passing this functions
    to the SyncManager start routine.
    """
    Signal_to_SIG_IGN(signals=[signal.SIGINT, signal.SIGTERM])
            

def try_pickle(obj, show_exception=False):
    blackhole = open(os.devnull, 'wb')
    try:
        pickle.dump(obj, blackhole)
        return True
    except:
        if show_exception:
            traceback.print_exc()
        return False
        

# a list of all names of the implemented python signals
all_signals = [s for s in dir(signal) if (s.startswith('SIG') and s[3] != '_')]



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
#import functools
import inspect
import multiprocessing as mp
import subprocess as sp
from multiprocessing.managers import BaseManager, RemoteError
import subprocess
import numpy as np
import os
import pickle
import signal
import socket
import sys
import time
import traceback
import warnings
from . import binfootprint as bf

from datetime import datetime

# This is a list of all python objects that will be imported upon
# initialization during module import (see __init__.py)
__all__ = ["JobManager_Client",
           "JobManager_Local",
           "JobManager_Server",
           "getDateForFileName",
           "hashDict",
           "hashableCopyOfNumpyArray"
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

sys.path.append(os.path.dirname(__file__))
from . import progress

myQueue = mp.Queue
AuthenticationError = mp.AuthenticationError

def humanize_size(size_in_bytes):
    """convert a speed in counts per second to counts per [s, min, h, d], choosing the smallest value greater zero.
    """
    thr = 999
    scales = [1024, 1024, 1024]
    units = ['k', 'M', 'G', 'T']
    i = 0
    while (size_in_bytes > thr) and (i < len(scales)):
        size_in_bytes = size_in_bytes / scales[i]
        i += 1
    return "{:.2f}{}B".format(size_in_bytes, units[i]) 

def get_user():
    try:
        out = subprocess.check_output('id -un', shell=True).decode().strip()
        return out
    except Exception as e:
        print("failed to determine user")
        print(e)
        return None
    
def get_user_process_limit():
    try:
        out = subprocess.check_output('ulimit -u', shell=True).decode().strip()
        return int(out)
    except Exception as e:
        print("failed to determine maximum number of user processeses")
        print(e)
        return None
    
def get_user_num_process():
    try:
        out = subprocess.check_output('ps ut | wc -l', shell=True).decode().strip()
        return int(out)-2
    except Exception as e:
        print("failed to determine current number of processes")
        print(e)
        return None    
    
    
         

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
                 verbose                 = 1,
                 show_statusbar_for_jobs = True,
                 show_counter_only       = False,
                 interval                = 0.3,
                 emergency_dump_path     = '.',
                 result_q_timeout        = 30,                 
                 job_q_timeout           = 0.1,
                 fail_q_timeout          = 10):
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
        """
        
        self.show_statusbar_for_jobs = show_statusbar_for_jobs
        self.show_counter_only = show_counter_only
        self.interval = interval
        self.verbose = verbose
        
        self._result_q_timeout = result_q_timeout
        self._job_q_timeout    = job_q_timeout
        self._fail_q_timeout   = fail_q_timeout
        
        self._pid = os.getpid()
        self._sid = os.getsid(self._pid)
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
            if self.nproc <= 0:
                raise RuntimeError("Invalid Number of Processes\ncan not spawn {} processes (cores found: {}, cores NOT to use: {} = -nproc)".format(self.nproc, mp.cpu_count(), abs(nproc)))
        # internally, njobs must be negative for infinite jobs
        if njobs == 0:
            njobs -= 1
        self.njobs = njobs
        self.emergency_dump_path = emergency_dump_path
        
        self.procs = []
        
        self.manager_objects = None  # will be set via connect()
        self.connect()               # get shared objects from server
        
        self.pbc = None
        
    def connect(self):
        if self.manager_objects is None:
            self.manager_objects = self.get_manager_objects()
        else:
            if self.verbose > 0:
                print("{}: already connected (at least shared object are available)".format(self._identifier))

    @property
    def connected(self):
        return self.manager_objects is not None
    
    def _dump_result_to_local_storage(self, res):
        pass
       
    def get_manager_objects(self):
        return JobManager_Client._get_manager_objects(self.server, 
                                                      self.port, 
                                                      self.authkey, 
                                                      self._identifier,
                                                      self.verbose)
#     @staticmethod
#     def _get_sync_manager_data(manager):        
       
    @staticmethod
    def _get_manager_objects(server, port, authkey, identifier, verbose=0, reconnect_wait=2, reconnect_tries=3):
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
    
        manager = ServerQueueManager(address=(server, port), authkey=authkey)

        try:
            call_connect(connect         = manager.connect,
                         dest            = address_authkey_from_manager(manager),
                         verbose         = verbose,
                         identifier      = identifier, 
                         reconnect_wait  = reconnect_wait, 
                         reconnect_tries = reconnect_tries)
        except:
            print("{}: FAILED to connect to {}".format(identifier, address_authkey_from_manager(manager)))
            return None
            
            
        job_q = manager.get_job_q()
        if verbose > 1:
            print("{}: found job_q with {} jobs".format(identifier, job_q.qsize()))
        
        result_q = manager.get_result_q()
        fail_q = manager.get_fail_q()
        # deep copy const_arg from manager -> non shared object in local memory
        const_arg = copy.deepcopy(manager.get_const_arg())
            
        return job_q, result_q, fail_q, const_arg, manager
    
    def get_overall_memory_cunsumption(self):
        vsize = 0
        # Example: /bin/ps -o vsize= --sid 23928
        #proc = sp.Popen(['ps', '-o', 'vsize=', '--sid', str(self._sid)], stdout=sp.PIPE, stderr=None, shell=False)
        proc = sp.Popen(['ps', '-o', 'rss=', '--sid', str(self._sid)], stdout=sp.PIPE, stderr=None, shell=False)
        (stdout, _stderr) = proc.communicate()
        print("sid", self._sid)
        print(stdout)
        # Iterate over each process within the process tree of our process session
        # (this ensures that we include processes launched by a child bash script, etc.)
        for line in stdout.split():
            vsize += int(line.strip())
        return vsize*1024   # in bytes
        
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
    def _handle_unexpected_queue_error(e, verbose, identifier):
        print("{}: unexpected Error {}, I guess the server went down, can't do anything, terminate now!".format(identifier, e))
        if verbose > 0:
            traceback.print_exc()

    @staticmethod
    def __emergency_dump(arg, res, path, identifier):
        now = datetime.now().isoformat()
        pid = os.getpid()
        fname = "{}_pid_{}".format(now, pid)
        full_path = os.path.join(path, fname)
        print("{}: emergency dump (arg, res) to {}".format(identifier, full_path))
        with open(full_path, 'wb') as f:
            pickle.dump(arg, f)
            pickle.dump(res, f)

    @staticmethod
    def __worker_func(func, nice, verbose, server, port, authkey, i, manager_objects, c, m, reset_pbc, njobs, 
                      emergency_dump_path, job_q_timeout, fail_q_timeout, result_q_timeout):
        """
        the wrapper spawned nproc times calling and handling self.func
        """
        identifier = progress.get_identifier(name='worker{}'.format(i+1))
        Signal_to_sys_exit(signals=[signal.SIGTERM])
        Signal_to_SIG_IGN(signals=[signal.SIGINT])

        job_q, result_q, fail_q, const_arg, manager = manager_objects 
        
        n = os.nice(0)
        try:
            n = os.nice(nice - n)
        except PermissionError:
            if verbose > 0:
                print("{}: changing niceness not permitted! run with niceness {}".format(identifier, n))

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
                kwargs = {count_args[0]: c,
                          count_args[1]: m}
                return func(arg, const_arg, **kwargs)
        else:
            if verbose > 1:
                print("{}: found standard keyword arguments: [c, m]".format(identifier))
            _func = func
            
        job_q_get    = proxy_operation_decorator(proxy           = job_q,
                                                 operation       = 'get',
                                                 verbose         = verbose, 
                                                 identifier      = identifier, 
                                                 reconnect_wait  = 2, 
                                                 reconnect_tries = 3)
        job_q_put    = proxy_operation_decorator(proxy           = job_q,
                                                 operation       = 'put',
                                                 verbose         = verbose, 
                                                 identifier      = identifier, 
                                                 reconnect_wait  = 2, 
                                                 reconnect_tries = 3)
        result_q_put = proxy_operation_decorator(proxy           = result_q,
                                                 operation       = 'put',
                                                 verbose         = verbose, 
                                                 identifier      = identifier, 
                                                 reconnect_wait  = 2, 
                                                 reconnect_tries = 3)
        fail_q_put   = proxy_operation_decorator(proxy           = fail_q,
                                                 operation       = 'put',
                                                 verbose         = verbose, 
                                                 identifier      = identifier, 
                                                 reconnect_wait  = 2, 
                                                 reconnect_tries = 3)        
        
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
                    arg = job_q_get(block = True, timeout = job_q_timeout)
                    tg_1 = time.time()
                    time_queue += (tg_1-tg_0)
                 
                # regular case, just stop working when empty job_q was found
                except queue.Empty:
                    if verbose > 0:
                        print("{}: finds empty job queue, processed {} jobs".format(identifier, cnt))
                    break
                # handle SystemExit in outer try ... except
                except SystemExit as e:
                    raise e
                # job_q.get failed -> server down?             
                except Exception as e:
                    print("{}: Error when calling 'job_q_get'".format(identifier)) 
                    JobManager_Client._handle_unexpected_queue_error(e, verbose, identifier)
                    break
                
                # try to process the retrieved argument
                try:
                    tf_0 = time.time()
                    res = _func(arg, const_arg, c, m)
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
                    if verbose > 0:
                        print("{}: caught exception '{}' when crunching 'func'".format(identifier, err.__name__))
                    
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
                        fail_q_put((arg, err.__name__, hostname), block = True, timeout=fail_q_timeout)
                    # handle SystemExit in outer try ... except                        
                    except SystemExit as e:
                        if verbose > 1:
                            print(" FAILED!")
                        raise e
                    # fail_q.put failed -> server down?             
                    except Exception as e:
                        if verbose > 1:
                            print(" FAILED!")
                        JobManager_Client._handle_unexpected_queue_error(e, verbose, identifier)
                        break
                    else:
                        if verbose > 1:
                            print(" done!")
                            
                # processing the retrieved arguments succeeded
                # - try to send the result back to the server                        
                else:
                    try:
                        tp_0 = time.time()
                        result_q_put((arg, res), block = True, timeout=result_q_timeout)
                        tp_1 = time.time()
                        time_queue += (tp_1-tp_0)
                        
                    # handle SystemExit in outer try ... except
                    except SystemExit as e:
                        raise e
                    
                    except Exception as e:
                        print("{}: Error when calling 'result_q_put'".format(identifier))
                        JobManager_Client.__emergency_dump(arg, res, emergency_dump_path, identifier)
                        JobManager_Client._handle_unexpected_queue_error(e, verbose, identifier)
                        break
                    
                cnt += 1
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
                job_q.put(arg, timeout=fail_q_timeout)
            # handle SystemExit in outer try ... except                        
            except SystemExit as e:
                if verbose > 1:
                    print(" FAILED!")
                raise e
            # fail_q.put failed -> server down?             
            except Exception as e:
                if verbose > 1:
                    print(" FAILED!")
                JobManager_Client._handle_unexpected_queue_error(e, verbose, identifier)
            else:
                if verbose > 1:
                    print(" done!")
                
        if verbose > 0:
            try:
                print("{}: pure calculation time: {}  single task average: {}".format(identifier, progress.humanize_time(time_calc), progress.humanize_time(time_calc / cnt) ))
                print("{}: calculation:{:.2%} communication:{:.2%}".format(identifier, time_calc/(time_calc+time_queue), time_queue/(time_calc+time_queue)))
            except:
                pass
        if verbose > 1:
            print("{}: JobManager_Client.__worker_func at end (PID {})".format(identifier, os.getpid()))

    def start(self):
        """
        starts a number of nproc subprocess to work on the job_q
        
        SIGTERM and SIGINT are managed to terminate all subprocesses
        
        retruns when all subprocesses have terminated
        """
        
        if not self.connected:
            raise JMConnectionError("Can not start Client with no connection to server (shared objetcs are not available)")

        print("{}: starting client with connection to server:{} authkey:{} port:{}".format(self._identifier, self.server, self.authkey.decode(), self.port))
        
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
            Progress = progress.ProgressBarCounterFancy
        else:
            Progress = progress.ProgressSilentDummy
            
        prepend = []
        infoline = progress.StringValue(num_of_bytes=12)
        infoline = None
        l = len(str(self.nproc))
        for i in range(self.nproc):
            prepend.append("w{0:0{1}}:".format(i+1, l))
            
        with Progress(count     = c, 
                      max_count = m_progress, 
                      interval  = self.interval,
                      prepend   = prepend, 
                      verbose   = self.verbose,
                      sigint    = 'ign',
                      sigterm   = 'ign',
                      info_line  = infoline) as self.pbc :
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
                                                                reset_pbc,
                                                                self.njobs,
                                                                self.emergency_dump_path,
                                                                self._job_q_timeout,
                                                                self._fail_q_timeout,
                                                                self._result_q_timeout))
                self.procs.append(p)
                p.start()
                time.sleep(0.3)

            time.sleep(self.interval/2)
            exit_handler = Signal_to_terminate_process_list(identifier      = self._identifier,
                                                            process_list    = self.procs,
                                                            identifier_list = [progress.get_identifier(name = "worker{}".format(i+1),
                                                                                                       pid  = p.pid,
                                                                                                       bold = True) for i, p in enumerate(self.procs)],
                                                            signals         = [signal.SIGTERM],                                                            
                                                            verbose         = self.verbose,
                                                            timeout         = 2)
            
            interrupt_handler = Signal_handler_for_Jobmanager_client(client_object = self,
                                                                     exit_handler=exit_handler,
                                                                     signals=[signal.SIGINT],
                                                                     verbose=self.verbose)
        
            for p in self.procs:
#                 s = self.get_overall_memory_cunsumption()
#                 infoline.value = bytes(humanize_size(s), encoding='ascii')
                if self.verbose > 2:
                    print("{}: join {} PID {}".format(self._identifier, p, p.pid))
                while p.is_alive():
                    if self.verbose > 2:
                        print("{}: still alive {} PID {}".format(self._identifier, p, p.pid))
                    p.join(timeout=self.interval)

                if self.verbose > 2:
                    print("{}: process {} PID {} was joined".format(self._identifier, p, p.pid))
                    
                    
            if self.verbose > 2:
                print("{}: still in progressBar context".format(self._identifier))                    
                                        
        if self.verbose > 2:
            print("{}: progressBar context has been left".format(self._identifier))


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
            
      
        self.const_arg = const_arg
        
        
        self.fname_dump = fname_dump        
        self.msg_interval = msg_interval
        self.speed_calc_cycles = speed_calc_cycles

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
        manager_identifier = progress.get_identifier(name='SyncManager')
        
        # stop SyncManager
        self.manager.shutdown()
        
        progress.check_process_termination(proc=manager_proc, 
                                  identifier=manager_identifier, 
                                  timeout=2, 
                                  verbose=self.verbose, 
                                  auto_kill_on_last_resort=True)

    def __check_bind(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((self.hostname, self.port))
            except:
                print("{}: test bind to {}:{} failed".format(progress.ESC_RED + self._identifier, self.hostname, self.port))
                raise
                
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
            print("{}: can not start {} on {}:{}".format(progress.ESC_RED + self._identifier, self.__class__.__name__, self.hostname, self.port))
            print("{}: this is usually the case when the port used is not available!".format(progress.ESC_RED + self._identifier))
            
            manager_proc = self.manager._process
            manager_identifier = progress.get_identifier(name='SyncManager')
            progress.check_process_termination(proc=manager_proc, 
                                               identifier=manager_identifier, 
                                               timeout=0.3, 
                                               verbose=self.verbose, 
                                               auto_kill_on_last_resort=True)
            
            self.manager = None
            return False
        
        if self.verbose > 1:
            print("{}: started on {}:{} with authkey '{}'".format(progress.get_identifier('SyncManager', self.manager._process.pid), 
                                                                  self.hostname, 
                                                                  self.port,  
                                                                  authkey))
        return True
    
    def __restart_SyncManager(self):
        self.__stop_SyncManager()
        if not self.__start_SyncManager():
            raise RuntimeError("could not start server")
        
    def __enter__(self):
        return self
        
    def __exit__(self, err, val, trb):
        # KeyboardInterrupt via SIGINT will be mapped to SystemExit  
        # SystemExit is considered non erroneous
        if (err is None) or (err == SystemExit):
            if self.verbose > 0:
                print("{}: normal shutdown".format(self._identifier))
            # bring everything down, dump status to file 
            self.shutdown()
            # no exception traceback will be printed
            return True
        else:
            if self.verbose > 0:
                print("{}: shutdown due to exception '{}'".format(progress.ESC_RED + self._identifier, err.__name__))
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
        
        # do user defined final processing
        self.process_final_result()
        if self.verbose > 1:
            print("{}: process_final_result done!".format(self._identifier))
        
        # print(self.fname_dump)
        if self.fname_dump is not None:
            if self.fname_dump == 'auto':
                fname = "{}_{}.dump".format(self.authkey.decode('utf8'), getDateForFileName(includePID=False))
            else:
                fname = self.fname_dump

            if self.verbose > 0:
                print("{}: dump current state to '{}'".format(self._identifier, fname))    
            with open(fname, 'wb') as f:
                self.__dump(f)

            if self.verbose > 1:
                print("{}:dump state done!".format(self._identifier))

        else:
            if self.verbose > 0:
                print("{}: fname_dump == None, ignore dumping current state!".format(self._identifier))
        
        # start also makes sure that it was not started as subprocess
        # so at default behavior this assertion will allays be True
        assert self._pid == os.getpid()
        
        self.show_statistics()
        
        self.__stop_SyncManager()
        if self.verbose > 1:
            print("{}: SyncManager stop done!".format(self._identifier))
        
        
        print("{}: JobManager_Server was successfully shut down".format(self._identifier))
        
    def show_statistics(self):
        if self.verbose > 0:
            all_jobs = self.numjobs
            succeeded = self.numresults
            failed = self.fail_q.qsize()
            all_processed = succeeded + failed
            
            id  = self._identifier + ": "
            stripped_id = progress.remove_ESC_SEQ_from_string(self._identifier)
            l = len(stripped_id)
            id2 = ' '*l + "| " 
            
            print("{}total number of jobs  : {}".format(id, all_jobs))
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
                raise RuntimeWarning("'all_not_processed != len(self.args_dict)' something is inconsistent!")
            

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
            raise RuntimeError("fname_dump must not be 'auto' when reading old state")
        
        if not os.path.isfile(fname_dump):
            raise RuntimeError("file '{}' to read old state from not found".format(fname_dump))

        if self.verbose > 0:
            print("{}: load state from file '{}'".format(self._identifier, fname_dump))
        
        with open(fname_dump, 'rb') as f:
            self.__load(f)
        
        self.show_statistics()
            
    def put_arg(self, a):
        """add argument a to the job_q
        """
#         if (not hasattr(a, '__hash__')) or (a.__hash__ == None):
#             # try to add hashability
#             if isinstance(a, dict):
#                 a = hashDict(a)
#             elif isinstance(a, np.ndarray):
#                 a = hashableCopyOfNumpyArray(a)
#             else:
#                 raise AttributeError("'{}' is not hashable".format(type(a)))
        
        bfa = bf.dump(a)
        if bfa in self.args_dict:
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

    def start(self):
        """
        starts to loop over incoming results
        
        When finished, or on exception call stop() afterwards to shut down gracefully.
        """
        
        if not self.__start_SyncManager():
            raise RuntimeError("could not start server")
        
        if self._pid != os.getpid():
            raise RuntimeError("do not run JobManager_Server.start() in a subprocess")

        if (self.numjobs - self.numresults) != len(self.args_dict):
            if self.verbose > 1:
                print("numjobs: {}".format(self.numjobs))
                print("numresults: {}".format(self.numresults))
                print("len(self.args_dict): {}".format(len(self.args_dict)))
                
            raise RuntimeError("inconsistency detected! (self.numjobs - self.numresults) != len(self.args_dict)! use JobManager_Server.put_arg to put arguments to the job_q")
        
        if self.numjobs == 0:
            print("{}: WARNING no jobs to process! use JobManager_Server.put_arg to put arguments to the job_q".format(self._identifier))
            return
        else:
            print("{}: started (host:{} authkey:{} port:{} jobs:{})".format(self._identifier, self.hostname, self.authkey.decode(), self.port, self.numjobs))
        
        Signal_to_sys_exit(signals=[signal.SIGTERM, signal.SIGINT], verbose = self.verbose)
        pid = os.getpid()
        user = get_user()
        max_proc = get_user_process_limit()
        
        if self.verbose > 1:
            print("{}: start processing incoming results".format(self._identifier))
        
        if self.verbose > 0:
            Progress = progress.ProgressBarFancy
        else:
            Progress = progress.ProgressSilentDummy
  
        info_line = progress.StringValue(num_of_bytes=100)
  
        with Progress(count             = self._numresults,
                      max_count         = self._numjobs, 
                      interval          = self.msg_interval,
                      speed_calc_cycles = self.speed_calc_cycles,
                      verbose           = self.verbose,
                      sigint            = 'ign',
                      sigterm           = 'ign',
                      info_line         = info_line) as stat:

            stat.start()
        
            while (len(self.args_dict) - self.fail_q.qsize()) > 0:
                info_line.value = "result_q size:{}, job_q size:{}, recieved results:{}, proc (curr/max): {}/{}".format(self.result_q.qsize(), 
                                                                                                                        self.job_q.qsize(), 
                                                                                                                        self.numresults,
                                                                                                                        get_user_num_process(),
                                                                                                                        max_proc).encode('utf-8')
        
                # allows for update of the info line
                try:
                    arg, result = self.result_q.get(timeout = self.msg_interval)
                except queue.Empty:
                    continue
                
                del self.args_dict[bf.dump(arg)]
                self.numresults += 1
                self.process_new_result(arg, result)
        
        if self.verbose > 1:
            print("{}: wait {}s before trigger clean up".format(self._identifier, self.__wait_before_stop))
        time.sleep(self.__wait_before_stop)
        

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
                              port=port,
                              nproc=nproc, 
                              nice=nice,
                              verbose=verbose,
                              show_statusbar_for_jobs=show_statusbar_for_jobs,
                              show_counter_only=show_counter_only)
        
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
                                           identifier='local_client',
                                           timeout=2,
                                           verbose=self.verbose_client)

class RemoteKeyError(RemoteError):
    pass

class RemoteValueError(RemoteError):
    pass

class Signal_handler_for_Jobmanager_client(object):
    def __init__(self, client_object, exit_handler, signals=[signal.SIGINT], verbose=0):
        self.client_object = client_object
        self.exit_handler = exit_handler
        self.verbose = verbose
        for s in signals:
            signal.signal(s, self._handler)
            
    def _handler(self, sig, frame):
        if self.verbose > 0:
            print("{}: received signal {}".format(self.client_object._identifier, progress.signal_dict[sig]))
        
        if self.client_object.pbc is not None:
            self.client_object.pbc.pause()
        
        try:
            r = input(progress.ESC_BOLD + progress.ESC_LIGHT_RED+"<q> - quit, <i> - server info: " + progress.ESC_NO_CHAR_ATTR)
        except:
            r = 'q'

        if r == 'i':
            self._show_server_info()
        elif r == 'q':
            print('{}: terminate worker functions'.format(self.client_object._identifier))
            self.exit_handler._handler(sig, frame)
            print('{}: call sys.exit -> raise SystemExit'.format(self.client_object._identifier))
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
        
 
class Signal_to_terminate_process_list(object):
    """
    SIGINT and SIGTERM will call terminate for process given in process_list
    """
    def __init__(self, identifier, process_list, identifier_list, signals = [signal.SIGINT, signal.SIGTERM], verbose=0, timeout=2):
        self.identifier = identifier
        self.process_list = process_list
        self.identifier_list = identifier_list
        self.verbose = verbose
        self.timeout = timeout
        
        for s in signals:
            signal.signal(s, self._handler)
            
    def _handler(self, signal, frame):
        if self.verbose > 0:
            print("{}: received sig {} -> terminate all given subprocesses".format(self.identifier, progress.signal_dict[signal]))
        for i, p in enumerate(self.process_list):
            p.terminate()
            progress.check_process_termination(proc       = p, 
                                               identifier = self.identifier_list[i], 
                                               timeout    = self.timeout,
                                               verbose    = self.verbose,
                                               auto_kill_on_last_resort=False)


class hashDict(dict):
    def __init__(self, **kwargs):
        warnings.warn("'hashDict' is obsolete, not needed anymore, use regular 'dict' instead!")
        dict.__init__(self, **kwargs)
    
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
        warnings.warn("'hashableCopyOfNumpyArray' is obsolete, not needed anymore, use regular 'np.ndarray' instead!")
        self[:] = other[:]
     
    def __hash__(self):
        return hash(self.shape + tuple(self.flat))
 
    def __eq__(self, other):
        return np.all(np.equal(self, other))


def address_authkey_from_proxy(proxy):
    return proxy._token.address, proxy._authkey.decode()

def address_authkey_from_manager(manager):
    return manager.address, manager._authkey.decode()

def call_connect_python3(connect, dest, verbose=1, identifier='', reconnect_wait=2, reconnect_tries=3):
    identifier = mod_id(identifier)
    c = 0
    while True:
        try:                                # here we try re establish the connection
            if verbose > 1:
                print("{}try connecting to {}".format(identifier, dest))
            connect()
    
        except Exception as e:
            if verbose > 0:   
                print("{}connection to {} could not be established due to '{}'".format(identifier, dest, type(e)))
                print(traceback.format_stack()[-3].strip())             
            
            if type(e) is ConnectionResetError:           # ... when the destination hangs up on us     
                c = handler_connection_reset(identifier, dest, c, reconnect_wait, reconnect_tries, verbose)
            elif type(e) is ConnectionRefusedError:       # ... when the destination refuses our connection
                handler_connection_refused(e, identifier, dest, verbose)
            elif type(e) is AuthenticationError :      # ... when the destination refuses our connection due authkey missmatch
                handler_authentication_error(e, identifier, dest, verbose)
            elif type(e) is RemoteError:                  # ... when the destination send us an error message
                if 'KeyError' in e.args[0]:
                    handler_remote_key_error(e, verbose, dest)
                elif 'ValueError: unsupported pickle protocol:' in e.args[0]:
                    handler_remote_value_error(e, verbose, dest)
                else:
                    handler_remote_error(e, verbose, dest)
            elif type(e) is ValueError:
                handler_value_error(e, verbose)
            else:                                   # any other exception
                handler_unexpected_error(e, verbose)            
        
        else:                               # no exception
            if verbose > 1:
                print("{}connection to {} successfully established".format(identifier, dest))
            return True      

def call_connect_python2(connect, dest, verbose=1, identifier='', reconnect_wait=2, reconnect_tries=3):
    identifier = mod_id(identifier)
    c = 0
    while True:
        try:                                # here we try re establish the connection
            if verbose > 1:
                print("{}try connecting to {}".format(identifier, dest))
            connect()
        
        except Exception as e:
            if verbose > 0:
                print("{}connection to {} could not be established due to '{}'".format(identifier, dest, type(e)))
                print(traceback.format_stack()[-3].strip())
            
            if type(e) is socket.error:                    # error in socket communication
                if verbose > 0:
                    print("{} with args {}".format(type(e), e.args)) 
                err_code = e.args[0]
                if err_code == errno.ECONNRESET:     # ... when the destination hangs up on us
                    c = handler_connection_reset(identifier, dest, c, reconnect_wait, reconnect_tries, verbose)
                elif err_code == errno.ECONNREFUSED: # ... when the destination refuses our connection
                    handler_connection_refused(e, identifier, dest, verbose)
                else:
                    handler_unexpected_error(e, verbose)
            elif type(e) is AuthenticationError :       # ... when the destination refuses our connection due authkey missmatch
                handler_authentication_error(e, identifier, dest, verbose)
            elif type(e) is RemoteError:                   # ... when the destination send us an error message
                if 'KeyError' in e.args[0]:
                    handler_remote_key_error(e, verbose, dest)
                elif 'ValueError: unsupported pickle protocol:' in e.args[0]:
                    handler_remote_value_error(e, verbose, dest)
                else:
                    handler_remote_error(e, verbose, dest)
            elif type(e) is ValueError:
                handler_value_error(e, verbose)
            else:                                    # any other exception
                handler_unexpected_error(e, verbose)            
        
        else:                               # no exception
            if verbose > 1:
                print("{}connection to {} successfully established".format(identifier, dest))
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


def getCountKwargs(func):
    """ Returns a list ["count kwarg", "count_max kwarg"] for a
    given function. Valid combinations are defined in 
    `jobmanager.jobmanager.validCountKwargs`.
    
    Returns None if no keyword arguments are found.
    """
    # Get all arguments of the function
    if hasattr(func, "__code__"):
        func_args = func.__code__.co_varnames[:func.__code__.co_argcount]
        for pair in validCountKwargs:
            if ( pair[0] in func_args and pair[1] in func_args ):
                return pair
    # else
    return None
    
     
def getDateForFileName(includePID = False):
    """returns the current date-time and optionally the process id in the format
    YYYY_MM_DD_hh_mm_ss_pid
    """
    date = time.localtime()
    name = '{:d}_{:02d}_{:02d}_{:02d}_{:02d}_{:02d}'.format(date.tm_year, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min, date.tm_sec)
    if includePID:
        name += "_{}".format(os.getpid()) 
    return name

def handler_authentication_error(e, identifier, dest, verbose):
    if verbose > 1:
        print("authkey specified does not match the authkey at destination side!")
    raise e

def handler_broken_pipe_error(e, verbose):
    if verbose > 1:
        print("this usually means that an established was closed, does not exists anymore")
        print("the server probably went down")
    raise e   

def handler_connection_refused(e, identifier, dest, verbose):
    if verbose > 1:
        print("this usually means that no matching Manager object was instanciated at destination side!")
        print("either there is no Manager running at all, or it is listening to another port.")
    raise JMConnectionRefusedError(e)

def handler_connection_reset(identifier, dest, c, reconnect_wait, reconnect_tries, verbose):
    if verbose > 1:
        print("during connect this Error might be due to firewall settings\n"+
              "or other TPC connections controlling mechanisms!")
    c += 1
    if c > reconnect_tries:
        raise JMConnectionError("{}connection to {} FAILED, ".format(identifier, dest)+
                                "{} retries were NOT successfull".format(reconnect_tries))
    if verbose > 0:
        traceback.print_exc(limit=1)
        print("{}try connecting to {} again in {} seconds".format(identifier, dest, reconnect_wait))
    time.sleep(reconnect_wait)
    return c                

def handler_eof_error(e, verbose):
    if verbose > 1:
        print("This usually means that server did not replay, although the connection is still there.")
        print("This is due to the fact that the connection is in 'timewait' status for about 60s")
        print("after the server went down. After that time the connection will not exist anymore.")
    raise e

def handler_remote_error(e, verbose, dest):
    if verbose > 1:
        print("the server {} send an RemoteError message!".format(dest))        
    raise RemoteError(e.args[0])

def handler_remote_key_error(e, verbose, dest):
    if verbose > 1:
        print("'KeyError' detected in RemoteError message from server {}!".format(dest))
        print("This hints to the fact that the actual instace of the shared object on the server")
        print("side has changed, for example due to a server restart")
        print("you need to reinstanciate the proxy object")
    raise RemoteKeyError(e.args[0])
    
def handler_remote_value_error(e, verbose, dest):
    if verbose > 1:
        print("'ValueError' due to 'unsupported pickle protocol' detected in RemoteError from server {}!".format(dest))
        print("You might have tried to connect to a SERVER running with an OLDER python version")
        print("At this stage (and probably for ever) this should be avoided")        
    raise RemoteValueError(e.args[0])

def handler_value_error(e, verbose):
    if 'unsupported pickle protocol' in e.args[0]:
        if verbose > 1:
            print("'ValueError' due to 'unsupported pickle protocol'!")
            print("You might have tried to connect to a SERVER running with an NEWER python version")
            print("At this stage (and probably for ever) this should be avoided")  
    raise e

def handler_unexpected_error(e, verbose):
    raise e

def mod_id(identifier):
    if identifier != '':
        identifier = identifier.strip()
        if identifier[-1] != ':':
            identifier += ':'
        identifier += ' '
        
    return identifier

def check_if_host_is_reachable_unix_ping(adr, timeout=2, retry=5):
    for i in range(retry):
        try:
            subprocess.check_output(['ping', '-c 1', '-W {}'.format(int(timeout)), adr])
        except subprocess.CalledProcessError as e:
            # on exception, resume with loop
            continue
        else:
            # no exception, ping was successful, return without error
            return
        
    # no early return happend, ping was never successful, raise error        
    raise JMHostNotReachableError("could not reach host '{}'\nping error reads: {}".format(adr, e.output))
        

def proxy_operation_decorator_python3(proxy, operation, verbose=1, identifier='', reconnect_wait=2, reconnect_tries=3):
    identifier = mod_id(identifier)
    o = getattr(proxy, operation)
    dest = address_authkey_from_proxy(proxy)
    
    def _operation(*args, **kwargs):
        c = 0
        while True:
            check_if_host_is_reachable_unix_ping(adr=dest[0][0])
            
            if verbose > 1:
                print("{}establish connection to {}".format(identifier, dest))
            try: 
                proxy._connect()
            except Exception as e:
                if verbose > 0:
                    print("{}establishing connection to {} FAILED due to '{}'".format(identifier, dest, type(e)))
                    print(traceback.format_stack()[-3].strip())
                    print("{}wait {} seconds and retry".format(identifier, reconnect_wait))
                c += 1
                if c > reconnect_tries:
                    if verbose > 0:
                        print("{}reached maximum number of reconnect tries, raise exception".format(identifier))
                    raise e
                time.sleep(reconnect_wait)
                continue
                
            if verbose > 1:
                print("{}execute operation '{}' -> {}".format(identifier, operation, dest))
            
            try:
                res = o(*args, **kwargs)
            except Exception as e:
                if verbose > 0:
                    print("{}operation '{}' -> {} FAILED due to '{}'".format(identifier, operation, dest, type(e)))
                    print(traceback.format_stack()[-3].strip())
                    
                if type(e) is ConnectionResetError:
                    if verbose > 0:
                        traceback.print_exc(limit=1)
                        print("{}wait {} seconds and retry".format(identifier, reconnect_wait))
                    c += 1
                    if c > reconnect_tries:
                        if verbose > 0:
                            print("{}reached maximum number of reconnect tries, raise exception".format(identifier))
                        raise e
                    time.sleep(reconnect_wait)
                    continue
                elif type(e) is BrokenPipeError:
                    handler_broken_pipe_error(e, verbose)
                elif type(e) is EOFError:
                    handler_eof_error(e, verbose)
                else:
                    handler_unexpected_error(e, verbose)
            else:                               # SUCCESS -> return True  
                if verbose > 1:
                    print("{}operation '{}' successfully executed".format(identifier, operation))
                return res
        
            if verbose > 1:
                print("{}close connection to {}".format(identifier, dest))
            try:
                proxy._tls.connection.close()
            except Exception as e:
                if verbose > 0:
                    print("{}closeing connection to {} FAILED due to {}".format(identifier, dest, type(e)))
                    print(traceback.format_stack()[-3].strip())
            
    return _operation

def proxy_operation_decorator_python2(proxy, operation, verbose=1, identifier='', reconnect_wait=2, reconnect_tries=3):
    identifier = mod_id(identifier)
    o = getattr(proxy, operation)
    dest = address_authkey_from_proxy(proxy)
    
    def _operation(*args, **kwargs):
        c = 0
        while True:
            check_if_host_is_reachable_unix_ping(adr=dest[0])
            
            if verbose > 1:
                print("{}establish connection to {}".format(identifier, dest))
            try: 
                o._connect()
            except Exception as e:
                if verbose > 0:
                    print("{}establishing connection to {} FAILED due to '{}'".format(identifier, dest, type(e)))
                    print(traceback.format_stack()[-3].strip())
                    print("wait {} seconds and retry".format(reconnect_wait))
                c += 1
                if c > reconnect_tries:
                    if verbose > 0:
                        print("reached maximum number of reconnect tries, raise exception")
                    raise e
                time.sleep(reconnect_wait)
                continue
                
            if verbose > 1:
                print("{}execute operation '{}' -> {}".format(identifier, operation, dest))
            
            try:
                res = o(*args, **kwargs)
            except Exception as e:
                if verbose > 0:
                    print("{}operation '{}' -> {} FAILED due to '{}'".format(identifier, operation, dest, type(e)))
                    print(traceback.format_stack()[-3].strip())

                if type(e) is IOError:
                    if verbose > 0:
                        print("{} with args {}".format(type(e), e.args))
                    err_code = e.args[0]
                    if err_code == errno.ECONNRESET:     # ... when the destination hangs up on us
                        if verbose > 0:
                            print("wait {} seconds and retry".format(reconnect_wait))
                        c += 1
                        if c > reconnect_tries:
                            if verbose > 0:
                                print("reached maximum number of reconnect tries, raise exception")
                            raise e
                        time.sleep(reconnect_wait)
                        continue
                    else:
                        handler_unexpected_error(e, verbose) 
                elif type(e) is BrokenPipeError:
                    handler_broken_pipe_error(e, verbose)
                elif type(e) is EOFError:
                    handler_eof_error(e, verbose)
                else:
                    handler_unexpected_error(e, verbose)
            else:                               # SUCCESS -> return True  
                if verbose > 1:
                    print("{}operation '{}' successfully executed".format(identifier, operation))
                return res
        
            if verbose > 1:
                print("{}close connection to {}".format(identifier, dest))
            try:
                o._tls.connection.close()
            except Exception as e:
                if verbose > 0:
                    print("{}closeing connection to {} FAILED due to {}".format(identifier, dest, type(e)))
                    print(traceback.format_stack()[-3].strip())
            
    return _operation

# def proxy_operation_decorator_python2(proxy, operation, verbose=1, identifier='', reconnect_wait=2, reconnect_tries=3):
#     identifier = mod_id(identifier)
#     o = getattr(proxy, operation)
#     dest = address_authkey_from_proxy(proxy)
#     
#     def _operation(*args, **kwargs):
#         while True:
#             try:                                # here we try to put new data to the queue
#                 if verbose > 1:
#                     print("{}execute operation '{}' -> {}".format(identifier, operation, dest))
#                 res = o(*args, **kwargs)
#                 
#             except Exception as e:
#                 if verbose > 0:
#                     print("{}operation '{}' -> {} FAILED due to '{}'".format(identifier, operation, dest, type(e)))
#                     print(traceback.format_stack()[-3].strip())
#                     
#                 if type(e) is IOError:
#                     if verbose > 0:
#                         print("{} with args {}".format(type(e), e.args))
#                     err_code = e.args[0]
#                     if err_code == errno.ECONNRESET:     # ... when the destination hangs up on us
#                         if verbose > 0:
#                             traceback.print_exc(limit=1)
#                             print("{}try to reconnect".format(identifier))
#                         call_connect(proxy._connect, dest, verbose, identifier, reconnect_wait, reconnect_tries)
#                     else:
#                         handler_unexpected_error(e, verbose)                    
#                     
#                 elif type(e) is BrokenPipeError:
#                     handler_broken_pipe_error(e, verbose)
#                 elif type(e) is EOFError:
#                     handler_eof_error(e, verbose)
#                 else:
#                     handler_unexpected_error(e, verbose)            
#                 
#             else:                               # SUCCESS -> return True  
#                 if verbose > 1:
#                     print("{}operation '{}' successfully executed".format(identifier, operation))
#                 return res
#             
#     return _operation

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
    Signal_to_SIG_IGN(signals=[signal.SIGINT, signal.SIGTERM], verbose=0)
            

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

# keyword arguments that define counting in wrapped functions
validCountKwargs = [
                    [ "count", "count_max"],
                    [ "count", "max_count"],
                    [ "c", "m"],
                    [ "jmc", "jmm"],
                   ]

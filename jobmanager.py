import multiprocessing as mp
from multiprocessing.managers import SyncManager
import queue
import copy
import signal
import pickle
import traceback
import socket
import os
import sys
import time
import numpy as np
import progress


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

def getDateForFileName(includePID = False):
    """returns the current date-time and optionally the process id in the format
    YYYY_MM_DD_hh_mm_ss_pid
    """
    date = time.localtime()
    name = '{:d}_{:02d}_{:02d}_{:02d}_{:02d}_{:02d}'.format(date.tm_year, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min, date.tm_sec)
    if includePID:
        name += "_{}".format(os.getpid()) 
    return name



def copyQueueToList(q):
    res_list = []
    res_q = mp.Queue()
    
    try:
        while True:
            res_list.append(q.get_nowait())
            res_q.put(res_list[-1])
    except queue.Empty:
        pass

    return res_q, res_list
        
class hashDict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))
    
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
        
class Signal_to_terminate_process_list(object):
    """
    SIGINT and SIGTERM will call terminate for process given in process_list
    """
    def __init__(self, process_list, signals = [signal.SIGINT, signal.SIGTERM], verbose=0):
        self.process_list = process_list
        self.verbose = verbose
        for s in signals:
            signal.signal(s, self._handler)
    def _handler(self, signal, frame):
        if self.verbose > 0:
            print("PID {}: received sig {} -> terminate all given subprocesses".format(os.getpid(), progress.signal_dict[signal]))
        for p in self.process_list:
            p.terminate()

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
        self.authkey = bytearray(authkey, encoding='utf8')
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
        self.job_q = mp.Queue()    # queue holding args to process
        self.result_q = mp.Queue() # queue holding returned results
        self.fail_q = mp.Queue()   # queue holding args where processing failed
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
        JobQueueManager.register('get_const_arg', callable=lambda: self.const_arg, exposed=["__iter__"])
    
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
                                                                  str(authkey, encoding='utf8')))
    
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
        
       
        # do user defined final processing
        self.process_final_result()
        
        if self.fname_dump != None:
        
            if self.verbose > 0:
                print("{}: dump current state ... ".format(self._identifier), end='', flush=True)
                
            if self.fname_dump == 'auto':
                fname = "{}_{}.dump".format(self.authkey.decode('utf8'), getDateForFileName(includePID=False))
            else:
                fname = self.fname_dump
    
            with open(fname, 'wb') as f:
                self.__dump(f)
                
                if self.verbose > 0:
                    print("done!")
        else:
            if self.verbose > 0:
                print("{}: fname_dump == None, ignore dumping current state!".format(self._identifier))
        
        print("{}: JobManager_Server was successfully shout down".format(self._identifier))

    @staticmethod
    def static_load(f):
        data = {}
        data['numjobs'] = pickle.load(f)
        data['numresults'] = pickle.load(f)
        data['final_result'] = pickle.load(f)
        data['args_set'] = pickle.load(f)
        
        fail_list = pickle.load(f)
        data['fail_set'] = {fail_item[0] for fail_item in fail_list}
        
        data['fail_q'] = mp.Queue()
        data['job_q'] = mp.Queue()
        
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
        

    def read_old_state(self, fname_dump=None):
        
        if fname_dump == None:
            fname_dump = self.fname_dump
        if fname_dump == 'auto':
            raise RuntimeError("fname_dump must not be 'auto' when reading old state")
        
        if not os.path.isfile(fname_dump):
            raise RuntimeError("file '{}' to read old state from not found".format(fname_dump))

        
        with open(fname_dump, 'rb') as f:
            self.__load(f)
        self.__restart_SyncManager()
            

    def put_arg(self, a):
        """add argument a to the job_q
        """
        if (not hasattr(a, '__hash__')) or (a.__hash__ == None):
            # try to add hashability
            if isinstance(a, dict):
                a = hashDict(a)
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
            raise RuntimeError("inconsistency detected! use JobManager_Server.put_arg to put arguments to the job_q")
        
        if self.numjobs == 0:
            raise RuntimeError("no jobs to process! use JobManager_Server.put_arg to put arguments to the job_q")
        
        Signal_to_sys_exit(signals=[signal.SIGTERM, signal.SIGINT], verbose = self.verbose)
        pid = os.getpid()
        
        if self.verbose > 1:
            print("{}: start processing incoming results".format(self._identifier))
        
  
        with progress.ProgressBar(count = self._numresults,
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
                    self.process_new_result(arg, result)
                    self.numresults = self.numjobs - (len(self.args_set) - self.fail_q.qsize()) 
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
                  no_warings=False, 
                  verbose=1,
                  show_processed_jobs=True,
                  show_statusbar_for_jobs=True):
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
        
        self.show_processed_jobs = show_processed_jobs
        self.show_statusbar_for_jobs = show_statusbar_for_jobs
        self.verbose = verbose
        
        self._pid = os.getpid()
        self._identifier = progress.get_identifier(name=self.__class__.__name__, pid=self._pid) 
        if self.verbose > 1:
            print("{}: init".format(self._identifier))
       
        if no_warings:
            import warnings
            warnings.filterwarnings("ignore")
            if self.verbose > 1:
                print("{}: ignore all warnings".format(self._identifier))
        self.server = server
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

        if verbose > 0:
            print('{}: connecting to {}:{} authkey {}... '.format(identifier, server, port, authkey.decode('utf8')), end='', flush=True)
        try:
            manager.connect()
        except:
            if verbose > 0:    
                print('failed!')
            
            err, val, trb = sys.exc_info()
            print("caught exception {}: {}".format(err.__name__, val))
            
            if err == ConnectionRefusedError:
                print("check if server is up!")
            
            if verbose > 1:
                traceback.print_exception(err, val, trb)
            return None
        else:
            if verbose > 0:    
                print('done!')
            
        
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
        
        const_arg - tuple of constant argruments also provided by the JobManager_Server
        
        NOTE: This is just some dummy implementation to be used for test reasons only!
        Subclass and overwrite this function to implement your own function.  
        """
        time.sleep(0.1)
        return os.getpid()


    @staticmethod
    def __worker_func(func, nice, verbose, server, port, authkey, i, manager_objects=None):
        """
        the wrapper spawned nproc trimes calling and handling self.func
        """
        identifier = progress.get_identifier(name='worker{}'.format(i+1))
        Signal_to_sys_exit(signals=[signal.SIGTERM, signal.SIGINT])

        if manager_objects is None:        
            manager_objects = JobManager_Client._get_manager_object(server, port, authkey, identifier, verbose)
            if manager_objects == None:
                if verbose > 1:
                    print("{}: no shared object recieved, terminate!".format(identifier))
                sys.exit(1)

        job_q, result_q, fail_q, const_arg = manager_objects 
        
        n = os.nice(0)
        n = os.nice(nice - n)
        c = 0
        if verbose > 1:
            print("{}: now alive, niceness {}".format(identifier, n))
        
        time_queue = 0
        time_calc = 0
        
        while True:
            try:
                t0 = time.clock()
                arg = job_q.get(block = True, timeout = 0.1)
                t1 = time.clock()
            # regular case, just stop working when empty job_q was found
            except queue.Empty:
                if verbose > 0:
                    print("{}: finds empty job queue, processed {} jobs".format(identifier, c))
                break
            except:
                
                
                
                res = func(arg, const_arg)
                t2 = time.clock()
                result_q.put((arg, res))
                t3 = time.clock()
                c += 1
                
                time_queue += (t1-t0 + t3-t2)
                time_calc += (t2-t1)
                
            
            
            # in this context usually raised if the communication to the server fails
            except EOFError:
                if verbose > 0:
                    print("{}: EOFError, I guess the server went down, can't do anything, terminate now!".format(identifier))
                break
            
            # considered as normal exit caused by some user interaction, SIGINT, SIGTERM
            # note SIGINT, SIGTERM -> SystemExit is achieved by overwriting the
            # default signal handlers
            except SystemExit:
                if verbose > 0:
                    print("{}: SystemExit, quit processing, reinsert current argument".format(identifier))
                try:
                    if verbose > 1:
                        print("{}: put argument back to job_q ... ".format(identifier), end='', flush=True)
                    job_q.put(arg, timeout=10)
                except queue.Full:
                    if verbose > 0:
                        print("{}: failed to reinsert argument, Server down? I quit!".format(identifier))
                else:
                    if verbose > 1:
                        print("done!")
                break
            
            # some unexpected Exception
            # write argument, exception name and hostname to fail_q, save traceback
            # continue workung 
            except:
                err, val, trb = sys.exc_info()
                if verbose > 0:
                    print("{}: caught exception '{}', report failure of current argument to server ... ".format(identifier, err.__name__), end='', flush=True)
                
                hostname = socket.gethostname()
                fname = 'traceback_err_{}_{}.trb'.format(err.__name__, getDateForFileName(includePID=True))
                fail_q.put((arg, err.__name__, hostname), timeout=10)
                
                if verbose > 0:
                    print("done")
                    print("        write exception to file {} ... ".format(fname), end='', flush=True)

                with open(fname, 'w') as f:
                    traceback.print_exception(etype=err, value=val, tb=trb, file=f)
                                    
                if verbose > 0:
                    print("done")
                    print("        continue processing next argument.")
                
        if verbose > 0:
            try:
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
        for i in range(self.nproc):
            p = mp.Process(target=self.__worker_func, args=(self.func, 
                                                            self.nice, 
                                                            self.verbose, 
                                                            self.server, 
                                                            self.port,
                                                            self.authkey,
                                                            i,
                                                            self.manager_objects))
            self.procs.append(p)
            p.start()
            time.sleep(0.3)
        
        Signal_to_terminate_process_list(process_list = self.procs, verbose=self.verbose)
    
        for p in self.procs:
            p.join()

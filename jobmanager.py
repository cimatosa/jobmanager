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
import datetime
import math
import psutil
import fcntl
import termios
import struct
import collections
import numpy as np


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

# a mapping from the numeric values of the signals to their names used in the
# standard python module signals
signal_dict = {}
for s in dir(signal):
    if s.startswith('SIG') and s[3] != '_':
        n = getattr(signal, s)
        if n in signal_dict:
            signal_dict[n] += ('/'+s)
        else:
            signal_dict[n] = s

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

def humanize_time(secs):
    """convert second in to hh:mm:ss format
    """
    mins, secs = divmod(secs, 60)
    hours, mins = divmod(mins, 60)
    return '{:02d}:{:02d}:{:02d}'.format(int(hours), int(mins), int(secs))

def humanize_speed(c_per_sec):
    """convert a speed in counts per second to counts per [s, min, h, d], choosing the smallest value greater zero.
    """
    scales = [60, 60, 24]
    units = ['c/s', 'c/min', 'c/h', 'c/d']
    speed = c_per_sec
    i = 0
    if speed > 0:
        while (speed < 1) and (i < len(scales)):
            speed *= scales[i]
            i += 1
        
    return "{:.1f}{}".format(speed, units[i])

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


class SIG_handler_Loop(object):
    """class to setup signal handling for the Loop class
    
    Note: each subprocess receives the default signal handling from it's parent.
    If the signal function from the module signal is evoked within the subprocess
    this default behavior can be overwritten.
    
    The init function receives a shared memory boolean object which will be set
    false in case of signal detection. Since the Loop class will check the state 
    of this boolean object before each repetition, the loop will stop when
    a signal was receives.
    """
    def __init__(self, shared_mem_run, sigint, sigterm, identifier, verbose=0):
        self.shared_mem_run = shared_mem_run
        self.set_signal(signal.SIGINT, sigint)
        self.set_signal(signal.SIGTERM, sigterm)
        self.verbose=verbose
        self.identifier = identifier
        if self.verbose > 1:
            print("{}: setup signal handler for loop (SIGINT:{}, SIGTERM:{})".format(self.identifier, sigint, sigterm))

    def set_signal(self, sig, handler_str):
        if handler_str == 'ign':
            signal.signal(sig, self._ignore_signal)
        elif handler_str == 'stop':
            signal.signal(sig, self._stop_on_signal)
        else:
            raise TypeError("unknown signal hander string '{}'".format(handler_str))
    
    def _ignore_signal(self, signal, frame):
        pass

    def _stop_on_signal(self, signal, frame):
        if self.verbose > 0:
            print("{}: received sig {} -> set run false".format(self.identifier, signal_dict[signal]))
        self.shared_mem_run.value = False

def get_identifier(name=None, pid=None):
    if pid == None:
        pid = os.getpid()
    
    if name == None:
        return "PID {}".format(pid) 
    else:
        return "{} ({})".format(name, pid)
def check_process_termination(proc, identifier, timeout, verbose=0, auto_kill_on_last_resort = False):
    if verbose > 1:
        print("{}: give running loop at most {}s to finish ... ".format(identifier, timeout), end='', flush=True)
    
    proc.join(timeout)
    
    if not proc.is_alive():
        if verbose > 1:
            print("done")
        return True
        
    # process still runs -> send SIGTERM -> see what happens
    if verbose > 1:    
        print("failed!")
    if verbose > 0:
        print("{}: found running loop still alive -> terminate via SIGTERM ...".format(identifier), end='', flush=True)
 
    proc.terminate()
    
    proc.join(3*timeout)
    if not proc.is_alive():
        if verbose > 0:
            print("done!")
        return True
        
    if verbose > 0:
        print("failed!")

    answer = 'y' if auto_kill_on_last_resort else '_'
    while True:
        if answer == 'y':
            print("{}: send SIGKILL to".format(identifier))
            os.kill(proc.pid, signal.SIGKILL)
            time.sleep(0.1)
            answer = '_'
            
        if not proc.is_alive():
            print("{}: has stopped running!".format(identifier))
            return True
        else:
            print("{}: still running!".format(identifier))
        
        while not answer in 'yn':
            print("Do you want to send SIGKILL to '{}'? [y/n]: ".format(identifier), end='', flush=True)
            answer = sys.stdin.readline()[:-1]

        
        if answer == 'n':
            while not answer in 'yn':
                print("Do you want let the process '{}' running? [y/n]: ".format(identifier), end='', flush=True)
                answer = sys.stdin.readline()[:-1]
            if answer == 'y':
                print("{}: keeps running".format(self._identifier))
                return False
    
            

class Loop(object):
    """
    class to run a function periodically an seperate process.
    
    In case the called function returns True, the loop will stop.
    Otherwise a time interval given by interval will be slept before
    another execution is triggered.
    
    The shared memory variable _run (accessible via the class property run)  
    also determines if the function if executed another time. If set to False
    the execution stops.
    
    For safe cleanup (and in order to catch any Errors)
    it is advisable to instantiate this class
    using 'with' statement as follows:
        
        with Loop(**kwargs) as my_loop:
            my_loop.start()
            ...
    
    this will guarantee you that the spawned loop process is
    down when exiting the 'with' scope.
    
    The only circumstance where the process is still running is
    when you set auto_kill_on_last_resort to False and answer the
    question to send SIGKILL with no.
    """
    def __init__(self, 
                  func, 
                  args=(), 
                  interval = 1, 
                  verbose=0, 
                  sigint='stop', 
                  sigterm='stop',
                  name=None,
                  auto_kill_on_last_resort=False):
        """
        func [callable] - function to be called periodically
        
        args [tuple] - arguments passed to func when calling
        
        intervall [pos number] - time to "sleep" between each call
        
        verbose [pos integer] - specifies the level of verbosity
          [0--silent, 1--important information, 2--some debug info]
          
        sigint [string] - signal handler string to set SIGINT behavior (see below)
        
        sigterm [string] - signal handler string to set SIGTERM behavior (see below)
        
        name [string] - use this name in messages instead of the PID 
        
        auto_kill_on_last_resort [bool] - If set False (default), ask user to send SIGKILL 
        to loop process in case normal stop and SIGTERM failed. If set True, send SIDKILL
        without asking.
        
        the signal handler string may be one of the following
            ing: ignore the incoming signal
            stop: set the shared memory boolean to false -> prevents the loop from
            repeating -> subprocess terminates when func returns and sleep time interval 
            has passed.
        """
        self.func = func
        self.args = args
        self.interval = interval
        self._run = mp.Value('b', False)
        self.verbose = verbose
        self._proc = None
        self._sigint = sigint
        self._sigterm = sigterm
        self._name = name
        self._auto_kill_on_last_resort = auto_kill_on_last_resort
        self._identifier = None
 
    def __enter__(self):
        return self
    
    def __exit__(self, *exc_args):
        # normal exit        
        if not self.is_alive():
            if self.verbose > 1:
                print("{}: has stopped on context exit".format(self._identifier))
            return
        
        # loop still runs on context exit -> __cleanup
        if self.verbose > 1:
            print("{}: is still running on context exit".format(self._identifier))
        self.__cleanup()
        

    def __cleanup(self):
        """
        Wait at most twice as long as the given repetition interval
        for the _wrapper_function to terminate.
        
        If after that time the _wrapper_function has not terminated,
        send SIGTERM to and the process.
        
        Wait at most five times as long as the given repetition interval
        for the _wrapper_function to terminate.
        
        If the process still running send SIGKILL automatically if
        auto_kill_on_last_resort was set True or ask the
        user to confirm sending SIGKILL
        """
        # set run to False and wait some time -> see what happens            
        self.run = False
        if check_process_termination(proc=self._proc, 
                                     identifier=self._identifier, 
                                     timeout=2*self.interval,
                                     verbose=self.verbose,
                                     auto_kill_on_last_resort=self._auto_kill_on_last_resort):
            if self.verbose > 1:
                print("{}: cleanup successful".format(self._identifier))
            self._proc = None


    @staticmethod
    def _wrapper_func(func, args, shared_mem_run, interval, verbose, sigint, sigterm, name):
        """to be executed as a seperate process (that's why this functions is declared static)
        """
        # implement the process specific signal handler
        identifier = get_identifier(name)
        SIG_handler_Loop(shared_mem_run, sigint, sigterm, identifier, verbose)

        while shared_mem_run.value:
            try:
                quit_loop = func(*args)
            except:
                err, val, trb = sys.exc_info()
                if verbose > 0:
                    print("{}: error {} occurred in Loop class calling 'func(*args)'".format(identifier, err))
                if verbose > 0:
                    traceback.print_tb(trb)
                return

            if quit_loop:
                return
            time.sleep(interval)
            
        if verbose > 1:
            print("{}: _wrapper_func terminates gracefully".format(identifier))

    def start(self):
        """
        uses multiprocess Process to call _wrapper_func in subprocess 
        """

        if self.is_alive():
            if self.verbose > 0:
                print("{}: is already running".format(self._identifier))
            return
            
        self.run = True
        self._proc = mp.Process(target = Loop._wrapper_func, 
                                args = (self.func, self.args, self._run, self.interval, 
                                        self.verbose, self._sigint, self._sigterm, self._name),
                                name=self._name)
        self._proc.start()
        self._identifier = get_identifier(self._name, self.getpid())
        if self.verbose > 1:
            print("{}: started as new loop process".format(self._identifier))
        
    def stop(self):
        """
        stops the process triggered by start
        
        Setting the shared memory boolean run to false, which should prevent
        the loop from repeating. Call __cleanup to make sure the process
        stopped. After that we could trigger start() again.
        """
        print("stop loop")
        self.run = False
        if not self.is_alive():
            if self.verbose > 0:
                print("PID None: there is no running loop to stop")
            return
        
        self.__cleanup()
        
    def join(self, timeout):
        """
        calls join for the spawned process with given timeout
        """
        if self.is_alive():
            return psutil.Process(self._pid).wait(timeout)

            
    def getpid(self):
        """
        return the process id of the spawned process
        """
        return self._proc.pid
    
    def is_alive(self):
        if self._proc == None:
            return False
        else:
            return self._proc.is_alive()
        
    
    @property
    def run(self):
        """
        makes the shared memory boolean accessible as class attribute
        
        Set run False, the loop will stop repeating.
        Calling start, will set run True and start the loop again as a new process.
        """
        return self._run.value
    @run.setter
    def run(self, run):
        self._run.value = run

def UnsignedIntValue(val=0):
    return mp.Value('I', val, lock=True)        
       
class StatusBar(Loop):
    """
    status bar in ascii art
    
    Uses Loop class to implement repeating function which shows the process
    based of the two shared memory values max_count and count. max_count is
    assumed to be the final state, whereas count represents the current state.
    
    The estimates time of arrival (ETA) will be calculated from a speed measurement
    given by the average over the last spee_calc_cycles calls of the looping
    function show_stat.
    """
    def __init__(self, 
                  count, 
                  max_count, 
                  interval=1, 
                  speed_calc_cycles=10, 
                  width='auto',
                  verbose=0,
                  sigint='stop', 
                  sigterm='stop',
                  name='statusbar',
                  prepend=None):
        """
        The init will also start to display the status bar if run was set True.
        Otherwise use the inherited method start() to start the show_stat loop.
        stop() will stop to show the status bar.
        
        count [mp.Value] - shared memory to hold the current state
        
        max_count [mp.Value] - shared memory holding the final state, may change
        by external process without having to explicitly tell this class
        
        interval [int] - seconds to wait between progress print
        
        speed_calc_cycles [int] - use the current (time, count) as
        well as the (old_time, old_count) read by the show_stat function 
        speed_calc_cycles calls before to calculate the speed as follows:
        s = count - old_count / (time - old_time)
        
        width [int/'auto'] - the number of characters used to show the status bar,
        use 'auto' to determine width from terminal information -> see _set_width 
        
        verbose, sigint, sigterm -> see loop class  
        """
        
        assert isinstance(count, mp.sharedctypes.Synchronized)
        assert isinstance(max_count, mp.sharedctypes.Synchronized)
        
        self.start_time = mp.Value('d', time.time())
        self.speed_calc_cycles = speed_calc_cycles
        self.q = mp.Queue()         # queue to save the last speed_calc_cycles
                                    # (time, count) information to calculate speed
        self.max_count = max_count  # multiprocessing value type
        self.count = count          # multiprocessing value type
        self.interval = interval
        self.verbose = verbose
        self.name = name
        if prepend is None:
            self.prepend = ''
        else:
            self.prepend = prepend
        self._set_width(width)
        
        
        # setup loop class
        super().__init__(func=StatusBar.show_stat, 
                         args=(self.count, 
                               self.start_time, 
                               self.max_count, 
                               self.width, 
                               self.speed_calc_cycles, 
                               self.q,
                               self.prepend), 
                         interval=interval, 
                         verbose=verbose, 
                         sigint=sigint, 
                         sigterm=sigterm, 
                         name=name,
                         auto_kill_on_last_resort=True)

    def __exit__(self, *exc_args):
        super().__exit__(*exc_args)
        StatusBar.show_stat(count=self.max_count,
                            start_time=self.start_time, 
                            max_count=self.max_count, 
                            width=self.width, 
                            speed_calc_cycles=self.speed_calc_cycles,
                            q=self.q,
                            prepend=self.prepend)
        print()
        
    def _set_width(self, width):
        """
        set the number of characters to be used to disply the status bar
        
        is set to 'auto' try to determine the width of the terminal used
        (experimental, depends on the terminal used, os dependens)
        use a width of 80 as fall back.
        """
        if width == 'auto':
            try:
                hw = struct.unpack('hh', fcntl.ioctl(sys.stdin, termios.TIOCGWINSZ, '1234'))
                self.width = hw[1]
            except:
                if self.verbose > 0:
                    print("{}: failed to determine the width of the terminal".format(get_identifier(name=self.name)))
                self.width = 80
        else:
            self.width = width
        
    def set_args(self, interval=1, speed_calc_cycles=10, width='auto', prepend='', start=False):
        """
        change some of the init arguments
        
        This will stop the loop, set changes and start the loop again.
        """
        self.stop()
        self.interval = interval
        self.speed_calc_cycles = speed_calc_cycles
        self._set_width(width)
        self.prepend = prepend
        
        self.args = (self.count, self.start_time, self.max_count, 
                     self.width, self.speed_calc_cycles, self.q, self.prepend)
        if start:
            self.start()

    @staticmethod        
    def show_stat(count, start_time, max_count, width, speed_calc_cycles, q, prepend):
        """
        the actual routine to bring the status to the screen
        """
        count_value = count.value
        max_count_value = max_count.value
        if count_value == 0:
            start_time.value = time.time()
            print("\rwait for first count ...", end='', flush=True)
            return False
        else:
            current_time = time.time()
            start_time_value = start_time.value
            q.put((count_value, current_time))
            
            if q.qsize() > speed_calc_cycles:
                old_count_value, old_time = q.get()
            else:
                old_count_value, old_time = 0, start_time_value

            tet = (current_time - start_time_value)
            speed = (count_value - old_count_value) / (current_time - old_time)
            if speed == 0:
                s3 = "] ETA --"
            else:
                eta = math.ceil((max_count_value - count_value) / speed)
                s3 = "] ETA {}".format(humanize_time(eta))

            
            s1 = "\r{}{} [{}] [".format(prepend, humanize_time(tet), humanize_speed(speed))
            
            l = len(s1) + len(s3)
            l2 = width - l - 1
            
            a = int(l2 * count_value / max_count_value)
            b = l2 - a
            s2 = "="*a + ">" + " "*b
            print(s1+s2+s3, end='', flush=True)
            return count_value >= max_count_value
        
    def stop(self):
        print()
        super().stop()
        
    def reset(self):
        super().stop()
        self.count.value=0
        while not self.q.empty():
            self.q.get()
        super().start()
        
class StatusBarMulti(StatusBar):
    def __init__(self, 
                  count, 
                  max_count, 
                  interval=1, 
                  speed_calc_cycles=10, 
                  width='auto',
                  verbose=0,
                  sigint='stop', 
                  sigterm='stop',
                  name='statusbarmulti',
                  prepend=None):
        
        self.num_sb = len(count) 
        assert self.num_sb == len(max_count)
        for c in count:
            assert isinstance(c, mp.sharedctypes.Synchronized)
        for m in max_count:
            assert isinstance(m, mp.sharedctypes.Synchronized)
        
        self.start_time = mp.Value('d', time.time())
        self.speed_calc_cycles = speed_calc_cycles
        
        self.q = []
        for i in range(self.num_sb):
            self.q.append(mp.Queue()) # queue to save the last speed_calc_cycles
                                      # (time, count) information to calculate speed
        self.max_count = max_count  # multiprocessing value type
        self.count = count          # multiprocessing value type
        self.interval = interval
        self.verbose = verbose
        self.name = name
        if prepend is None:
            self.prepend = [''] * self.num_sb
        else:
            assert self.num_sb == len(prepend)
            self.prepend = prepend
        self._set_width(width)
        
        # setup loop class
        Loop.__init__(self,
                      func=StatusBarMulti.show_stat_multi, 
                      args=(self.count, 
                            self.start_time, 
                            self.max_count, 
                            self.width, 
                            self.speed_calc_cycles, 
                            self.q,
                            self.prepend), 
                      interval=interval, 
                      verbose=verbose, 
                      sigint=sigint, 
                      sigterm=sigterm, 
                      name=name,
                      auto_kill_on_last_resort=True)
        
    @staticmethod
    def show_stat_multi(count, start_time, max_count, width, speed_calc_cycles, q, prepend):
        n = len(count)
        for i in range(n):
            StatusBar.show_stat(count[i], start_time, max_count[i], width, speed_calc_cycles, q[i], prepend[i])
            print()
        print("\033[{}A".format(n+1))
        
    def _reset(self, i):
        self.count[i].value=0
        while not self.q[i].empty():
            self.q[i].get()
                
    def reset(self, i):
        Loop.stop(self)
        self._reset(i)
        Loop.stop(self)
        
    def reset_all(self):
        Loop.stop(self)
        for i in range(self.num_sb):
            self._reset(i)
        Loop.start(self)
        

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
            print("PID {}: received signal {} -> will be ignored".format(os.getpid(), signal_dict[sig]))


        
class Signal_to_sys_exit(object):
    def __init__(self, signals=[signal.SIGINT, signal.SIGTERM], verbose=0):
        self.verbose = verbose
        for s in signals:
            signal.signal(s, self._handler)
    def _handler(self, signal, frame):
        if self.verbose > 0:
            print("PID {}: received signal {} -> call sys.exit -> raise SystemExit".format(os.getpid(), signal_dict[signal]))
        sys.exit('exit due to signal {}'.format(signal_dict[signal]))
        
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
            print("PID {}: received sig {} -> terminate all given subprocesses".format(os.getpid(), signal_dict[signal]))
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
        self._identifier = get_identifier(name=self.__class__.__name__, pid=self._pid)
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
        manager_identifier = get_identifier(name='SyncManager')
        
        # stop SyncManager
        self.manager.shutdown()
        
        check_process_termination(proc=manager_proc, 
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
            print("{}: started on {}:{} with authkey '{}'".format(get_identifier('SyncManager', self.manager._process.pid), 
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
        
  
        with StatusBar(count = self._numresults, max_count = self._numjobs, 
                       interval=self.msg_interval, speed_calc_cycles=self.speed_calc_cycles,
                       verbose = self.verbose, sigint='ign', sigterm='ign') as stat:
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
                  verbose=1):
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
        
       
        self.verbose = verbose
        self._pid = os.getpid()
        self._identifier = get_identifier(name=self.__class__.__name__, pid=self._pid) 
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
        identifier = get_identifier(name='worker{}'.format(i+1))
        Signal_to_sys_exit(signals=[signal.SIGTERM, signal.SIGINT])

        if manager_objects is None:        
            manager_objects = JobManager_Client._get_manager_object(server, port, authkey, identifier, verbose)
            if res == None:
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
                res = func(arg, const_arg)
                t2 = time.clock()
                result_q.put((arg, res))
                t3 = time.clock()
                c += 1
                
                time_queue += (t1-t0 + t3-t2)
                time_calc += (t2-t1)
                
            # regular case, just stop woring when empty job_q was found
            except queue.Empty:
                if verbose > 0:
                    print("{}: finds empty job queue, processed {} jobs".format(identifier, c))
                break
            
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

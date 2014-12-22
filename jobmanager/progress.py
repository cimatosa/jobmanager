#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import multiprocessing as mp
import time
import traceback
import signal
import sys
import os
import math
import subprocess
import copy

if sys.version_info[0] == 2:
    # Python 2
    import Queue as queue
else:
    # Python 3
    import queue

myQueue = mp.Queue

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

def UnsignedIntValue(val=0):
    return mp.Value('I', val, lock=True)

def FloatValue(val=0.):
    return mp.Value('d', val, lock=True)

def printQueue(q, lock=None):
    if lock is not None:
        lock.acquire()
    
    res = []
    for i in range(q.qsize()):
        item = q.get()
        res.append(copy.deepcopy(item[0]))
        q.put(item)
    
    if lock is not None:
        lock.release()
        
    print(res)
        

def check_process_termination(proc, identifier, timeout, verbose=0, auto_kill_on_last_resort = False):
    
    proc.join(timeout)
    
    if not proc.is_alive():
        if verbose > 1:
            print("{}: loop termination within given timeout of {}s SUCCEEDED!".format(identifier, timeout))
        return True
        
    # process still runs -> send SIGTERM -> see what happens
    if verbose > 0:
        print("{}: loop termination within given timeout of {}s FAILED!".format(identifier, timeout))
 
    proc.terminate()

    new_timeout = 3*timeout     
    proc.join(new_timeout)
    if not proc.is_alive():
        if verbose > 0:
            print("{}: loop termination via SIGTERM with timeout of {}s SUCCEEDED!".format(identifier, new_timeout))
        return True
        
    if verbose > 0:
        print("{}: loop termination via SIGTERM with timeout of {}s FAILED!".format(identifier, new_timeout))

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
            print("Do you want to send SIGKILL to '{}'? [y/n]: ".format(identifier), end='')
            sys.stdout.flush()
            answer = sys.stdin.readline()[:-1]

        
        if answer == 'n':
            while not answer in 'yn':
                print("Do you want let the process '{}' running? [y/n]: ".format(identifier), end='')
                sys.stdout.flush()
                answer = sys.stdin.readline()[:-1]
            if answer == 'y':
                print("{}: keeps running".format(identifier))
                return False
            
def get_identifier(name=None, pid=None):
    if pid == None:
        pid = os.getpid()
    
    if name == None:
        return "PID {}".format(pid) 
    else:
        return "{} ({})".format(name, pid)
    
def get_terminal_width(default=80, name=None, verbose=0):
    identifier = get_identifier(name=name)
    try:
        out = subprocess.check_output(["tput", "cols"])
        width = int(out.decode("utf-8").strip())
        if verbose > 1:
            print("{}: determined terminal width to {}".format(identifier, width))
        return width
    except Exception as e:
        if verbose > 0:
            print("{}: failed to determine the width of the terminal".format(identifier))
        if verbose > 1:
            if isinstance(e, subprocess.CalledProcessError):
                print("calling 'tput cols' returned: {}".format(e.output.decode('utf-8')))
            traceback.print_exc() 
        return default

    
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
        self._run   = mp.Value('b', False)
        self._pause = mp.Value('b', False)
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
    def _wrapper_func(func, args, shared_mem_run, shared_mem_pause, interval, verbose, sigint, sigterm, name):
        """to be executed as a seperate process (that's why this functions is declared static)
        """
        # implement the process specific signal handler
        identifier = get_identifier(name)
        SIG_handler_Loop(shared_mem_run, sigint, sigterm, identifier, verbose)

        while shared_mem_run.value:
            # in pause mode, simply sleep 
            if shared_mem_pause.value:
                quit_loop = False
            else:
                # if not pause mode -> call func and see what happens
                try:
                    quit_loop = func(*args)
                except:
                    err, val, trb = sys.exc_info()
                    print('\033[0m', end='')
                    sys.stdout.flush()
                    if verbose > 0:
                        print("{}: error {} occurred in Loop class calling 'func(*args)'".format(identifier, err))
                        traceback.print_exc()
                    return

                if quit_loop is True:
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
                                args = (self.func, self.args, self._run, self._pause, self.interval, 
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
            self._proc.join(timeout)

            
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
        
    def pause(self):
        if not self.run:
            if self.verbose > 0:
                print("{} is not running -> can not pause".format(self._identifier))
        
        if self._pause.value == True:
            if self.verbose > 1:
                print("{} is already in pause mode!".format(self._identifier))
        self._pause.value = True
        
    def resume(self):
        if not self.run:
            if self.verbose > 0:
                print("{} is not running -> can not resume".format(self._identifier))
        
        if self._pause.value == False:
            if self.verbose > 1:
                print("{} is not in pause mode -> can not resume!".format(self._identifier))
                
        self._pause.value = False
    
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

class Progress(Loop):
    """
    Abstract Progress Loop
    
    Uses Loop class to implement a repeating function to display the progress
    of multiples processes.
    
    In the simplest case, given only a list of shared memory values 'count' (NOTE: 
    a single value will be automatically mapped to a one element list),
    the total elapses time (TET) and a speed estimation are calculated for
    each process and passed to the display function show_stat.
    
    This functions needs to be implemented in a subclass. It is intended to show
    the progress of ONE process in one line.
    
    When max_count is given (in general as list of shared memory values, a single
    shared memory value will be mapped to a one element list) the estimates time 
    of arrival ETA will also be calculated and passed tow show_stat.
    
    Information about the terminal width will be retrieved when setting width='auto'.
    If supported by the terminal emulator the width in characters of the terminal
    emulator will be stored in width and also passed to show_stat. 
    Otherwise, a default width of 80 characters will be chosen.
    
    Also you can specify a fixed width by setting width to the desired number.
    
    NOTE: in order to achieve multiline progress special escape sequences are used
    which may not be supported by all terminal emulators.
    
    example:
       
        c1 = UnsignedIntValue(val=0)
        c2 = UnsignedIntValue(val=0)
    
        c = [c1, c2]
        prepend = ['c1: ', 'c2: ']
        with ProgressCounter(count=c, prepend=prepend) as sc:
            sc.start()
            while True:
                i = np.random.randint(0,2)
                with c[i].get_lock():
                    c[i].value += 1
                    
                if c[0].value == 1000:
                    break
                
                time.sleep(0.01)
    
    using start() within the 'with' statement ensures that the subprocess
    which is started by start() in order to show the progress repeatedly
    will be terminated on context exit. Otherwise one has to make sure
    that at some point the stop() routine is called. When dealing with 
    signal handling and exception this might become tricky, so that the  
    use of the 'with' statement is highly encouraged. 
    """    
    def __init__(self, 
                  count, 
                  max_count=None,
                  prepend=None,
                  width='auto',
                  speed_calc_cycles=10, 
                  interval=1, 
                  verbose=0,
                  sigint='stop', 
                  sigterm='stop',
                  name='progress'):
        """       
        count [mp.Value] - shared memory to hold the current state, (list or single value)
        
        max_count [mp.Value] - shared memory holding the final state, (None, list or single value),
        may be changed by external process without having to explicitly tell this class.
        If None, no ETA and relative progress can be calculated -> ETA = None 
        
        prepend [string] - string to put in front of the progress output, (None, single string
        or of list of strings)
        
        interval [int] - seconds to wait between progress print
        
        speed_calc_cycles [int] - use the current (time, count) as
        well as the (old_time, old_count) read by the show_stat function 
        speed_calc_cycles calls before to calculate the speed as follows:
        s = count - old_count / (time - old_time)
        
        verbose, sigint, sigterm -> see loop class  
        """
        
        try:
            for c in count:
                assert isinstance(c, mp.sharedctypes.Synchronized), "each element of 'count' must be if the type multiprocessing.sharedctypes.Synchronized"
            self.is_multi = True
        except TypeError:
            assert isinstance(count, mp.sharedctypes.Synchronized), "'count' must be if the type multiprocessing.sharedctypes.Synchronized"
            self.is_multi = False
            count = [count]
        
        self.len = len(count)
            
        if max_count is not None:
            if self.is_multi:
                try:
                    for m in max_count:
                        assert isinstance(m, mp.sharedctypes.Synchronized), "each element of 'max_count' must be if the type multiprocessing.sharedctypes.Synchronized"
                except TypeError:
                    raise TypeError("'max_count' must be iterable")
            else:
                assert isinstance(max_count, mp.sharedctypes.Synchronized), "'max_count' must be if the type multiprocessing.sharedctypes.Synchronized"
                max_count = [max_count]
        else:
            max_count = [None] * self.len
            
        
        self.start_time = []
        self.speed_calc_cycles = speed_calc_cycles
        
        if width == 'auto':
            self.width = get_terminal_width(default=80, name=name, verbose=verbose)
        else:
            self.width = width
        
        self.q = []
        self.prepend = []
        self.lock = []
        self.last_count = []
        self.last_old_count = []
        self.last_old_time = []
        for i in range(self.len):
            self.q.append(myQueue())  # queue to save the last speed_calc_cycles
                                      # (time, count) information to calculate speed
            self.last_count.append(UnsignedIntValue())
            self.last_old_count.append(UnsignedIntValue())
            self.last_old_time.append(FloatValue())
            self.lock.append(mp.Lock())
            self.start_time.append(FloatValue(val=time.time()))
            if prepend is None:
                # no prepend given
                self.prepend.append('')
            else:
                try:
                    # assume list of prepend 
                    self.prepend.append(prepend[i])
                except:
                    # list fails -> assume single prepend for all 
                    self.prepend.append(prepend)
                                                       
        self.max_count = max_count  # list of multiprocessing value type
        self.count = count          # list of multiprocessing value type
        
        self.interval = interval
        self.verbose = verbose
        self.name = name
        
        self.add_args = {}
            
        # setup loop class
        super(Progress, self).__init__(func=Progress.show_stat_wrapper_multi, 
                         args=(self.count,
                               self.last_count, 
                               self.start_time,
                               self.max_count, 
                               self.speed_calc_cycles,
                               self.width, 
                               self.q,
                               self.last_old_count,
                               self.last_old_time,
                               self.prepend,
                               self.__class__.show_stat,
                               self.len,
                               self.add_args,
                               self.lock), 
                         interval=interval, 
                         verbose=verbose, 
                         sigint=sigint, 
                         sigterm=sigterm, 
                         name=name,
                         auto_kill_on_last_resort=True)
        
    def __exit__(self, *exc_args):
        """
            will terminate loop process
            
            show a last progress -> see the full 100% on exit
        """
        super(Progress, self).__exit__(*exc_args)
        self._show_stat()
        print('\n'*(self.len-1))
        
    def _show_stat(self):
        """
            convenient functions to call the static show_stat_wrapper_multi with
            the given class members
        """
        Progress.show_stat_wrapper_multi(self.count,
                                         self.last_count, 
                                         self.start_time, 
                                         self.max_count, 
                                         self.speed_calc_cycles,
                                         self.width,
                                         self.q,
                                         self.last_old_count,
                                         self.last_old_time,
                                         self.prepend,
                                         self.__class__.show_stat,
                                         self.len, 
                                         self.add_args,
                                         self.lock)
    @staticmethod
    def show_stat_wrapper_multi(count, 
                                last_count, 
                                start_time, 
                                max_count, 
                                speed_calc_cycles, 
                                width, 
                                q, 
                                last_old_count,
                                last_old_time,
                                prepend, 
                                show_stat_function, 
                                len_, 
                                add_args,
                                lock):
        """
            call the static method show_stat_wrapper for each process
        """
        print('\033[1;32m', end='')
        sys.stdout.flush()
        for i in range(len_):
            Progress.show_stat_wrapper(count[i], 
                                       last_count[i], 
                                       start_time[i], 
                                       max_count[i], 
                                       speed_calc_cycles, 
                                       width, 
                                       q[i],
                                       last_old_count[i],
                                       last_old_time[i],
                                       prepend[i], 
                                       show_stat_function, 
                                       add_args, 
                                       i, 
                                       lock[i])
        print("\033[{}A\033[0m".format(len_), end='')
        sys.stdout.flush()
        
    @staticmethod
    def _calc(count, 
              last_count, 
              start_time, 
              max_count, 
              speed_calc_cycles, 
              q, 
              last_old_count,
              last_old_time,
              lock):
        """
            do the pre calculations in order to get TET, speed, ETA
            and call the actual display routine show_stat with these arguments
            
            NOTE: show_stat is purely abstract and need to be reimplemented to
            achieve a specific progress display.  
        """
        
        count_value = count.value
        start_time_value = start_time.value
        current_time = time.time()
        
        if last_count.value != count_value:
            # some progress happened
        
            with lock:
                # save current state (count, time) to queue
                
                q.put((count_value, current_time))
    
                # get older state from queue (or initial state)
                # to to speed estimation                
                if q.qsize() > speed_calc_cycles:
                    old_count_value, old_time = q.get()
                else:
                    old_count_value, old_time = 0, start_time_value
            
            last_count.value = count_value
            last_old_count.value = old_count_value
            last_old_time.value = old_time 
        else:
            # progress has not changed since last call
            # use also old (cached) data from the queue
            old_count_value, old_time = last_old_count.value, last_old_time.value

        if (max_count is None):
            max_count_value = None
        else:
            max_count_value = max_count.value
            
        tet = (current_time - start_time_value)
        speed = (count_value - old_count_value) / (current_time - old_time)
        if (speed == 0) or (max_count_value is None) or (max_count_value == 0):
            eta = None
        else:
            eta = math.ceil((max_count_value - count_value) / speed)
            
        return count_value, max_count_value, speed, tet, eta
        
    @staticmethod        
    def show_stat_wrapper(count, 
                          last_count, 
                          start_time, 
                          max_count, 
                          speed_calc_cycles, 
                          width, 
                          q,
                          last_old_count,
                          last_old_time, 
                          prepend, 
                          show_stat_function,
                          add_args, 
                          i, 
                          lock):
        count_value, max_count_value, speed, tet, eta, = Progress._calc(count, 
                                                                        last_count, 
                                                                        start_time, 
                                                                        max_count, 
                                                                        speed_calc_cycles, 
                                                                        q,
                                                                        last_old_count,
                                                                        last_old_time, 
                                                                        lock) 
        return show_stat_function(count_value, max_count_value, prepend, speed, tet, eta, width, i, **add_args)
    
    @staticmethod        
    def show_stat(count_value, max_count_value, prepend, speed, tet, eta, width, **kwargs):
        """
            re implement this function in a subclass
            
            count_value - current value of progress
            
            max_count_value - maximum value the progress can take
            
            prepend - some extra string to be put for example in front of the
            progress display
            
            speed - estimated speed in counts per second (use for example humanize_speed
            to get readable information in string format)
            
            tet - total elapsed time in seconds (use for example humanize_time
            to get readable information in string format)
            
            eta - estimated time of arrival in seconds (use for example humanize_time
            to get readable information in string format)
        """
        raise NotImplementedError
        
    def stop(self):
        """
            trigger clean up by hand, needs to be done when not using
            context management via 'with' statement
        """
        super(Progress, self).stop()
        self._show_stat()
        print('\n'*(self.len-1))
        
    def _reset_all(self):
        """
            reset all progress information
        """
        for i in range(self.len):
            self._reset_i(i)
            
    def _reset_i(self, i):
        """
            reset i-th progress information
        """
        self.count[i].value=0
        self.lock[i].acquire()
        for x in range(self.q[i].qsize()):
            self.q[i].get()
        
        self.lock[i].release()
        self.start_time[i].value = time.time()

        
    def reset(self, i = None):
        """
            convenient function to reset progress information
            
            i [None, int] - None: reset all, int: reset process indexed by i 
        """
#        super(Progress, self).stop()
        if i is None:
            self._reset_all()
        else:
            self._reset_i(i)
#        super(Progress, self).start()
                   
class ProgressBar(Progress):
    """
    Implements a Progress bar (progress par) similar to the one known from 'wget'
    or 'pv'
    
    
    
    """
    def __init__(self, 
                  count, 
                  max_count=None,
                  width='auto',
                  prepend=None,
                  speed_calc_cycles=10, 
                  interval=1, 
                  verbose=0,
                  sigint='stop', 
                  sigterm='stop',
                  name='progress_bar'):
        """
            width [int/'auto'] - the number of characters used to show the Progress bar,
            use 'auto' to determine width from terminal information -> see _set_width
        """
        super(ProgressBar, self).__init__(count=count,
                         max_count=max_count,
                         prepend=prepend,
                         speed_calc_cycles=speed_calc_cycles,
                         width=width,
                         interval=interval,
                         verbose = verbose,
                         sigint=sigint,
                         sigterm=sigterm,
                         name=name)

            
    @staticmethod        
    def show_stat(count_value, max_count_value, prepend, speed, tet, eta, width, i, **kwargs):
        if max_count_value is None:
            # only show current absolute progress as number and estimated speed
            print("{}{} [{}] #{}    ".format(prepend, humanize_time(tet), humanize_speed(speed), count_value))             
        else:
            # deduce relative progress and show as bar on screen
            if eta is None:
                s3 = "] ETA --"
            else:
                s3 = "] ETA {}".format(humanize_time(eta))
               
            s1 = "{}{} [{}] [".format(prepend, humanize_time(tet), humanize_speed(speed))
            
            l = len(s1) + len(s3)
            l2 = width - l - 1
            
            a = int(l2 * count_value / max_count_value)
            b = l2 - a
            s2 = "="*a + ">" + " "*b
            print(s1+s2+s3)
        
# class ProgressCounter(Progress):
#     """
#         simple Progress counter, not using the max_count information
#     """
#     def __init__(self, 
#                  count, 
#                  max_count=None,
#                  prepend=None,
#                  speed_calc_cycles=10,
#                  width='auto', 
#                  interval=1, 
#                  verbose=0,
#                  sigint='stop', 
#                  sigterm='stop',
#                  name='progress_counter'):
#         
#         super(ProgressCounter, self).__init__(count=count,
#                          max_count=max_count,
#                          prepend=prepend,
#                          speed_calc_cycles=speed_calc_cycles,
#                          width=width,
#                          interval=interval,
#                          verbose = verbose,
#                          sigint=sigint,
#                          sigterm=sigterm,
#                          name=name)
#         
#     @staticmethod        
#     def show_stat(count_value, max_count_value, prepend, speed, tet, eta, width, i, **kwargs):
#         if max_count_value is not None:
#             max_count_str = "/{}".format(max_count_value)
#         else:
#             max_count_value = count_value + 1 
#             max_count_str = ""
#             
#         s = "{}{} [{}{}] ({})".format(prepend, humanize_time(tet), count_value, max_count_str, humanize_speed(speed))
#         print(s)
        
class ProgressBarCounter(Progress):
    """
        records also the time of each reset and calculates the speed
        of the resets.
        
        shows the TET since init (not effected by reset)
        the speed of the resets (number of finished processed per time)
        and the number of finished processes
        
        after that also show a progress of each process
        max_count > 0 and not None -> bar
        max_count == None -> absolute count statistic
        max_count == 0 -> hide process statistic at all 
    """
    def __init__(self, 
                 count, 
                 max_count=None,
                 prepend=None,
                 speed_calc_cycles_bar=10,
                 speed_calc_cycles_counter=5,
                 width='auto', 
                 interval=1, 
                 verbose=0,
                 sigint='stop', 
                 sigterm='stop',
                 name='progress_bar_counter'):
        
        super(ProgressBarCounter, self).__init__(count=count,
                         max_count=max_count,
                         prepend=prepend,
                         speed_calc_cycles=speed_calc_cycles_bar,
                         width=width,
                         interval=interval,
                         verbose = verbose,
                         sigint=sigint,
                         sigterm=sigterm,
                         name=name)
        
        self.counter_count = []
        self.counter_q = []
        self.counter_speed = []
        for i in range(self.len):
            self.counter_count.append(UnsignedIntValue(val=0))
            self.counter_q.append(myQueue())
            self.counter_speed.append(FloatValue())
        
        self.counter_speed_calc_cycles = speed_calc_cycles_counter
        self.init_time = time.time()
            
        self.add_args['counter_count'] = self.counter_count
        self.add_args['counter_speed'] = self.counter_speed
        self.add_args['init_time'] = self.init_time

    def get_counter_count(self, i=0):
        return self.counter_count[i].value
        
    def _reset_i(self, i):
        c = self.counter_count[i] 
        with c.get_lock():
            c.value += 1
            
        count_value = c.value
        q = self.counter_q[i]
         
        current_time = time.time()
        q.put((count_value, current_time))
        
        if q.qsize() > self.counter_speed_calc_cycles:
            old_count_value, old_time = q.get()
        else:
            old_count_value, old_time = 0, self.init_time

        speed = (count_value - old_count_value) / (current_time - old_time)
        
        self.counter_speed[i].value = speed
                    
        super(ProgressBarCounter, self)._reset_i(i)
        
    @staticmethod
    def show_stat(count_value, max_count_value, prepend, speed, tet, eta, width, i, **kwargs):
        counter_count = kwargs['counter_count'][i]
        counter_speed = kwargs['counter_speed'][i]
        counter_tet = time.time() - kwargs['init_time']
        
        s_c = "{}{} [{}] #{}".format(prepend,
                                    humanize_time(counter_tet),
                                    humanize_speed(counter_speed.value), 
                                    counter_count.value)
        if max_count_value != 0:
            s_c += ' - '
        
            if max_count_value is None:
                s_c = "{}{}{} [{}] #{}    ".format(s_c, prepend, humanize_time(tet), humanize_speed(speed), count_value)            
            else:
                if eta is None:
                    s3 = "] ETA --"
                else:
                    s3 = "] ETA {}".format(humanize_time(eta))
                   
                s1 = "{} [{}] [".format(humanize_time(tet), humanize_speed(speed))
                
                l = len(s1) + len(s3) + len(s_c)
                l2 = width - l - 1
                
                a = int(l2 * count_value / max_count_value)
                b = l2 - a
                s2 = "="*a + ">" + " "*b
                s_c = s_c+s1+s2+s3
                
        print(s_c + ' '*(width - len(s_c)))
            
class ProgressSilentDummy(Progress):
    def __init__(self, **kwargs):
        pass

    def __exit__(self, *exc_args):
        pass
    
    def start(self):
        pass
    
    def _reset_i(self, i):
        pass
    
    def reset(self, i):
        pass
    
    def _reset_all(self):
        pass
        
    def stop(self):
        pass
    
    def pause(self):
        pass
    
    def resume(self):
        pass

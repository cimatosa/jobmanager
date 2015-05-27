#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import copy
import datetime
import math
import multiprocessing as mp
import signal
import subprocess as sp
import sys
import time
import traceback
import os
import warnings

try:
    from shutil import get_terminal_size as shutil_get_terminal_size
except ImportError:
    shutil_get_terminal_size = None
    
if sys.version_info[0] == 2:
    old_math_ceil = math.ceil 
    def my_int_ceil(f):
        return int(old_math_ceil(f))
    
    math.ceil = my_int_ceil


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
        
        if not hasattr(self, '_identifier'):
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
            self._identifier = get_identifier(self._name, 'not started')
        else:
            raise RuntimeError("{}: cleanup FAILED!".format(self._identifier))
            

    @staticmethod
    def _wrapper_func(func, args, shared_mem_run, shared_mem_pause, interval, verbose, sigint, sigterm, name):
        """to be executed as a separate process (that's why this functions is declared static)
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
                    print(ESC_NO_CHAR_ATTR, end='')
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
    shared memory value will be mapped to a one element list) the time to go TTG
    will also be calculated and passed tow show_stat.
    
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
        If None, no TTG and relative progress can be calculated -> TTG = None 
        
        prepend [string] - string to put in front of the progress output, (None, single string
        or of list of strings)
        
        interval [int] - seconds to wait between progress print
        
        speed_calc_cycles [int] - use the current (time, count) as
        well as the (old_time, old_count) read by the show_stat function 
        speed_calc_cycles calls before to calculate the speed as follows:
        s = count - old_count / (time - old_time)
        
        verbose, sigint, sigterm -> see loop class  
        """
        self.name = name
        self._identifier = get_identifier(self.name, pid='not started')
        
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
                    # assume list of prepend, (needs to be a sequence)
                    # except if prepend is an instance of string
                    # the assert will cause the except to be executed
                    assert not isinstance(prepend, str)
                    self.prepend.append(prepend[i])
                except:
                    # list fails -> assume single prepend for all 
                    self.prepend.append(prepend)
                                                       
        self.max_count = max_count  # list of multiprocessing value type
        self.count = count          # list of multiprocessing value type
        
        self.interval = interval
        self.verbose = verbose

        self.show_on_exit = False
        self.add_args = {}
        
        # setup loop class with func
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
        self.stop()
            
        
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
            do the pre calculations in order to get TET, speed, TTG
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
            ttg = None
        else:
            ttg = math.ceil((max_count_value - count_value) / speed)
            
        return count_value, max_count_value, speed, tet, ttg

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

    @staticmethod        
    def show_stat(count_value, max_count_value, prepend, speed, tet, ttg, width, **kwargs):
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
            
            ttg - time to go in seconds (use for example humanize_time
            to get readable information in string format)
        """
        raise NotImplementedError

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
        count_value, max_count_value, speed, tet, ttg, = Progress._calc(count, 
                                                                        last_count, 
                                                                        start_time, 
                                                                        max_count, 
                                                                        speed_calc_cycles, 
                                                                        q,
                                                                        last_old_count,
                                                                        last_old_time, 
                                                                        lock) 
        return show_stat_function(count_value, max_count_value, prepend, speed, tet, ttg, width, i, **add_args)

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
#         print(ESC_BOLD, end='')
#         sys.stdout.flush()
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
        print(ESC_MOVE_LINE_UP(len_) + ESC_NO_CHAR_ATTR, end='')
        sys.stdout.flush()

    def start(self):
        # before printing any output to stout, we can now check this
        # variable to see if any other ProgressBar has reserved that
        # terminal.
        
        if (self.__class__.__name__ in TERMINAL_PRINT_LOOP_CLASSES):
            if not terminal_reserve(progress_obj=self, verbose=self.verbose, identifier=self._identifier):
                if self.verbose > 1:
                    print("{}: tty already reserved, NOT starting the progress loop!".format(self._identifier))
                return
        
        super(Progress, self).start()
        self.show_on_exit = True

    def stop(self, make_sure_its_down = False):
        """
            trigger clean up by hand, needs to be done when not using
            context management via 'with' statement
        
            - will terminate loop process
            - show a last progress -> see the full 100% on exit
            - releases terminal reservation
        """
        self._auto_kill_on_last_resort = make_sure_its_down 
            
        super(Progress, self).stop()
        terminal_unreserve(progress_obj=self, verbose=self.verbose, identifier=self._identifier)

        if self.show_on_exit:
            self._show_stat()
            print('\n'*(self.len-1))
        self.show_on_exit = False
        

class ProgressBar(Progress):
    """
    implements a progress bar similar to the one known from 'wget' or 'pv'
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

        self._PRE_PREPEND = ESC_NO_CHAR_ATTR + ESC_RED
        self._POST_PREPEND = ESC_BOLD + ESC_GREEN

    @staticmethod        
    def show_stat(count_value, max_count_value, prepend, speed, tet, ttg, width, i, **kwargs):
        if max_count_value is None:
            # only show current absolute progress as number and estimated speed
            print("{}{}{}{} [{}] #{}    ".format(ESC_NO_CHAR_ATTR + ESC_RED,
                                                 prepend, 
                                                 ESC_BOLD + ESC_GREEN,
                                                 humanize_time(tet), humanize_speed(speed), count_value))             
        else:
            if width == 'auto':
                width = get_terminal_width()
            
            # deduce relative progress and show as bar on screen
            if ttg is None:
                s3 = "] TTG --"
            else:
                s3 = "] TTG {}".format(humanize_time(ttg))
               
            s1 = "{}{}{}{} [{}] [".format(ESC_NO_CHAR_ATTR + ESC_RED,
                                          prepend, 
                                          ESC_BOLD + ESC_GREEN,
                                          humanize_time(tet),
                                          humanize_speed(speed))
            
            l = len_string_without_ESC(s1+s3)
            
            if max_count_value != 0:
                l2 = width - l - 1
                a = int(l2 * count_value / max_count_value)
                b = l2 - a
                s2 = "="*a + ">" + " "*b
            else:
                s2 = " "*(width - l)

            print(s1+s2+s3)


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
    def show_stat(count_value, max_count_value, prepend, speed, tet, ttg, width, i, **kwargs):
        counter_count = kwargs['counter_count'][i]
        counter_speed = kwargs['counter_speed'][i]
        counter_tet = time.time() - kwargs['init_time']
        
        s_c = "{}{}{}{} [{}] #{}".format(ESC_NO_CHAR_ATTR + ESC_RED,
                                     prepend, 
                                     ESC_BOLD + ESC_GREEN,
                                     humanize_time(counter_tet),
                                     humanize_speed(counter_speed.value), 
                                     counter_count.value)

        if width == 'auto':
            width = get_terminal_width()        
        
        if max_count_value != 0:
            s_c += ' - '
        
            if max_count_value is None:
                s_c = "{}{} [{}] #{}    ".format(s_c, humanize_time(tet), humanize_speed(speed), count_value)            
            else:
                if ttg is None:
                    s3 = "] TTG --"
                else:
                    s3 = "] TTG {}".format(humanize_time(ttg))
                   
                s1 = "{} [{}] [".format(humanize_time(tet), humanize_speed(speed))
                
                l = len_string_without_ESC(s1 + s3 + s_c)
                l2 = width - l - 1
                
                a = int(l2 * count_value / max_count_value)
                b = l2 - a
                s2 = "="*a + ">" + " "*b
                s_c = s_c+s1+s2+s3

        print(s_c + ' '*(width - len_string_without_ESC(s_c)))

class ProgressBarFancy(Progress):
    """
        implements a progress bar where the color indicates the current status
        similar to the bars known from 'htop'
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
        if not self.__class__.__name__ in TERMINAL_PRINT_LOOP_CLASSES:
            TERMINAL_PRINT_LOOP_CLASSES.append(self.__class__.__name__)
            
        super(ProgressBarFancy, self).__init__(count=count,
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
    def get_d(s1, s2, width, lp, lps):
        d = width-len(remove_ESC_SEQ_from_string(s1))-len(remove_ESC_SEQ_from_string(s2))-2-lp-lps
        if d >= 0:
            d1 = d // 2
            d2 = d - d1
            return s1, s2, d1, d2

    @staticmethod
    def full_stat(p, tet, speed, ttg, eta, ort, repl_ch, width, lp, lps):
        s1 = "TET {} {:>12} TTG {}".format(tet, speed, ttg)
        s2 = "ETA {} ORT {}".format(eta, ort)
        return ProgressBarFancy.get_d(s1, s2, width, lp, lps)


    @staticmethod
    def full_minor_stat(p, tet, speed, ttg, eta, ort, repl_ch, width, lp, lps):
        s1 = "E {} {:>12} G {}".format(tet, speed, ttg)
        s2 = "A {} O {}".format(eta, ort)
        return ProgressBarFancy.get_d(s1, s2, width, lp, lps)

    @staticmethod
    def reduced_1_stat(p, tet, speed, ttg, eta, ort, repl_ch, width, lp, lps):
        s1 = "E {} {:>12} G {}".format(tet, speed, ttg)
        s2 = "O {}".format(ort)
        return ProgressBarFancy.get_d(s1, s2, width, lp, lps)  

    @staticmethod
    def reduced_2_stat(p, tet, speed, ttg, eta, ort, repl_ch, width, lp, lps):
        s1 = "E {} G {}".format(tet, ttg)
        s2 = "O {}".format(ort)
        return ProgressBarFancy.get_d(s1, s2, width, lp, lps)
    
    @staticmethod
    def reduced_3_stat(p, tet, speed, ttg, eta, ort, repl_ch, width, lp, lps):
        s1 = "E {} G {}".format(tet, ttg)
        s2 = ''
        return ProgressBarFancy.get_d(s1, s2, width, lp, lps)
    
    @staticmethod
    def reduced_4_stat(p, tet, speed, ttg, eta, ort, repl_ch, width, lp, lps):
        s1 = ''
        s2 = ''
        return ProgressBarFancy.get_d(s1, s2, width, lp, lps)    

    @staticmethod        
    def kw_bold(s, ch_after):
        kws = ['TET', 'TTG', 'ETA', 'ORT', 'E', 'G', 'A', 'O']
        for kw in kws:
            for c in ch_after:
                s = s.replace(kw + c, ESC_BOLD + kw + ESC_RESET_BOLD + c)
            
        return s

    @staticmethod        
    def _stat(count_value, max_count_value, prepend, speed, tet, ttg, width, i, **kwargs):
        if max_count_value is None:
            # only show current absolute progress as number and estimated speed
            stat = "{}{} [{}] #{}    ".format(prepend, humanize_time(tet), humanize_speed(speed), count_value) 
        else:
            if width == 'auto':
                width = get_terminal_width()
            # deduce relative progress
            p = count_value / max_count_value
            if p < 1:
                ps = " {:.1%} ".format(p)
            else:
                ps = " {:.0%} ".format(p)
            
            if ttg is None:
                eta = '--'
                ort = None
            else:
                eta = datetime.datetime.fromtimestamp(time.time() + ttg).strftime("%Y%m%d_%H:%M:%S")
                ort = tet + ttg
                
            tet = humanize_time(tet)
            speed = '['+humanize_speed(speed)+']'
            ttg = humanize_time(ttg)
            ort = humanize_time(ort)
            repl_ch = '-'
            lp = len(prepend)
            
            args = p, tet, speed, ttg, eta, ort, repl_ch, width, lp, len(ps)
            
            res = ProgressBarFancy.full_stat(*args)
            if res is None:
                res = ProgressBarFancy.full_minor_stat(*args)
                if res is None:
                    res = ProgressBarFancy.reduced_1_stat(*args)
                    if res is None:
                        res = ProgressBarFancy.reduced_2_stat(*args)
                        if res is None:
                            res = ProgressBarFancy.reduced_3_stat(*args)
                            if res is None:
                                res = ProgressBarFancy.reduced_4_stat(*args)
                                    
            if res is not None:
                s1, s2, d1, d2 = res                
                s = s1 + ' '*d1 + ps + ' '*d2 + s2

                s_before = s[:math.ceil(width*p)].replace(' ', repl_ch)
                if (len(s_before) > 0) and (s_before[-1] == repl_ch):
                    s_before = s_before[:-1] + '>'
                s_after  = s[math.ceil(width*p):]
                
                s_before = ProgressBarFancy.kw_bold(s_before, ch_after=[repl_ch, '>'])
                s_after = ProgressBarFancy.kw_bold(s_after, ch_after=[' '])
                stat = prepend + ESC_BOLD + '[' + ESC_RESET_BOLD + ESC_LIGHT_GREEN + s_before + ESC_DEFAULT + s_after + ESC_BOLD + ']' + ESC_NO_CHAR_ATTR
            else:
                ps = ps.strip()
                if p == 1:
                    ps = ' '+ps
                stat = prepend + ps
        
        return stat

    @staticmethod        
    def show_stat(count_value, max_count_value, prepend, speed, tet, ttg, width, i, **kwargs):
        stat = ProgressBarFancy._stat(count_value, max_count_value, prepend, speed, tet, ttg, width, i, **kwargs)
        print(stat)

class ProgressBarCounterFancy(ProgressBarCounter):
    @staticmethod
    def show_stat(count_value, max_count_value, prepend, speed, tet, ttg, width, i, **kwargs):
        counter_count = kwargs['counter_count'][i]
        counter_speed = kwargs['counter_speed'][i]
        counter_tet = time.time() - kwargs['init_time']
        
        s_c = "{}{}{}{} {:>12} #{}".format(ESC_RED,
                                         prepend, 
                                         ESC_NO_CHAR_ATTR,
                                         humanize_time(counter_tet),
                                         '['+humanize_speed(counter_speed.value)+']', 
                                         counter_count.value)

        if width == 'auto':
            width = get_terminal_width()        
        
        if max_count_value != 0:
            s_c += ' '
        
            if max_count_value is None:
                s_c = "{}{} {:>12} #{}    ".format(s_c, humanize_time(tet), '['+humanize_speed(speed)+']', count_value)            
            else:
                _width = width - len_string_without_ESC(s_c)
                s_c += ProgressBarFancy._stat(count_value, max_count_value, '', speed, tet, ttg, _width, i)

        print(s_c + ' '*(width - len_string_without_ESC(s_c))) 
                        

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
#     def show_stat(count_value, max_count_value, prepend, speed, tet, ttg, width, i, **kwargs):
#         if max_count_value is not None:
#             max_count_str = "/{}".format(max_count_value)
#         else:
#             max_count_value = count_value + 1 
#             max_count_str = ""
#             
#         s = "{}{} [{}{}] ({})".format(prepend, humanize_time(tet), count_value, max_count_str, humanize_speed(speed))
#         print(s)


def ESC_MOVE_LINE_UP(n):
    return "\033[{}A".format(n)


def ESC_MOVE_LINE_DOWN(n):
    return "\033[{}B".format(n)


def FloatValue(val=0.):
    return mp.Value('d', val, lock=True)


def UnsignedIntValue(val=0):
    return mp.Value('I', val, lock=True)


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
            
        if not proc.is_alive():
            print("{}: has stopped running!".format(identifier))
            return True
        else:
            print("{}: still running!".format(identifier))

        answer = '_'        
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


def get_identifier(name=None, pid=None, bold=True):
    if pid == None:
        pid = os.getpid()
        
    if bold:
        esc_bold = ESC_BOLD
        esc_no_char_attr = ESC_NO_CHAR_ATTR
    else:
        esc_bold = ""
        esc_no_char_attr = ""
    
    if name == None:
        return "{}PID {}{}".format(esc_bold, pid, esc_no_char_attr) 
    else:
        return "{}{} ({}){}".format(esc_bold, name, pid, esc_no_char_attr)


def get_terminal_size(defaultw=80):
    """ Checks various methods to determine the terminal size
    
    
    Methods:
    - shutil.get_terminal_size (only Python3)
    - fcntl.ioctl
    - subprocess.check_output
    - os.environ
    
    Parameters
    ----------
    defaultw : int
        Default width of terminal.
    
    
    Returns
    -------
    width, height : int
        Width and height of the terminal. If one of them could not be
        found, None is return in its place.
    
    """
    if hasattr(shutil_get_terminal_size, "__call__"):
        return shutil_get_terminal_size()
    else:
        try:
            import fcntl, termios, struct
            fd = 0
            hw = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
                                                                '1234'))
            return (hw[1], hw[0])
        except:
            try:
                out = sp.check_output(["tput", "cols"])
                width = int(out.decode("utf-8").strip())
                return (width, None)
            except:
                try:
                    hw = (os.environ['LINES'], os.environ['COLUMNS'])
                    return (hw[1], hw[0])
                except:
                    return (defaultw, None)

    
def get_terminal_width(default=80, name=None, verbose=0):
    id = get_identifier(name=name)
    try:
        width = get_terminal_size(defaultw=default)[0]
    except:
        width = default
    if verbose > 1:
        print("{}: use terminal width {}".format(id, width))
    return width


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


def humanize_time(secs):
    """convert second in to hh:mm:ss format
    """
    if secs is None:
        return '--'
    
    mins, secs = divmod(secs, 60)
    hours, mins = divmod(mins, 60)
    return '{:02d}:{:02d}:{:02d}'.format(int(hours), int(mins), int(secs))
    


def len_string_without_ESC(s):
    return len(remove_ESC_SEQ_from_string(s))


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


def remove_ESC_SEQ_from_string(s):
    for esc_seq in ESC_SEQ_SET:
        s = s.replace(esc_seq, '')
    return s


def terminal_reserve(progress_obj, terminal_obj=None, verbose=0, identifier=None):
    """ Registers the terminal (stdout) for printing.
    
    Useful to prevent multiple processes from writing progress bars
    to stdout.
    
    One process (server) prints to stdout and a couple of subprocesses
    do not print to the same stdout, because the server has reserved it.
    Of course, the clients have to be nice and check with 
    terminal_reserve first if they should (not) print.
    Nothing is locked.
    
    Returns
    -------
    True if reservation was successful (or if we have already reserved this tty),
    False if there already is a reservation from another instance.
    """
    if terminal_obj is None:
        terminal_obj = sys.stdout
    
    if identifier is None:
        identifier = ''
    else:
        identifier = identifier + ': ' 
    
    if terminal_obj in TERMINAL_RESERVATION:    # terminal was already registered
        if verbose > 1:
            print("{}this terminal {} has already been added to reservation list".format(identifier, terminal_obj))
        
        if TERMINAL_RESERVATION[terminal_obj] is progress_obj:
            if verbose > 1:
                print("{}we {} have already reserved this terminal {}".format(identifier, progress_obj, terminal_obj))
            return True
        else:
            if verbose > 1:
                print("{}someone else {} has already reserved this terminal {}".format(identifier, TERMINAL_RESERVATION[terminal_obj], terminal_obj))
            return False
    else:                                       # terminal not yet registered
        if verbose > 1:
            print("{}terminal {} was reserved for us {}".format(identifier, terminal_obj, progress_obj))
        TERMINAL_RESERVATION[terminal_obj] = progress_obj
        return True


def terminal_unreserve(progress_obj, terminal_obj=None, verbose=0, identifier=None):
    """ Unregisters the terminal (stdout) for printing.
    
    an instance (progress_obj) can only unreserve the tty (terminal_obj) when it also reserved it
    
    see terminal_reserved for more information
    
    Returns
    -------
    None
    """
    
    if terminal_obj is None:
        terminal_obj =sys.stdout

    if identifier is None:
        identifier = ''
    else:
        identifier = identifier + ': '         
    
    po = TERMINAL_RESERVATION.get(terminal_obj)
    if po is None:
        if verbose > 1:
            print("{}terminal {} was not reserved, nothing happens".format(identifier, terminal_obj))
    else:
        if po is progress_obj:
            if verbose > 1:
                print("{}terminal {} now unreserned".format(identifier, terminal_obj))
            del TERMINAL_RESERVATION[terminal_obj]
        else:
            if verbose > 1:
                print("{}you {} can NOT unreserve terminal {} be cause it was reserved by {}".format(identifier, progress_obj, terminal_obj, po))


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

ESC_NO_CHAR_ATTR  = "\033[0m"

ESC_BOLD          = "\033[1m"
ESC_DIM           = "\033[2m"
ESC_UNDERLINED    = "\033[4m"
ESC_BLINK         = "\033[5m"
ESC_INVERTED      = "\033[7m"
ESC_HIDDEN        = "\033[8m"

# not widely supported, use '22' instead 
# ESC_RESET_BOLD       = "\033[21m"

ESC_RESET_DIM        = "\033[22m"
ESC_RESET_BOLD       = ESC_RESET_DIM

ESC_RESET_UNDERLINED = "\033[24m"
ESC_RESET_BLINK      = "\033[25m"
ESC_RESET_INVERTED   = "\033[27m"
ESC_RESET_HIDDEN     = "\033[28m"

ESC_DEFAULT       = "\033[39m"
ESC_BLACK         = "\033[30m"
ESC_RED           = "\033[31m"
ESC_GREEN         = "\033[32m"
ESC_YELLOW        = "\033[33m"
ESC_BLUE          = "\033[34m"
ESC_MAGENTA       = "\033[35m"
ESC_CYAN          = "\033[36m"
ESC_LIGHT_GREY    = "\033[37m"
ESC_DARK_GREY     = "\033[90m"
ESC_LIGHT_RED     = "\033[91m"
ESC_LIGHT_GREEN   = "\033[92m"
ESC_LIGHT_YELLOW  = "\033[93m"
ESC_LIGHT_BLUE    = "\033[94m"
ESC_LIGHT_MAGENTA = "\033[95m"
ESC_LIGHT_CYAN    = "\033[96m"
ESC_WHITE         = "\033[97m"

ESC_SEQ_SET = [ESC_NO_CHAR_ATTR,
               ESC_BOLD,
               ESC_DIM,
               ESC_UNDERLINED,
               ESC_BLINK,
               ESC_INVERTED,
               ESC_HIDDEN,
               ESC_RESET_BOLD,
               ESC_RESET_DIM,
               ESC_RESET_UNDERLINED,
               ESC_RESET_BLINK,
               ESC_RESET_INVERTED,
               ESC_RESET_HIDDEN,
               ESC_DEFAULT,
               ESC_BLACK,
               ESC_RED,
               ESC_GREEN,
               ESC_YELLOW,
               ESC_BLUE,
               ESC_MAGENTA,
               ESC_CYAN,
               ESC_LIGHT_GREY,
               ESC_DARK_GREY,
               ESC_LIGHT_RED,
               ESC_LIGHT_GREEN,
               ESC_LIGHT_YELLOW,
               ESC_LIGHT_BLUE,
               ESC_LIGHT_MAGENTA,
               ESC_LIGHT_CYAN,
               ESC_WHITE]

# terminal reservation list, see terminal_reserve
TERMINAL_RESERVATION = {}
# these are classes that print progress bars, see terminal_reserve
TERMINAL_PRINT_LOOP_CLASSES = ["ProgressBar", "ProgressBarCounter"]

"""jobmanager module

Richard Hartmann 2014-2018


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
import copy
from datetime import datetime
import multiprocessing as mp
from multiprocessing.managers import BaseManager, RemoteError
import subprocess
import os
import pickle
import signal
import socket
import sys

import random
import time
import traceback
import warnings

import threadpoolctl
import binfootprint as bf
import pathlib
import progression as progress
import shelve
import hashlib
import logging
import threading
import ctypes
from shutil import rmtree
from .signalDelay import sig_delay

log = logging.getLogger(__name__)

# This is a list of all python objects that will be imported upon
# initialization during module import (see __init__.py)
__all__ = [
    "log",
    "JobManager_Client",
    "JobManager_Local",
    "JobManager_Server",
    "getDateForFileName",
]


import queue

JMConnectionError = ConnectionError
JMConnectionRefusedError = ConnectionRefusedError
JMConnectionResetError = ConnectionResetError

input_promt = input


class JMHostNotReachableError(JMConnectionError):
    pass


myQueue = queue.Queue
AuthenticationError = mp.AuthenticationError


def humanize_size(size_in_bytes):
    thr = 999
    scales = [1024, 1024, 1024]
    units = ["k", "M", "G", "T"]
    i = 0
    while (size_in_bytes > thr) and (i < len(scales)):
        size_in_bytes = size_in_bytes / scales[i]
        i += 1
    return "{:.4g}{}B".format(size_in_bytes, units[i])


def get_user():
    out = subprocess.check_output("id -un", shell=True).decode().strip()
    return out


def get_user_process_limit():
    out = subprocess.check_output("ulimit -u", shell=True).decode().strip()
    return int(out)


def get_user_num_process():
    out = subprocess.check_output("ps ut | wc -l", shell=True).decode().strip()
    return int(out) - 2


def set_mkl_threads(n):
    raise DeprecationWarning(
        "use threadpoolclt.threadtool_limit context manager instead"
    )

    if n == 0:
        # print("MKL threads not set!")
        pass
    else:
        noMKL = noOB = False
        try:
            mkl_rt = ctypes.CDLL("libmkl_rt.so")
            mkl_rt.MKL_Set_Num_Threads(n)
            print("MKL threads set to", mkl_rt.mkl_get_max_threads())
        except OSError as e:
            print(e)
            noMKL = True

        try:
            openblas = ctypes.CDLL("libopenblas.so")
            openblas = ctypes.CDLL(
                "/home/cima/.local/lib/python3.9/site-packages/scipy.libs/libopenblasp-r0-9f9f5dbc.3.18.so"
            )
            openblas.openblas_set_num_threads(n)
            print("openblas threads set to", openblas.openblas_get_num_threads())
        except OSError as e:
            print(e)
            noOB = True

        if noMKL and noOB:
            warnings.warn("num_threads could not be set, MKL / openblas not found")
    print()


class ServerQueueManager(BaseManager):
    pass


ServerQueueManager.register("get_job_q")
ServerQueueManager.register("get_result_q")
ServerQueueManager.register("get_fail_q")
ServerQueueManager.register("get_const_arg")


def parse_nproc(nproc):
    # nproc specifies n explicitly
    if nproc >= 1:
        n = int(nproc)
    # nproc specifies n by telling how many core should be left unused
    elif nproc <= 0:
        n = int(mp.cpu_count() + nproc)
        if n <= 0:
            raise RuntimeError(
                "Invalid Number of Processes\ncan not spawn {} processes (cores found: {}, cores NOT to use: {} = -nproc)".format(
                    n, mp.cpu_count(), abs(nproc)
                )
            )
    # nproc specifies n as the fraction of all cores available, however, at least one
    else:
        n = max(int(nproc * mp.cpu_count()), 1)

    return n


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

    def __init__(
        self,
        server,
        authkey,
        port=42524,
        nproc=0,
        njobs=0,
        nice=19,
        no_warnings=False,
        verbose=None,
        show_statusbar_for_jobs=True,
        show_counter_only=False,
        interval=0.3,
        emergency_dump_path=".",
        # job_q_get_timeout       = 1,
        # job_q_put_timeout       = 10,
        # result_q_put_timeout   = 300,
        # fail_q_put_timeout      = 10,
        reconnect_wait=2,
        reconnect_tries=3,
        ping_timeout=2,
        ping_retry=3,
        hide_progress=False,
        use_special_SIG_INT_handler=True,
        timeout=None,
        log_level=logging.WARNING,
        ask_on_sigterm=True,
        nthreads=1,
        status_output_for_srun=False,
        emtpy_lines_at_end=0,
        use_exclusive_cpu_per_process=True,
        cpu_start_index=0,
    ):
        """
        server [string] - ip address or hostname where the JobManager_Server is running

        authkey [string] - authentication key used by the SyncManager.
        Server and Client must have the same authkey.

        port [int] - network port to use

        nproc [integer] - number of subprocesses to start

            positive integer: number of processes to spawn

            zero: number of spawned processes == number cpu cores

            negative integer: number of spawned processes == number cpu cores - |nproc|

            between 0 and 1: ... fraction

        nthreads, MLK threads for each subprocess

        njobs [integer] - total number of jobs to run per process

            negative integer or zero: run until there are no more jobs

            positive integer: run only njobs number of jobs per nproc
                              The total number of jobs this client will
                              run is njobs*nproc.

        nice [integer] - niceness of the subprocesses

        no_warnings [bool] - call warnings.filterwarnings("ignore") -> all warnings are ignored

        verbose [int] - 0: quiet, 1: status only, 2: debug messages

        timeout [int] - second until client stops automaticaly, if negative, do not start at all

        use_exclusive_cpu_per_process - bind a worker to a specific (logical) CPU
        cpu_start_index - offset for the CPU ID to use for the workers, may be a string starting with 'x'
                          to calculate dynamically as multiple of the number of processes to use,
                          e.g. if we have 16 log. cores, and we have set '-np 0.5' which means we start 8
                          workers. If we then set '-e x1' we have 8 as offset, i.e., the workers will use the
                          cores 8 - 15 (useful if you start various instances of clients in parallel)
                          To obtain a useful CPU ID, the calculated value as allways taken modulo the number
                          of cores.

        DO NOT SIGTERM CLIENT TOO EARLY, MAKE SURE THAT ALL SIGNAL HANDLERS ARE UP (see log at debug level)
        """

        global log
        log = logging.getLogger(__name__ + "." + self.__class__.__name__)
        log.setLevel(log_level)
        # print("this client has logger:", log)

        self._pid = os.getpid()
        self._sid = os.getsid(self._pid)

        self.init_time = time.time()

        if verbose is not None:
            log.warning("\nverbose is deprecated, only allowed for compatibility")
            warnings.warn("verbose is deprecated", DeprecationWarning)

        self.hide_progress = hide_progress
        self.use_special_SIG_INT_handler = use_special_SIG_INT_handler

        log.info("init JobManager Client instance (pid %s)", os.getpid())

        self.show_statusbar_for_jobs = show_statusbar_for_jobs
        log.debug("show_statusbar_for_jobs:%s", self.show_statusbar_for_jobs)
        self.show_counter_only = show_counter_only
        log.debug("show_counter_only:%s", self.show_counter_only)
        self.interval = interval
        log.debug("interval:%s", self.interval)

        # self._job_q_get_timeout = job_q_get_timeout
        # log.debug("_job_q_get_timeout:%s", self._job_q_get_timeout)
        # self._job_q_put_timeout = job_q_put_timeout
        # log.debug("_job_q_put_timeout:%s", self._job_q_put_timeout)
        # self._result_q_put_timeout = result_q_put_timeout
        # log.debug("_result_q_put_timeout:%s", self._result_q_put_timeout)
        # self._fail_q_put_timeout = fail_q_put_timeout
        # log.debug("_fail_q_put_timeout:%s", self._fail_q_put_timeout)
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
            self.authkey = bytearray(authkey, encoding="utf8")
        log.debug("authkey:%s", self.authkey.decode())
        self.port = port
        log.debug("port:%s", self.port)
        self.nice = nice
        log.debug("nice:%s", self.nice)
        self.nproc = parse_nproc(nproc)
        log.debug("nproc:%s", self.nproc)

        self.nthreads = nthreads

        if njobs == 0:  # internally, njobs must be negative for infinite jobs
            njobs -= 1
        self.njobs = njobs
        log.debug("njobs:%s", self.njobs)
        self.emergency_dump_path = emergency_dump_path
        log.debug("emergency_dump_path:%s", self.emergency_dump_path)

        self.pbc = None

        self.procs = []
        self.manager_objects = None  # will be set via connect()

        self.timeout = timeout
        if (self.timeout is not None) and (self.timeout < 0):
            log.warning("negative timeout! client will not start")

        self.ask_on_sigterm = ask_on_sigterm
        self.status_output_for_srun = status_output_for_srun
        self.emtpy_lines_at_end = emtpy_lines_at_end
        self.use_exclusive_cpu_per_process = use_exclusive_cpu_per_process
        if isinstance(cpu_start_index, str) and cpu_start_index.startswith('x'):
            self.cpu_start_index = self.nproc * int(cpu_start_index[1:])
        else:
            self.cpu_start_index = int(cpu_start_index)



    def connect(self):
        if self.manager_objects is None:
            try:
                self.manager_objects = self.create_manager_objects()
            except Exception as e:
                log.critical(
                    "creating manager objects failed due to {}".format(type(e))
                )
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

        manager = ServerQueueManager(
            address=(self.server, self.port), authkey=self.authkey
        )

        try:
            call_connect(
                connect=manager.connect,
                dest=address_authkey_from_manager(manager),
                reconnect_wait=self.reconnect_wait,
                reconnect_tries=self.reconnect_tries,
            )
        except:
            log.warning(
                "FAILED to connect to %s", address_authkey_from_manager(manager)
            )
            log.info(traceback.format_exc())
            return None

        job_q = manager.get_job_q()
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
        # print("{} sleeps for {}s".format(pid, const_arg))
        time.sleep(const_arg)
        return pid

    @staticmethod
    def __worker_func(
        func,
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
        #                      job_q_get_timeout,
        host,
        port,
        authkey,
        nthreads,
        nproc,
    ):
        """
        the wrapper spawned nproc times calling and handling self.func
        """
        global log
        log = logging.getLogger(__name__ + ".worker{}".format(i + 1))
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
        time_queue = 0.0
        time_calc = 0.0

        # check for func definition without status members count, max_count
        # args_of_func = inspect.getfullargspec(func).args
        # if len(args_of_func) == 2:
        count_args = progress.getCountKwargs(func)

        if count_args is None:
            log.info(
                "found function without status information (progress will not work)"
            )
            m.value = 0  # setting max_count to -1 will hide the progress bar
            _func = lambda arg, const_arg, c, m: func(arg, const_arg)
        elif count_args != ["c", "m"]:
            log.debug("found counter keyword arguments: %s", count_args)
            # Allow other arguments, such as ["jmc", "jmm"] as defined
            # in `validCountKwargs`.
            # Here we translate to "c" and "m".
            def _func(arg, const_arg, c, m):
                kwargs = {count_args[0]: c, count_args[1]: m}
                return func(arg, const_arg, **kwargs)

        else:
            log.debug("found standard keyword arguments: [c, m]")
            _func = func

        log.info("set mkl threads to {}".format(nthreads))
        with threadpoolctl.threadpool_limits(limits=nthreads, user_api="blas"):
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
                    tg_0 = time.time()
                    try:
                        log.debug("wait until local result q is almost empty")
                        while local_result_q.qsize() > nproc:
                            time.sleep(0.5)
                        log.debug("done waiting, call job_q_get")

                        with sig_delay([signal.SIGTERM]):
                            arg = job_q_get()
                        log.debug("process {}".format(arg))

                    # regular case, just stop working when empty job_q was found
                    except queue.Empty:
                        log.info("finds empty job queue, processed %s jobs", cnt)
                        break
                    except ContainerClosedError:
                        log.info("job queue was closed, processed %s jobs", cnt)
                        break

                    # handle SystemExit in outer try ... except
                    except SystemExit as e:
                        arg = None
                        log.warning("getting arg from job_q failed due to SystemExit")
                        raise e
                    # job_q.get failed -> server down?
                    except Exception as e:
                        arg = None
                        log.error("Error when calling 'job_q_get'")
                        handle_unexpected_queue_error(e)
                        break
                    tg_1 = time.perf_counter()
                    time_queue += tg_1 - tg_0

                    # try to process the retrieved argument
                    try:
                        tf_0 = time.perf_counter()
                        log.debug("START crunching _func")
                        res = _func(arg, const_arg, c, m)
                        log.debug("DONE crunching _func")
                        tf_1 = time.perf_counter()
                        time_calc_this = tf_1 - tf_0
                        time_calc += time_calc_this
                    # handle SystemExit in outer try ... except
                    except SystemExit as e:
                        raise e
                    # something went wrong while doing the actual calculation
                    # - write traceback to file
                    # - try to inform the server of the failure
                    except:
                        err, val, trb = sys.exc_info()
                        log.error(
                            "caught exception '%s' when crunching 'func'\n%s",
                            err.__name__,
                            traceback.print_exc(),
                        )

                        # write traceback to file
                        hostname = socket.gethostname()
                        fname = "traceback_err_{}_{}.trb".format(
                            err.__name__, getDateForFileName(includePID=True)
                        )

                        log.info("write exception to file %s", fname)
                        with open(fname, "w") as f:
                            traceback.print_exception(
                                etype=err, value=val, tb=trb, file=f
                            )

                        log.debug("put arg to local fail_q")
                        try:
                            with sig_delay([signal.SIGTERM]):
                                local_fail_q.put((arg, err.__name__, hostname))
                        # handle SystemExit in outer try ... except
                        except SystemExit as e:
                            log.warning(
                                "putting arg to local fail_q failed due to SystemExit"
                            )
                            raise e
                        # fail_q.put failed -> server down?
                        except Exception as e:
                            log.error("putting arg to local fail_q failed")
                            handle_unexpected_queue_error(e)
                            break
                        else:
                            log.debug("putting arg to local fail_q was successful")

                        raise

                    # processing the retrieved arguments succeeded
                    # - try to send the result back to the server
                    else:
                        try:
                            tp_0 = time.perf_counter()
                            with sig_delay([signal.SIGTERM]):
                                bin_data = pickle.dumps(
                                    {"arg": arg, "res": res, "time": time_calc_this}
                                )
                                local_result_q.put(bin_data)
                            log.debug("put result to local result_q, done!")
                            tp_1 = time.perf_counter()
                            time_queue += tp_1 - tp_0

                        # handle SystemExit in outer try ... except
                        except SystemExit as e:
                            log.warning(
                                "putting result to local result_q failed due to SystemExit"
                            )
                            raise e

                        except Exception as e:
                            log.error(
                                "putting result to local result_q failed due to %s",
                                type(e),
                            )
                            emergency_dump(
                                arg, res, emergency_dump_path, host, port, authkey
                            )
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
                if arg is None:
                    log.warning("SystemExit, quit processing, no argument to reinsert")
                else:
                    log.warning(
                        "SystemExit, quit processing, reinsert current argument, please wait"
                    )
                    log.debug("put arg back to local job_q")
                    try:
                        with sig_delay([signal.SIGTERM]):
                            local_job_q.put(arg)
                    # handle SystemExit in outer try ... except
                    except SystemExit as e:
                        log.error(
                            "puting arg back to local job_q failed due to SystemExit"
                        )
                        raise e
                    # fail_q.put failed -> server down?
                    except Exception as e:
                        log.error(
                            "puting arg back to local job_q failed due to %s", type(e)
                        )
                        handle_unexpected_queue_error(e)
                    else:
                        log.debug("putting arg back to local job_q was successful")
            finally:
                try:
                    sta = progress.humanize_time(time_calc / cnt)
                except:
                    sta = "invalid"

                stat = "pure calculation time: {} single task average: {}".format(
                    progress.humanize_time(time_calc), sta
                )
                try:
                    stat += "\ncalculation:{:.2%} communication:{:.2%}".format(
                        time_calc / (time_calc + time_queue),
                        time_queue / (time_calc + time_queue),
                    )
                except ZeroDivisionError:
                    pass

                log.info(stat)
                # print("client {}:{}\n".format(i, stat))
                log.debug(
                    "JobManager_Client.__worker_func at end (PID %s)", os.getpid()
                )

    def start(self):
        """
        starts a number of nproc subprocess to work on the job_q

        SIGTERM and SIGINT are managed to terminate all subprocesses

        retruns when all subprocesses have terminated
        """

        if (self.timeout is not None) and (self.timeout < 0):
            return

        self.connect()  # get shared objects from server
        if not self.connected:
            raise JMConnectionError(
                "Can not start Client with no connection to server (shared objetcs are not available)"
            )

        log.info(
            "STARTING CLIENT\nserver:%s authkey:%s port:%s num proc:%s",
            self.server,
            self.authkey.decode(),
            self.port,
            self.nproc,
        )
        # print("on start client, log.level", log.level)

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
        else:
            m_progress = None

        prepend = []

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
            prepend.append("w{0:0{1}}:".format(i + 1, l))

        job_q, result_q, fail_q, const_arg, manager = self.manager_objects

        local_job_q = mp.Queue()
        local_result_q = mp.Queue()
        local_fail_q = mp.Queue()

        infoline = progress.StringValue(num_of_bytes=64)
        bytes_send = mp.Value("L", 0)  # 4 byte unsigned int
        time_result_q_put = mp.Value("d", 0)  # 8 byte float (double)

        kwargs = {
            "reconnect_wait": self.reconnect_wait,
            "reconnect_tries": self.reconnect_tries,
            "ping_timeout": self.ping_timeout,
            "ping_retry": self.ping_retry,
        }

        job_q_get = proxy_operation_decorator(proxy=job_q, operation="get", **kwargs)
        job_q_put = proxy_operation_decorator(proxy=job_q, operation="put", **kwargs)
        result_q_put = proxy_operation_decorator(
            proxy=result_q, operation="put", **kwargs
        )
        fail_q_put = proxy_operation_decorator(proxy=fail_q, operation="put", **kwargs)

        result_q_put_pending_lock = threading.Lock()
        job_q_put_pending_lock = threading.Lock()
        fail_q_put_pending_lock = threading.Lock()

        def update_infoline(infoline, local_result_q, bytes_send, time_result_q_put):
            while True:
                if time_result_q_put.value > 0:
                    speed = (
                        humanize_size(bytes_send.value / time_result_q_put.value) + "/s"
                    )
                else:
                    speed = ""
                infoline.value = "local res_q {} {}".format(
                    local_result_q.qsize(), speed
                ).encode("utf-8")
                if self.timeout:
                    infoline.value += " timeout in: {}s".format(
                        int(self.timeout - (time.time() - self.init_time))
                    ).encode("utf-8")
                time.sleep(1)

        def pass_job_q_put(job_q_put, local_job_q, job_q_put_pending_lock):
            #             log.debug("this is thread thr_job_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))
            while True:
                data = local_job_q.get()
                log.debug("reinsert {}".format(data))
                with job_q_put_pending_lock:
                    job_q_put(data)

        #             log.debug("stopped thread thr_job_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))

        def pass_result_q_put(
            result_q_put,
            local_result_q,
            result_q_put_pending_lock,
            bytes_send,
            time_result_q_put,
        ):
            log.debug(
                "this is thread thr_result_q_put with tid %s",
                ctypes.CDLL("libc.so.6").syscall(186),
            )
            try:
                while True:
                    data = local_result_q.get()
                    log.debug("result_q client forward...\n{}".format(data))
                    with result_q_put_pending_lock:
                        t0 = time.perf_counter()
                        result_q_put(data)
                    t1 = time.perf_counter()
                    with bytes_send.get_lock():
                        bytes_send.value += len(data)
                    with time_result_q_put.get_lock():
                        time_result_q_put.value += t1 - t0
                    del data
                    log.debug(
                        "result_q client forward, done! ({:.2f}s)".format(t1 - t0)
                    )
            except Exception as e:
                log.error("thr_result_q_put caught error %s", type(e))
                log.info(traceback.format_exc())
            log.debug(
                "stopped thread thr_result_q_put with tid %s",
                ctypes.CDLL("libc.so.6").syscall(186),
            )

        def pass_fail_q_put(fail_q_put, local_fail_q, fail_q_put_pending_lock):
            #             log.debug("this is thread thr_fail_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))
            while True:
                data = local_fail_q.get()
                log.info("put {} to failq".format(data))
                with fail_q_put_pending_lock:
                    fail_q_put(data)

        #             log.debug("stopped thread thr_fail_q_put with tid %s", ctypes.CDLL('libc.so.6').syscall(186))

        thr_job_q_put = threading.Thread(
            target=pass_job_q_put, args=(job_q_put, local_job_q, job_q_put_pending_lock)
        )
        thr_job_q_put.daemon = True
        thr_result_q_put = threading.Thread(
            target=pass_result_q_put,
            args=(
                result_q_put,
                local_result_q,
                result_q_put_pending_lock,
                bytes_send,
                time_result_q_put,
            ),
        )
        thr_result_q_put.daemon = True
        thr_fail_q_put = threading.Thread(
            target=pass_fail_q_put,
            args=(fail_q_put, local_fail_q, fail_q_put_pending_lock),
        )
        thr_fail_q_put.daemon = True

        thr_update_infoline = threading.Thread(
            target=update_infoline,
            args=(infoline, local_result_q, bytes_send, time_result_q_put),
        )
        thr_update_infoline.daemon = True

        thr_job_q_put.start()
        thr_result_q_put.start()
        thr_fail_q_put.start()
        thr_update_infoline.start()

        if self.status_output_for_srun:
            self.hide_progress = True
        this_hostname = socket.gethostname().split(".")[0]

        with progress.ProgressBarCounterFancy(
            count=c,
            max_count=m_progress,
            interval=self.interval,
            prepend=prepend,
            sigint="ign",
            sigterm="ign",
            info_line=infoline,
            emtpy_lines_at_end=self.emtpy_lines_at_end,
        ) as self.pbc:

            if (not self.hide_progress) and self.show_statusbar_for_jobs:
                self.pbc.start()

            for i in range(self.nproc):
                reset_pbc = lambda: self.pbc.reset(i)
                p = mp.Process(
                    target=self.__worker_func,
                    args=(
                        self.func,  # func
                        self.nice,  # nice
                        log.level,  # loglevel
                        i,  # i
                        job_q_get,  # job_q_get
                        local_job_q,  # local_job_q
                        local_result_q,  # local_result_q
                        local_fail_q,  # local_fail_q
                        const_arg,  # const_arg
                        c[i],  # c
                        m_set_by_function[i],  # m
                        reset_pbc,  # reset_pbc
                        self.njobs,  # njobs
                        self.emergency_dump_path,  # emergency_dump_path
                        # self._job_q_get_timeout,  # job_q_get_timeout
                        self.server,  # host
                        self.port,  # port
                        self.authkey,  # authkey
                        self.nthreads,
                        self.nproc,
                    ),
                )
                self.procs.append(p)
                p.start()
                log.debug(f"started new worker with pid{p.pid}")

                if self.use_exclusive_cpu_per_process:
                    cpu_idx = (i+self.cpu_start_index) % mp.cpu_count()
                    os.sched_setaffinity(p.pid, [cpu_idx])
                    log.debug(f"restrict pid {p.pid} to CPU #{cpu_idx}")

                time.sleep(0.1)

            log.debug("all worker processes startes")

            # time.sleep(self.interval/2)

            if self.use_special_SIG_INT_handler:
                exit_handler_signals = [signal.SIGTERM]
                jm_client_special_interrupt_signals = [signal.SIGINT]
            else:
                exit_handler_signals = [signal.SIGTERM, signal.SIGINT]
                jm_client_special_interrupt_signals = []

            log.debug(
                "setup Signal_to_terminate_process_list handler for signals %s",
                exit_handler_signals,
            )
            exit_handler = Signal_to_terminate_process_list(
                process_list=self.procs,
                identifier_list=[
                    progress.get_identifier(
                        name="worker{}".format(i + 1), pid=p.pid, bold=True
                    )
                    for i, p in enumerate(self.procs)
                ],
                signals=exit_handler_signals,
                timeout=15,
            )

            log.debug(
                "setup Signal_handler_for_Jobmanager_client handler for signals %s",
                jm_client_special_interrupt_signals,
            )
            Signal_handler_for_Jobmanager_client(
                client_object=self,
                exit_handler=exit_handler,
                signals=jm_client_special_interrupt_signals,
            )

            for p in self.procs:
                while p.is_alive():
                    if self.timeout is not None:
                        elps_time = int(time.time() - self.init_time)
                        if self.timeout < elps_time:
                            log.debug(
                                "TIMEOUT for Client reached, terminate worker process %s",
                                p.pid,
                            )
                            p.terminate()
                            log.debug(
                                "wait for worker process %s to be joined ...", p.pid
                            )
                            p.join()
                            log.debug("worker process %s was joined", p.pid)
                            break
                        infoline.value = "timeout in: {}s".format(
                            int(self.timeout - elps_time)
                        ).encode("utf-8")
                    p.join(10)
                    if self.status_output_for_srun:
                        total_c = 0
                        for i in range(self.nproc):
                            total_c += self.pbc.counter_count[i].value
                        print(
                            "{} : {} jobs ".format(this_hostname, total_c)
                            + infoline.value.decode("utf-8")
                        )
                log.debug("worker process %s exitcode %s", p.pid, p.exitcode)
                log.debug("worker process %s was joined", p.pid)

            log.debug("all workers joind")
            log.debug("still in progressBar context")

        log.debug("progressBar context has been left")

        while (not local_job_q.empty()) or job_q_put_pending_lock.locked():
            log.info("still data in local_job_q (%s)", local_job_q.qsize())
            if thr_job_q_put.is_alive():
                log.debug("allow the thread thr_job_q_put to process items")
                time.sleep(1)
            else:
                log.warning(
                    "the thread thr_job_q_put has died, can not process remaining items"
                )
                break
        log.info("local_job_q now empty")

        while (not local_result_q.empty()) or result_q_put_pending_lock.locked():
            log.info("still data in local_result_q (%s)", local_result_q.qsize())
            if thr_result_q_put.is_alive():
                log.debug("allow the thread thr_result_q_put to process items")
                time.sleep(1)
            else:
                log.warning(
                    "the thread thr_result_q_put has died, dump remaining results"
                )
                while not local_result_q.empty():
                    arg, res = local_result_q.get()
                    emergency_dump(
                        arg,
                        res,
                        self.emergency_dump_path,
                        self.server,
                        self.port,
                        self.authkey,
                    )
                break
        log.info("local_result_q now empty")

        while (not local_fail_q.empty()) or fail_q_put_pending_lock.locked():
            log.info("still data in local_fail_q (%s)", local_fail_q.qsize())
            if thr_fail_q_put.is_alive():
                log.debug("allow the thread thr_fail_q_put to process items")
                time.sleep(1)
            else:
                log.warning(
                    "the thread thr_fail_q_put has died, can not process remaining items"
                )
                break
        log.info("local_fail_q now empty")

        log.info("client stopped")

        log.debug("setup default signal handler for SIGINT")
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        log.debug("setup default signal handler for SIGTERM")
        signal.signal(signal.SIGTERM, signal.SIG_DFL)


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


def set_shared_status(ss, v):
    if ss is not None:
        ss.value = v


def get_shared_status(ss):
    if ss in None:
        return None
    else:
        return ss.value


class ContainerClosedError(Exception):
    pass


class ClosableQueue_Data(object):
    def __init__(self, name=None):
        self.q = myQueue()
        self.is_closed = False
        self.lock = threading.Lock()
        if name:
            self.log_prefix = "ClosableQueue:{} - ".format(name)
        else:
            self.log_prefix = ""
        self.bytes_recieved = 0

    def new_conn(self):
        conn1, conn2 = mp.Pipe()
        log.debug("{}create new pair of connections, {}".format(self.log_prefix, conn2))
        thr_listener = threading.Thread(target=self._listener, args=(conn1,))
        thr_listener.daemon = True
        thr_listener.start()
        return conn2

    @staticmethod
    def _exec_cmd(f, conn, args):
        try:
            res = f(*args)
        except Exception as e:
            log.debug("exec_cmd '{}' sends exception '{}'".format(f.__name__, type(e)))
            conn.send(("#exc", e))
        else:
            log.debug("exec_cmd succesfull")
            conn.send(("#suc", res))

    def _listener(self, conn):
        while True:
            try:
                cmd = conn.recv()
                log.debug("{}listener got cmd '{}'".format(self.log_prefix, cmd))
                args_bytes = conn.recv_bytes()
                self.bytes_recieved += len(args_bytes)
                args = pickle.loads(args_bytes)
                if cmd == "#put":
                    self._exec_cmd(self.put, conn, args)
                elif cmd == "#get":
                    self._exec_cmd(self.get, conn, args)
                elif cmd == "#close":
                    self._exec_cmd(self.close, conn, args)
                elif cmd == "#qsize":
                    self._exec_cmd(self.qsize, conn, args)
                else:
                    raise ValueError("unknown command '{}'".format(cmd))
            except EOFError:
                log.debug(
                    "{}listener got EOFError, stop thread".format(self.log_prefix)
                )
                break

    def put(self, item, block=True, timeout=None):
        with self.lock:
            if self.is_closed:
                raise ContainerClosedError
            self.q.put(item, block, timeout)

    def get(self, block=True, timeout=None):
        with self.lock:
            return self.q.get(block, timeout)

    def qsize(self):
        with self.lock:
            return self.q.qsize()

    def close(self):
        with self.lock:
            self.is_closed = True
            log.debug("{}queue closed".format(self.log_prefix))


class ClosableQueue(object):
    def __init__(self, name=None):
        self.data = ClosableQueue_Data(name)
        self.conn = self.data.new_conn()
        self.lock = threading.Lock()
        self.name = name

    def client(self):
        cls = ClosableQueue.__new__(ClosableQueue)
        cls.data = None
        cls.conn = self.data.new_conn()
        cls.lock = threading.Lock()
        return cls

    def _communicate(self, cmd, args):
        with self.lock:
            try:
                self.conn.send(cmd)
                self.conn.send(args)
                res = self.conn.recv()
            except:
                # in case of any Error clear the pipe
                while self.conn.poll(timeout=1):
                    self.conn.recv()
                raise

        if res[0] == "#exc":
            raise res[1]
        else:
            return res[1]

    def put(self, item, block=True, timeout=None):
        return self._communicate("#put", (item, block, timeout))

    def get(self, block=True, timeout=None):
        return self._communicate("#get", (block, timeout))

    def qsize(self):
        return self._communicate("#qsize", tuple())

    def close(self):
        return self._communicate("#close", tuple())

    def get_bytes_recieved(self):
        if self.data:
            return self.data.bytes_recieved
        else:
            raise RuntimeError("client side has no counter for bytes_revieved")


class ArgsContainerQueue(object):
    def __init__(self, put_conn, get_conn):
        self.put_conn = put_conn
        self.get_conn = get_conn
        self.put_lock = threading.Lock()
        self.get_lock = threading.Lock()

    def put(self, item):
        with self.put_lock:
            try:
                self.put_conn.send(item)
                kind, res = self.put_conn.recv()
            except:
                while self.put_conn.poll(timeout=1):
                    self.put_conn.recv()
                raise
        if kind == "#suc":
            return
        elif kind == "#exc":
            raise res
        else:
            raise RuntimeError("unknown kind '{}'".format(kind))

    def get(self):
        with self.get_lock:
            try:
                self.get_conn.send("#GET")
                kind, res = self.get_conn.recv()
            except:
                while self.get_conn.poll(timeout=1):
                    self.get_conn.recv()
                raise
        if kind == "#res":
            return res
        elif kind == "#exc":
            raise res
        else:
            raise RuntimeError("unknown kind '{}'".format(kind))


class ArgsContainer(object):
    r"""a container for the arguments hold by the jobmanager server
    and fed to the jobmanager clients

    With the option to keep the data on the disk while providing
    a Queue like thread save interface. For multiprocessing the 'get_Queue'
    method returns again a Queue like object, that can be passes to subprocesses
    to access the actual container class.

    Additional features:
        - items may only be inserted once via 'put'
        - if an item was drawn using 'get', it may be reinserted using 'put'
        - items are identified via hash values, using sha256
        - items that were drawn using 'get' can be marked using 'mark'
        - items that are 'marked' can not be reinserted
        - the class is pickable, when unpickled, ALL items that are NOT marked
          will be accessible via 'get'

    These Features allows the following workflow for the Server/Client communication.

    The Client used 'get' to draw an item. Onces successfully processed the client
    returns the item and its results. The item will be marked once it was received by the server.
    Now the item is 'save', even when shutting down the server, dumping its state and restarting
    the server, the item will not be calculated again.
    """

    def __init__(self, path=None):
        self._path = path
        self._lock = threading.Lock()

        if self._path is None:
            self.data = {}
        else:
            self._open_shelve()

        self._closed = False
        self._not_gotten_ids = set()
        self._marked_ids = set()
        self._max_id = 0

    def get_queue(self):
        c_get_1, c_get_2 = mp.Pipe()
        c_put_1, c_put_2 = mp.Pipe()

        thr_sender = threading.Thread(target=self._sender, args=(c_get_1,))
        thr_sender.daemon = True
        thr_sender.start()

        thr_receiver = threading.Thread(target=self._receiver, args=(c_put_1,))
        thr_receiver.daemon = True
        thr_receiver.start()

        return ArgsContainerQueue(put_conn=c_put_2, get_conn=c_get_2)

    def _receiver(self, conn):
        while True:
            try:
                item = conn.recv()
            except EOFError:
                break
            try:
                self.put(item)
            except Exception as e:
                conn.send(("#exc", type(e)))
            else:
                conn.send(("#suc", None))

    def _sender(self, conn):
        while True:
            try:
                msg = conn.recv()
            except EOFError:
                break
            if msg == "#GET":
                try:
                    conn.send(("#res", self.get()))
                except Exception as e:
                    conn.send(("#exc", type(e)))
            else:
                raise RuntimeError("reveived unknown message '{}'".format(msg))

    def _open_shelve(self, new_shelve=True):
        if os.path.exists(self._path):
            if os.path.isfile(self._path):
                raise RuntimeWarning(
                    "can not create shelve, path '{}' is an existing file".format(
                        self._path
                    )
                )
            if new_shelve:
                raise RuntimeError(
                    "a shelve with name {} already exists".format(self._path)
                )
        else:
            os.makedirs(self._path)
        fname = os.path.abspath(os.path.join(self._path, "args"))
        self.data = shelve.open(fname)

    def __getstate__(self):
        with self._lock:
            if self._path is None:
                return (self.data, self._not_gotten_ids, self._marked_ids, self._max_id)
            else:
                return (
                    self._path,
                    self._not_gotten_ids,
                    self._marked_ids,
                    self._max_id,
                )

    def __setstate__(self, state):
        tmp, tmp_not_gotten_ids, self._marked_ids, self._max_id = state
        # the not gotten ones are all items except the markes ones
        # the old gotten ones which are not marked where lost
        self._not_gotten_ids = set(range(self._max_id)) - self._marked_ids
        if isinstance(tmp, dict):
            self.data = tmp
            self._path = None
        else:
            self._path = tmp
            self._open_shelve(new_shelve=False)
        self._closed = False
        self._lock = threading.Lock()

    def close(self):
        self._closed = True

    def close_shelve(self):
        if self._path is not None:
            self.data.close()

    def clear(self):
        with self._lock:
            if self._path is not None:
                self.data.close()
                rmtree(self._path)
            else:
                self.data.clear()
            self._closed = False
            self._not_gotten_ids = set()
            self._marked_ids = set()

    def qsize(self):
        return len(self._not_gotten_ids)

    def put_items(self):
        s = len(self.data) // 2
        return s

    def marked_items(self):
        return len(self._marked_ids)

    def gotten_items(self):
        return self.put_items() - self.qsize()

    def unmarked_items(self):
        return self.put_items() - self.marked_items()

    def put(self, item):
        with self._lock:
            if self._closed:
                raise ContainerClosedError

            item_hash = hashlib.sha256(bf.dump(item)).hexdigest()
            # print("ADD arg with hash", item_hash)
            # print(item)
            # print()
            if item_hash in self.data:
                item_id = self.data[item_hash]
                if (item_id in self._not_gotten_ids) or (item_id in self._marked_ids):
                    # the item has either not 'gotten' yet or is 'marked'
                    # in both cases a reinsert is not allowed
                    msg = (
                        "do not add the same argument twice! If you are sure, they are not the same, "
                        + "there might be an error with the binfootprint mehtods or a hash collision!"
                    )
                    log.critical(msg)
                    raise ValueError(msg)
                else:
                    # the item is allready known, but has been 'gotten' and not marked yet
                    # thefore a reinster it allowd
                    self._not_gotten_ids.add(item_id)
            else:
                str_id = "_" + str(self._max_id)
                self.data[str_id] = item
                self.data[item_hash] = self._max_id
                self._not_gotten_ids.add(self._max_id)
                self._max_id += 1

            # print("put", self._not_gotten_ids, self._marked_ids)

    def get(self):
        with self._lock:
            # print("get", self._not_gotten_ids, self._marked_ids)
            if self._closed:
                raise ContainerClosedError
            try:
                get_idx = self._not_gotten_ids.pop()
            except KeyError:
                raise queue.Empty

            str_id = "_" + str(get_idx)
            item = self.data[str_id]
            item_hash = hashlib.sha256(bf.dump(item)).hexdigest()
            # print("GET item with hash", item_hash)
            # print(item)
            # print()
            return item

    def mark(self, item):
        with self._lock:
            item_hash = hashlib.sha256(bf.dump(item)).hexdigest()
            # print("MARK item with hash", item_hash)
            # print(item)
            # print()

            item_id = self.data[item_hash]
            # print("mark", item_id, self._not_gotten_ids, self._marked_ids)
            if item_id in self._not_gotten_ids:
                raise ValueError("item not gotten yet, can not be marked")
            if item_id in self._marked_ids:
                raise RuntimeWarning("item already marked")
            self._marked_ids.add(item_id)


RAND_STR_ASCII_IDX_LIST = (
    list(range(48, 58)) + list(range(65, 91)) + list(range(97, 123))
)


def rand_str(l=8):
    s = ""
    for i in range(l):
        s += chr(random.choice(RAND_STR_ASCII_IDX_LIST))
    return s


def _new_rand_file_name(path=".", pre="", end="", l=8):
    c = 0
    while True:
        fname = pre + rand_str(l) + end
        full_name = os.path.join(path, fname)
        if not os.path.exists(full_name):
            return full_name

        c += 1
        if c > 10:
            l += 2
            c = 0
            print("INFO: increase random file name length to", l)


class JobManager_Manager(BaseManager):
    pass


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

    def __init__(
        self,
        authkey,
        const_arg=None,
        port=42524,
        verbose=None,
        msg_interval=1,
        fname_dump="auto",
        speed_calc_cycles=50,
        keep_new_result_in_memory=False,
        hide_progress=False,
        show_statistics=True,
        job_q_on_disk=False,
        job_q_on_disk_path=".",
        timeout=None,
        log_level=logging.WARNING,
        status_file_name=None,
        jm_ready_callback=lambda: print("jm ready"),
    ):
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

        assert fname_dump is None

        self.timeout = timeout
        self.start_time = datetime.now()

        global log
        log = logging.getLogger(__name__ + "." + self.__class__.__name__)
        log.setLevel(log_level)

        self._pid = os.getpid()
        self._pid_start = None

        if status_file_name:
            self.status_file = pathlib.Path(status_file_name)
            with open(self.status_file, "w") as f:
                f.write("init")
        else:
            self.status_file = None

        if verbose is not None:
            log.warning("verbose is deprecated, only allowed for compatibility")
            warnings.warn("verbose is deprecated", DeprecationWarning)

        self.hide_progress = hide_progress
        self.show_stat = show_statistics

        log.debug("I'm the JobManager_Server main process (pid %s)", os.getpid())

        self.__wait_before_stop = 2
        self.port = port

        if isinstance(authkey, bytearray):
            self.authkey = authkey
        else:
            self.authkey = bytearray(authkey, encoding="utf8")

        self.const_arg = const_arg

        self.fname_dump = fname_dump
        self.msg_interval = msg_interval
        self.speed_calc_cycles = speed_calc_cycles
        self.keep_new_result_in_memory = keep_new_result_in_memory

        # final result as list, other types can be achieved by subclassing
        self.final_result = []

        # NOTE: it only works using multiprocessing.Queue()
        # the Queue class from the module queue does NOT work

        self.manager = None
        self.hostname = socket.gethostname()
        self.job_q_on_disk = job_q_on_disk
        self.job_q_on_disk_path = job_q_on_disk_path
        if self.job_q_on_disk:
            fname = _new_rand_file_name(
                path=self.job_q_on_disk_path, pre=".", end="_jobqdb"
            )
        else:
            fname = None

        self.job_q = ArgsContainer(fname)
        self.result_q = mp.Queue()  # ClosableQueue(name='result_q')
        self.fail_q = mp.Queue()  # ClosableQueue(name='fail_q')

        self.stat = None

        self.jm_ready_callback = jm_ready_callback

        self.single_job_max_time = 0
        self.single_job_min_time = 10**10
        self.single_job_acu_time = 0
        self.single_job_cnt = 0

    @staticmethod
    def _check_bind(host, port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((host, port))
        except:
            log.critical("test bind to %s:%s failed", host, port)
            raise
        finally:
            s.close()

    def _start_manager(self):
        self._check_bind(self.hostname, self.port)

        # make job_q, result_q, fail_q, const_arg available via network
        q = self.job_q.get_queue()
        JobManager_Manager.register(
            "get_job_q", callable=lambda: q, exposed=["get", "put"]
        )
        JobManager_Manager.register("get_const_arg", callable=lambda: self.const_arg)

        # rc = self.result_q.client()
        # fc = self.fail_q.client()
        # JobManager_Manager.register('get_result_q', callable=lambda: rc, exposed=['get', 'put', 'qsize'])
        # JobManager_Manager.register('get_fail_q', callable=lambda: fc, exposed=['get', 'put', 'qsize'])

        JobManager_Manager.register(
            "get_result_q",
            callable=lambda: self.result_q,
            exposed=["get", "put", "qsize"],
        )
        JobManager_Manager.register(
            "get_fail_q", callable=lambda: self.fail_q, exposed=["get", "put", "qsize"]
        )

        address = ("", self.port)  # ip='' means local
        authkey = self.authkey
        self.manager = JobManager_Manager(address, authkey)
        self.manager.start()

        m_test = BaseManager(("localhost", self.port), authkey)
        try:
            m_test.connect()
        except:
            raise ConnectionError("test conntect to JobManager_Manager failed")

        log.info(
            "JobManager_Manager started on %s:%s (%s)",
            self.hostname,
            self.port,
            authkey,
        )
        print(
            "JobManager started on {}:{} ({})".format(self.hostname, self.port, authkey)
        )

    def _stop_manager(self):
        if self.manager == None:
            return

        manager_proc = self.manager._process
        # stop SyncManager
        self.manager.shutdown()
        log.info("JobManager Manager shutdown triggered")
        progress.check_process_termination(
            proc=manager_proc,
            prefix="JobManager Manager: ",
            timeout=2,
            auto_kill_on_last_resort=True,
        )

    def __enter__(self):
        return self

    def __exit__(self, err, val, trb):
        print("\n############## in JM SERVER EXIT\n")
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

    def shutdown(self):
        """ "stop all spawned processes and clean up

        - call process_final_result to handle all collected result
        - if job_q is not empty dump remaining job_q
        """
        # will only be False when _shutdown was started in subprocess
        if self.status_file:
            with open(self.status_file, "w") as f:
                f.write("stop")
        self.job_q.close()
        # self.result_q.close()
        # self.fail_q.close()
        log.debug("queues closed!")

        self._stop_manager()

        # do user defined final processing
        self.process_final_result()
        log.debug("process_final_result done!")

        self.show_statistics()

        if self.fname_dump is not None:
            if self.fname_dump == "auto":
                fname = "{}_{}.dump".format(
                    self.authkey.decode("utf8"), getDateForFileName(includePID=False)
                )
            else:
                fname = self.fname_dump

            log.info("dump current state to '%s'", fname)

            with open(fname, "wb") as f:
                self.__dump(f)

            log.debug("dump state done!")

        else:
            log.info("fname_dump == None, ignore dumping current state!")
            self.job_q.clear()

        # start also makes sure that it was not started as subprocess
        # so at default behavior this assertion will allays be True
        assert self._pid == os.getpid()

        log.info("JobManager_Server was successfully shut down")

    #        if self.status_file:
    #            os.remove(self.status_file)

    def show_statistics(self):
        if self.show_stat:
            all_jobs = self.job_q.put_items()
            succeeded = self.job_q.marked_items()
            failed = self.fail_q.qsize()

            all_processed = succeeded + failed

            id1 = self.__class__.__name__ + " "
            l = len(id1)
            id2 = " " * l + "| "

            print()
            dt = datetime.now() - self.start_time
            print(
                "{} start at {} | runtime {:.3e}s".format(
                    id1, self.start_time, dt.seconds
                )
            )

            print("{}total number of jobs  : {}".format(id1, all_jobs))
            print("{}  processed   : {}".format(id2, all_processed))
            print("{}    succeeded : {}".format(id2, succeeded))
            print("{}    failed    : {}".format(id2, failed))
            if self.single_job_cnt > 0:
                print(
                    "{}    timing in sec: min {:.3e} | max {:.3e} | avr {:.3e}".format(
                        id2,
                        self.single_job_min_time,
                        self.single_job_max_time,
                        self.single_job_acu_time / self.single_job_cnt,
                    )
                )

            all_not_processed = all_jobs - all_processed
            not_queried = self.number_of_jobs()
            queried_but_not_processed = all_not_processed - not_queried

            print("{}  not processed     : {}".format(id2, all_not_processed))
            print("{}    queried         : {}".format(id2, queried_but_not_processed))
            print("{}    not queried yet : {}".format(id2, not_queried))

    def all_successfully_processed(self):
        return self.number_of_jobs() == 0

    @staticmethod
    def static_load(f):
        data = {}
        data["final_result"] = pickle.load(f)
        data["job_q"] = pickle.load(f)
        data["fail_list"] = pickle.load(f)
        return data

    def __load(self, f):
        data = JobManager_Server.static_load(f)
        self.final_result = data["final_result"]
        self.job_q = data["job_q"]
        for fail_item in data["fail_list"]:
            self.fail_q.put(fail_item)

        log.debug("load: len(final_result): {}".format(len(self.final_result)))
        log.debug("load: job_q.qsize: {}".format(self.number_of_jobs()))
        log.debug("load: job_q.marked_items: {}".format(self.job_q.marked_items()))
        log.debug("load: job_q.gotten_items: {}".format(self.job_q.gotten_items()))
        log.debug("load: job_q.unmarked_items: {}".format(self.job_q.unmarked_items()))
        log.debug("load: len(fail_q): {}".format(self.fail_q.qsize()))

    def __dump(self, f):
        pickle.dump(self.final_result, f, protocol=pickle.HIGHEST_PROTOCOL)
        log.debug("dump: len(final_result): {}".format(len(self.final_result)))
        pickle.dump(self.job_q, f, protocol=pickle.HIGHEST_PROTOCOL)
        log.debug("dump: job_q.qsize: {}".format(self.number_of_jobs()))
        log.debug("dump: job_q.marked_items: {}".format(self.job_q.marked_items()))
        log.debug("dump: job_q.gotten_items: {}".format(self.job_q.gotten_items()))
        log.debug("dump: job_q.unmarked_items: {}".format(self.job_q.unmarked_items()))

        fail_list = []
        try:
            while True:
                fail_list.append(self.fail_q.get(timeout=0))
        except queue.Empty:
            pass
        pickle.dump(fail_list, f, protocol=pickle.HIGHEST_PROTOCOL)
        log.debug("dump: len(fail_q): {}".format(len(fail_list)))

    def read_old_state(self, fname_dump=None):
        if fname_dump == None:
            fname_dump = self.fname_dump
        if fname_dump == "auto":
            log.critical("fname_dump must not be 'auto' when reading old state")
            raise RuntimeError("fname_dump must not be 'auto' when reading old state")

        if not os.path.isfile(fname_dump):
            log.critical("file '%s' to read old state from not found", fname_dump)
            raise RuntimeError(
                "file '{}' to read old state from not found".format(fname_dump)
            )

        log.info("load state from file '%s'", fname_dump)

        with open(fname_dump, "rb") as f:
            self.__load(f)

        self.show_statistics()

    def put_arg(self, a):
        """add argument a to the job_q"""
        # hash_bfa = hashlib.sha256(bf.dump(a)).digest()
        #         if hash_bfa in self.args_dict:
        #             msg = ("do not add the same argument twice! If you are sure, they are not the same, "+
        #                    "there might be an error with the binfootprint mehtods or a hash collision!")
        #             log.critical(msg)
        #             raise ValueError(msg)

        # this dict associates an unique index with each argument 'a'
        # or better with its binary footprint
        # self.args_dict[hash_bfa] = len(self.args_list)
        # self.args_list.append(a)

        # the actual shared queue
        self.job_q.put(copy.copy(a))

        # with self._numjobs.get_lock():
        #    self._numjobs.value += 1

    def number_of_jobs(self):
        return self.job_q.qsize()

    def args_from_list(self, args):
        """serialize a list of arguments to the job_q"""
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

    def bring_him_up(self, no_sys_exit_on_signal=False):

        self._start_manager()

        if self._pid != os.getpid():
            log.critical("do not run JobManager_Server.start() in a subprocess")
            raise RuntimeError("do not run JobManager_Server.start() in a subprocess")

        jobqsize = self.number_of_jobs()
        if jobqsize == 0:
            log.info(
                "no jobs to process! use JobManager_Server.put_arg to put arguments to the job_q"
            )
            return
        else:
            log.info(
                "started (host:%s authkey:%s port:%s jobs:%s)",
                self.hostname,
                self.authkey.decode(),
                self.port,
                jobqsize,
            )
            # print("{} started (host:{} authkey:{} port:{} jobs:{})".format(self.__class__.__name__,
            #                                                                self.hostname,
            #                                                                self.authkey.decode(),
            #                                                                self.port,
            #                                                                jobqsize))
        if no_sys_exit_on_signal:
            log.info(
                "no_sys_exit_on_signal was set to True. It's the users responsability to call the 'shutdown' method."
            )
        else:
            Signal_to_sys_exit(signals=[signal.SIGTERM, signal.SIGINT])

        log.debug("ready for processing incoming results")
        self.jm_ready_callback()

        if self.status_file:
            with open(self.status_file, "w") as f:
                f.write("ready")

    def join(self, stopEvent=None):
        """
        starts to loop over incoming results

        When finished, or on exception call stop() afterwards to shut down gracefully.
        """

        info_line = progress.StringValue(num_of_bytes=128)

        numresults = progress.UnsignedIntValue(
            self.job_q.marked_items() + self.fail_q.qsize()
        )
        numjobs = progress.UnsignedIntValue(self.job_q.put_items())

        log.debug("at start: number of jobs: {}".format(numjobs.value))
        log.debug("at start: number of results: {}".format(numresults.value))

        speed_q = myQueue()
        time_stamp = time.perf_counter()
        bytes_recieved = 0
        for i in range(15):
            speed_q.put((bytes_recieved, time_stamp))

        with progress.ProgressBarFancy(
            count=numresults,
            max_count=numjobs,
            interval=self.msg_interval,
            speed_calc_cycles=self.speed_calc_cycles,
            sigint="ign",
            sigterm="ign",
            info_line=info_line,
        ) as self.stat:
            if not self.hide_progress:
                self.stat.start()

            data_speed = 0
            while numresults.value < numjobs.value:

                if stopEvent is not None:
                    if stopEvent.is_set():
                        log.info(
                            "received externally set stop event -> leave join loop"
                        )
                        break

                numjobs.value = self.job_q.put_items()
                failqsize = self.fail_q.qsize()
                jobqsize = self.number_of_jobs()
                markeditems = self.job_q.marked_items()
                numresults.value = failqsize + markeditems
                if (time.perf_counter() - time_stamp) > self.msg_interval:
                    old_bytes, old_time_stamp = speed_q.get()

                    time_stamp = time.perf_counter()
                    speed_q.put((bytes_recieved, time_stamp))
                    data_speed = humanize_size(
                        (bytes_recieved - old_bytes) / (time_stamp - old_time_stamp)
                    )

                if self.timeout is not None:
                    time_left = int(
                        self.timeout
                        - self.__wait_before_stop
                        - (datetime.now() - self.start_time).total_seconds()
                    )
                    if time_left < 0:
                        if self.stat:
                            self.stat.stop()
                        log.warning(
                            "timeout ({}s) exceeded -> quit server".format(self.timeout)
                        )
                        break
                    info_line.value = (
                        (
                            "res_q #{} {}/s {}|rem{} "
                            + "done{} fail{} prog{} "
                            + "timeout:{}s"
                        )
                        .format(
                            self.result_q.qsize(),
                            data_speed,
                            humanize_size(bytes_recieved),
                            jobqsize,
                            markeditems,
                            failqsize,
                            numjobs.value - numresults.value - jobqsize,
                            time_left,
                        )
                        .encode("utf-8")
                    )
                else:
                    info_line.value = (
                        ("res_q #{} {}/s {}|rem.:{}, " + "done:{}, failed:{}, prog.:{}")
                        .format(
                            self.result_q.qsize(),
                            data_speed,
                            humanize_size(bytes_recieved),
                            jobqsize,
                            markeditems,
                            failqsize,
                            numjobs.value - numresults.value - jobqsize,
                        )
                        .encode("utf-8")
                    )
                log.info("infoline {}".format(info_line.value))
                # allows for update of the info line
                try:
                    bin_data = self.result_q.get(timeout=self.msg_interval)
                    data_dict = pickle.loads(bin_data)
                    bytes_recieved += len(bin_data)
                    del bin_data
                except queue.Empty:
                    continue
                # print("got arg", arg)
                arg = data_dict["arg"]
                res = data_dict["res"]
                single_job_time = data_dict["time"]
                self.job_q.mark(data_dict["arg"])
                # print("has been marked!")
                log.debug("received {}".format(data_dict["arg"]))
                self.process_new_result(arg, res)
                self.single_job_max_time = max(
                    self.single_job_max_time, single_job_time
                )
                self.single_job_min_time = min(
                    self.single_job_min_time, single_job_time
                )
                self.single_job_acu_time += single_job_time
                self.single_job_cnt += 1
                if not self.keep_new_result_in_memory:
                    del data_dict

        self.stat = None

        log.debug("wait %ss before trigger clean up", self.__wait_before_stop)
        time.sleep(self.__wait_before_stop)

    def start(self):
        self.bring_him_up()
        self.join()


class JobManager_Local(object):
    def __init__(
        self,
        server_class,
        client_class,
        server_init_kwargs={},
        client_init_kwargs={},
        authkey="local_jobmanager",
        nproc=-1,
        const_arg=None,
        port=42524,
        verbose_client=None,
        show_statusbar_for_jobs=False,
        show_counter_only=False,
        niceness_clients=19,
    ):

        self.server = server_class(
            authkey=authkey, const_arg=const_arg, port=port, **server_init_kwargs
        )

        self.client_class = client_class
        self.client_init_kwargs = client_init_kwargs
        self.authkey = authkey
        self.port = port
        self.nproc = nproc
        self.verbose_client = verbose_client
        self.show_statusbar_for_jobs = show_statusbar_for_jobs
        self.show_counter_only = show_counter_only
        self.niceness_clients = niceness_clients
        self.p_client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.server.stat:
            self.server.stat.stop()  # stop the progress bar to see all messages

        if self.p_client is not None:
            self.p_client.terminate()
            # print("join client ...")
            self.p_client.join()
            # print("client has joined")

        # print("shutdown server")
        self.server.shutdown()

    @staticmethod
    def _start_client(
        authkey,
        port,
        client_class,
        client_init_kwargs,
        nproc=0,
        nice=19,
        verbose=None,
        show_statusbar_for_jobs=False,
        show_counter_only=False,
    ):

        client = client_class(
            server="localhost",
            authkey=authkey,
            port=port,
            nproc=nproc,
            nice=nice,
            verbose=verbose,
            show_statusbar_for_jobs=show_statusbar_for_jobs,
            show_counter_only=show_counter_only,
            ask_on_sigterm=False,
            **client_init_kwargs
        )

        Signal_to_sys_exit(signals=[signal.SIGINT, signal.SIGTERM])
        client.start()

    def start(self):
        n = parse_nproc(self.nproc)
        nproc = min(self.server.number_of_jobs(), n)

        self.p_client = mp.Process(
            target=JobManager_Local._start_client,
            args=(
                self.authkey,
                self.port,
                self.client_class,
                self.client_init_kwargs,
                nproc,
                self.niceness_clients,
                self.verbose_client,
                self.show_statusbar_for_jobs,
                self.show_counter_only,
            ),
        )

        self.server.bring_him_up(no_sys_exit_on_signal=True)
        if nproc == 0:
            self.p_client = None
            return

        self.p_client.start()
        self.server.join()


class RemoteKeyError(RemoteError):
    pass


class RemoteValueError(RemoteError):
    pass


class Signal_handler_for_Jobmanager_client(object):
    def __init__(self, client_object, exit_handler, signals=[signal.SIGINT]):
        self.client_object = client_object
        self.exit_handler = exit_handler
        for s in signals:
            log.debug(
                "setup Signal_handler_for_Jobmanager_client for signal %s",
                progress.signal_dict[s],
            )
            signal.signal(s, self._handler)

    def _handler(self, sig, frame):

        log.info("received signal %s", progress.signal_dict[sig])

        if self.client_object.pbc is not None:
            self.client_object.pbc.pause()

        try:
            if self.client_object.ask_on_sigterm:
                r = input_promt(
                    progress.terminal.ESC_BOLD
                    + progress.terminal.ESC_LIGHT_RED
                    + "<q> - quit, <i> - server info, <k> - kill: "
                    + progress.terminal.ESC_NO_CHAR_ATTR
                )
            else:
                r = "q"
        except Exception as e:
            log.error("Exception during input %s", type(e))
            log.info(traceback.format_exc())
            r = "q"

        log.debug("input: '%s'", r)

        if r == "i":
            self._show_server_info()
        elif r == "q":
            log.info("terminate worker functions")
            self.exit_handler._handler(sig, frame)
        #            log.info("call sys.exit -> raise SystemExit")
        #            sys.exit('exit due to user')
        elif r == "k":
            for p in self.exit_handler.process_list:
                print("send SIGKILL to", p)
                os.kill(p.pid, signal.SIGKILL)
        else:
            print("input '{}' ignored".format(r))

        if self.client_object.pbc is not None:
            self.client_object.pbc.resume()

    def _show_server_info(self):
        self.client_object.server
        self.client_object.authkey
        print(
            "connected to {}:{} using authkey {}".format(
                self.client_object.server,
                self.client_object.port,
                self.client_object.authkey,
            )
        )


class Signal_to_SIG_IGN(object):
    def __init__(self, signals=[signal.SIGINT, signal.SIGTERM]):
        for s in signals:
            signal.signal(s, self._handler)

    def _handler(self, sig, frame):
        log.info(
            "PID %s: received signal %s -> will be ignored",
            os.getpid(),
            progress.signal_dict[sig],
        )


class Signal_to_sys_exit(object):
    def __init__(self, signals=[signal.SIGINT, signal.SIGTERM]):
        for s in signals:
            signal.signal(s, self._handler)

    def _exit(self, signal):
        log.info(
            "PID %s: received signal %s -> call sys.exit -> raise SystemExit",
            os.getpid(),
            progress.signal_dict[signal],
        )
        sys.exit("exit due to signal {}".format(progress.signal_dict[signal]))

    def _handler(self, signal, frame):
        self._exit(signal)


class Signal_to_terminate_process_list(object):
    """
    SIGINT and SIGTERM will call terminate for process given in process_list
    """

    def __init__(
        self,
        process_list,
        identifier_list,
        signals=[signal.SIGINT, signal.SIGTERM],
        timeout=2,
    ):
        self.process_list = process_list
        self.identifier_list = identifier_list
        self.timeout = timeout

        for s in signals:
            log.debug(
                "setup Signal_to_terminate_process_list for signal %s",
                progress.signal_dict[s],
            )
            signal.signal(s, self._handler)

    def _handler(self, signal, frame):
        log.debug(
            "received sig %s -> terminate all given subprocesses",
            progress.signal_dict[signal],
        )
        for i, p in enumerate(self.process_list):
            p.terminate()

        for i, p in enumerate(self.process_list):
            progress.check_process_termination(
                proc=p,
                prefix=self.identifier_list[i],
                timeout=self.timeout,
                auto_kill_on_last_resort=False,
            )


def address_authkey_from_proxy(proxy):
    return proxy._token.address, proxy._authkey.decode()


def address_authkey_from_manager(manager):
    return manager.address, manager._authkey.decode()


def call_connect(connect, dest, reconnect_wait=2, reconnect_tries=3):
    c = 0
    while True:
        try:  # here we try re establish the connection
            log.debug("try connecting to %s", dest)
            connect()

        except Exception as e:
            log.error(
                "connection to %s could not be established due to '%s'", dest, type(e)
            )
            log.error(traceback.format_stack()[-3].strip())

            if (
                type(e) is ConnectionResetError
            ):  # ... when the destination hangs up on us
                c = handler_connection_reset(dest, c, reconnect_wait, reconnect_tries)
            elif (
                type(e) is ConnectionRefusedError
            ):  # ... when the destination refuses our connection
                handler_connection_refused(e, dest)
            elif (
                type(e) is AuthenticationError
            ):  # ... when the destination refuses our connection due authkey missmatch
                handler_authentication_error(e, dest)
            elif (
                type(e) is RemoteError
            ):  # ... when the destination send us an error message
                if "KeyError" in e.args[0]:
                    handler_remote_key_error(e, dest)
                elif "ValueError: unsupported pickle protocol:" in e.args[0]:
                    handler_remote_value_error(e, dest)
                else:
                    handler_remote_error(e, dest)
            elif type(e) is ValueError:
                handler_value_error(e)
            else:  # any other exception
                handler_unexpected_error(e)

        else:  # no exception
            log.debug("connection to {} successfully established".format(dest))
            return True


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


def getDateForFileName(includePID=False):
    """returns the current date-time and optionally the process id in the format
    YYYY_MM_DD_hh_mm_ss_pid
    """
    date = time.localtime()
    name = "{:d}_{:02d}_{:02d}_{:02d}_{:02d}_{:02d}".format(
        date.tm_year, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min, date.tm_sec
    )
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
    log.info(
        "This usually means that no matching Manager object was instanciated at destination side!"
    )
    log.info(
        "Either there is no Manager running at all, or it is listening to another port."
    )
    raise JMConnectionRefusedError(e)


def handler_connection_reset(dest, c, reconnect_wait, reconnect_tries):
    log.error("connection reset error")
    log.info(
        "During 'connect' this error might be due to firewall settings"
        + "or other TPC connections controlling mechanisms!"
    )
    c += 1
    if c > reconnect_tries:
        log.error("maximium number of reconnects %s was reached", reconnect_tries)
        raise JMConnectionError(
            "connection to %s FAILED, ".format(dest)
            + "{} retries were NOT successfull".format(reconnect_tries)
        )
    log.debug("try connecting to %s again in %s seconds", dest, reconnect_wait)
    time.sleep(reconnect_wait)
    return c


def handler_eof_error(e):
    log.error("EOF error")
    log.info(
        "This usually means that server did not replay, although the connection is still there.\n"
        + "This is due to the fact that the connection is in 'timewait' status for about 60s\n"
        + "after the server went down inappropriately."
    )
    raise e


def handler_remote_error(e, dest):
    log.error("remote error")
    log.info("The server %s send an RemoteError message!\n%s", dest, e.args[0])
    raise RemoteError(e.args[0])


def handler_remote_key_error(e, dest):
    log.error("remote key error")
    log.info(
        "'KeyError' detected in RemoteError message from server %s!\n"
        + "This hints to the fact that the actual instace of the shared object on the server side has changed,\n"
        + "for example due to a server restart you need to reinstanciate the proxy object.",
        dest,
    )
    raise RemoteKeyError(e.args[0])


def handler_remote_value_error(e, dest):
    log.error("remote value error")
    log.info(
        "'ValueError' due to 'unsupported pickle protocol' detected in RemoteError from server %s!\n"
        + "You might have tried to connect to a SERVER running with an OLDER python version.\n"
        + "At this stage (and probably for ever) this should be avoided!",
        dest,
    )
    raise RemoteValueError(e.args[0])


def handler_value_error(e):
    log.error("value error")
    if "unsupported pickle protocol" in e.args[0]:
        log.info(
            "'ValueError' due to 'unsupported pickle protocol'!\n"
            "You might have tried to connect to a SERVER running with an NEWER python version.\n"
            "At this stage (and probably for ever) this should be avoided.\n"
        )
    raise e


def handler_unexpected_error(e):
    log.error("unexpected error of type %s and args %s", type(e), e.args)
    raise e


def handle_unexpected_queue_error(e):
    log.error(
        "unexpected error of type %s and args %s\n"
        + "I guess the server went down, can't do anything, terminate now!",
        type(e),
        e.args,
    )
    log.debug(traceback.print_exc())


def emergency_dump(arg, res, emergency_dump_path, host, port, authkey):
    now = datetime.now().isoformat()
    pid = os.getpid()
    fname = "jmclient_dump_{}_pid_{}_host_{}_port_{}_authkey_{}.dump".format(
        now, pid, host, port, authkey.decode()
    )
    full_path = os.path.join(emergency_dump_path, fname)
    log.warning("emergency dump (arg, res) to %s", full_path)
    with open(full_path, "wb") as f:
        pickle.dump(arg, f)
        pickle.dump(res, f)


def check_if_host_is_reachable_unix_ping(adr, timeout=2, retry=5):
    output = ""

    i = 0
    while True:
        try:
            cmd = "ping -c 1 -W {} {}    ".format(int(timeout), adr)
            log.debug("[%s/%s]call: %s", i + 1, retry, cmd)
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError as e:
            # on exception, resume with loop
            log.warning("CalledProcessError on ping with message: %s", e)
            output = e.output
            continue
        else:
            # no exception, ping was successful, return without error
            log.debug("ping was succesfull")
            return

        i += 1
        if (i >= retry) and (retry > 0):
            break

    # no early return happend, ping was never successful, raise error
    log.error("ping failed after %s retries", retry)
    raise JMHostNotReachableError(
        "could not reach host '{}'\nping error reads: {}".format(adr, output)
    )


class proxy_operation_decorator(object):
    def __init__(
        self,
        proxy,
        operation,
        reconnect_wait=2,
        reconnect_tries=3,
        ping_timeout=2,
        ping_retry=5,
    ):
        self.proxy = proxy
        self.operation = operation
        self.o = getattr(proxy, operation)
        self.dest = address_authkey_from_proxy(proxy)
        self.reconnect_wait = reconnect_wait
        self.reconnect_tries = reconnect_tries
        self.ping_timeout = ping_timeout
        self.ping_retry = ping_retry

    def __call__(self, *args, **kwargs):
        try:
            res = self.o(*args, **kwargs)
        except queue.Empty as e:
            log.debug(
                "operation '%s' -> %s FAILED due to '%s' -> retry",
                self.operation,
                self.dest,
                type(e),
            )
        except Exception as e:
            log.warning(
                "operation '%s' -> %s FAILED due to '%s' -> retry",
                self.operation,
                self.dest,
                type(e),
            )
        else:
            log.debug("operation '{}' successfully executed".format(self.operation))
            return res

        t0 = time.perf_counter()
        c = 0
        reconnect_wait = self.reconnect_wait
        while True:
            reconnect_wait *= 1.2
            t1 = time.perf_counter()
            check_if_host_is_reachable_unix_ping(
                adr=self.dest[0][0], timeout=self.ping_timeout, retry=self.ping_retry
            )
            log.debug("ping time: {:.2f}s".format(time.perf_counter() - t1))
            log.debug("establish connection to %s", self.dest)
            try:
                t1 = time.perf_counter()
                self.proxy._connect()
            except Exception as e:
                log.warning(
                    "establishing connection to %s FAILED due to '%s'",
                    self.dest,
                    type(e),
                )
                log.debug(
                    "show traceback.format_stack()[-3]\n%s",
                    traceback.format_stack()[-3].strip(),
                )
                c += 1
                if (c > self.reconnect_tries) and (self.reconnect_tries > 0):
                    log.error(
                        "reached maximum number of reconnect tries %s, raise exception",
                        self.reconnect_tries,
                    )
                    raise e
                log.info("wait %s seconds and retry", reconnect_wait)
                time.sleep(reconnect_wait)
                continue
            log.debug(
                "self.proxy._connect() time: {:.2f}s".format(time.perf_counter() - t1)
            )
            log.debug("execute operation '%s' -> %s", self.operation, self.dest)

            try:
                t1 = time.perf_counter()
                res = self.o(*args, **kwargs)
            except queue.Empty as e:
                log.info(
                    "operation '%s' -> %s FAILED due to '%s'",
                    self.operation,
                    self.dest,
                    type(e),
                )
                raise e
            except Exception as e:
                log.warning(
                    "operation '%s' -> %s FAILED due to '%s'",
                    self.operation,
                    self.dest,
                    type(e),
                )
                log.debug(
                    "show traceback.format_stack()[-3]\n%s",
                    traceback.format_stack()[-3].strip(),
                )
                if type(e) is ConnectionResetError:
                    log.debug("show traceback.print_exc(limit=1))")
                    log.debug(traceback.print_exc(limit=1))

                    c += 1
                    if (c > self.reconnect_tries) and (self.reconnect_tries > 0):
                        log.error(
                            "reached maximum number of reconnect tries %s",
                            self.reconnect_tries,
                        )
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
            else:  # SUCCESS -> return True
                log.debug(
                    "self.o(*args, **kwargs) time: {:.2f}s".format(
                        time.perf_counter() - t1
                    )
                )
                log.debug(
                    "operation '{}' successfully executed (overall time {:.2f}s)".format(
                        self.operation, time.perf_counter() - t0
                    )
                )
                return res

            log.debug("close connection to %s", self.dest)
            try:
                self.proxy._tls.connection.close()
            except Exception as e:
                log.error(
                    "closeing connection to %s FAILED due to %s", self.dest, type(e)
                )
                log.info(
                    "show traceback.format_stack()[-3]\n%s",
                    traceback.format_stack()[-3].strip(),
                )


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
    blackhole = open(os.devnull, "wb")
    try:
        pickle.dump(obj, blackhole)
        return True
    except:
        if show_exception:
            traceback.print_exc()
        return False


# a list of all names of the implemented python signals
all_signals = [s for s in dir(signal) if (s.startswith("SIG") and s[3] != "_")]

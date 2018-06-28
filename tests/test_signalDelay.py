import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import logging
import multiprocessing as mp
from jobmanager import signalDelay
import os
import signal

import time




ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter(fmt="%(asctime)s|%(name)s|%(levelname)s|%(msg)s"))

signalDelay.log.setLevel(logging.DEBUG)
signalDelay.log.addHandler(ch)

v = mp.Value('I')

sleep_time = 0.1


def no_output():
    return
    f = open('/dev/null', 'w')
    sys.stdout = f
    sys.stderr = f

def test_sd():

    def _important_func(v):
        no_output()
        try:
            for i in range(10):
                v.value = i
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            # this prevents the traceback
            pass

    @signalDelay.sig_delay([signal.SIGINT])
    def _important_func_with_dec(v):
        _important_func(v)

    # call _important_func in a subprocess and send SIGINT after 1 second
    # the subprocess will terminate immediately
    # v should be smaller than 5
    p = mp.Process(target=_important_func, args=(v,))
    p.start()
    time.sleep(2*sleep_time)
    os.kill(p.pid, signal.SIGINT)
    p.join()
    assert v.value < 5
    assert p.exitcode == 0   # since the KeyboardInterrupt Error is caught and ignored


    # call _important_func in a subprocess and send SIGINT after 1 second
    # the subprocess will terminate immediately
    # v should be smaller than 5
    p = mp.Process(target=_important_func_with_dec, args=(v,))
    p.start()
    time.sleep(2*sleep_time)
    os.kill(p.pid, signal.SIGINT)
    p.join()
    assert v.value == 9
    assert p.exitcode == 1  # since the SIGINT is reemited again after the scope
                            # of _important_func the KeyboardInterrupt Error can not be caught


def test_sd_ctx():
    def _important_func(v):
        no_output()
        try:
            for i in range(10):
                v.value = i
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            # this prevents the traceback
            pass

    def _important_func_with_dec(v):
        with signalDelay.sig_delay([signal.SIGINT]):
            _important_func(v)

    # call _important_func in a subprocess and send SIGINT after 1 second
    # the subprocess will terminate immediately
    # v should be smaller than 5
    p = mp.Process(target=_important_func, args=(v,))
    p.start()
    time.sleep(2*sleep_time)
    os.kill(p.pid, signal.SIGINT)
    p.join()
    assert v.value < 5
    assert p.exitcode == 0   # since the KeyboardInterrupt Error is caught and ignored


    # call _important_func in a subprocess and send SIGINT after 1 second
    # the subprocess will terminate immediately
    # v should be smaller than 5
    p = mp.Process(target=_important_func_with_dec, args=(v,))
    p.start()
    time.sleep(2*sleep_time)
    os.kill(p.pid, signal.SIGINT)
    p.join()
    assert v.value == 9
    assert p.exitcode == 1  # since the SIGINT is reemited again after the scope
                            # of _important_func the KeyboardInterrupt Error can not be caught

if __name__ == "__main__":
    test_sd()
    test_sd_ctx()
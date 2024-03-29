#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

""" How to wrap or decorate a function with a progress bar.


"""

import multiprocessing as mp
from os.path import split, dirname, abspath
import sys
import time

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

import jobmanager as jm


def UnsignedIntValue(val=0):
    return mp.Value("I", val, lock=True)


@jm.decorators.ProgressBar
def decorated_function_alpha(an_argument, c=UnsignedIntValue(), m=UnsignedIntValue()):
    """A simple example of a ProgressBar-decorated function.

    The arguments `c` and `m` are the counter and maximal counter
    variables of the ProgressBar. They are instances of
    `multiprocessing.Value`.
    """
    m.value = 10
    c.value = 0
    for i in range(10):
        # this is were things are computed
        c.value += 1
        time.sleep(0.2)
    return an_argument


@jm.decorators.ProgressBar
def decorated_function_beta(
    an_argument, jmc=UnsignedIntValue(), jmm=UnsignedIntValue()
):
    """A simple example of a ProgressBar-decorated function.

    In comparison to `decorated_function_alpha`, we now have the
    arguments `jmc` and `jmm`. Jobmanager automatically detects
    arguments that are registered in
    `jobmanager.jobmanager.validCountKwargs`.

    Note that we do not need to set the value of jmc to zero, as the
    ProgressBar initiates the variable with zero.
    """
    jmm.value = 10

    for i in range(10):
        # this is were things are computed
        jmc.value += 1
        time.sleep(0.2)
    return an_argument


@jm.decorators.ProgressBar
def decorated_function_gamma(
    arg, kwarg="2", jmc=UnsignedIntValue(), jmm=UnsignedIntValue()
):
    """A simple example of a ProgressBar-decorated function.

    In comparison to `decorated_function_alpha`, we now have the
    arguments `jmc` and `jmm`. Jobmanager automatically detects
    arguments that are registered in
    `jobmanager.jobmanager.validCountKwargs`.

    Note that we do not need to set the value of jmc to zero, as the
    ProgressBar initiates the variable with zero.
    """
    jmm.value = 10

    for i in range(10):
        # this is were things are computed
        jmc.value += 1
        time.sleep(0.2)
    return "{} {}".format(arg, kwarg)


def wrapped_function_beta(an_argument, jmc=None, jmm=None):
    """A simple example of a ProgressBar-decorated function.

    In comparison to `decorated_function_beta`, the count arguments
    became keyword arguments. The function works with and without
    the ProgressBar.
    """
    if jmm is not None:
        jmm.value = 10

    for i in range(10):
        # this is were things are computed
        if jmc is not None:
            jmc.value += 1
        time.sleep(0.2)
    return an_argument


if __name__ == "__main__":
    ##d ecorated
    retd1 = decorated_function_alpha("argument")
    retd2 = decorated_function_beta("argument")
    retd3 = decorated_function_gamma("argument", kwarg="test")
    ## wrapped
    # When using the wrapper, you can define arguments for
    # `jm.progress.ProgressBar`.
    pb = jm.decorators.ProgressBarOverrideCount(wrapped_function_beta, interval=0.05)
    retw1 = pb("argument")
    # or
    retw2 = jm.decorators.ProgressBarOverrideCount(wrapped_function_beta)("arg")

    print(retd1, retd2, retd3, sep=" | ")
    print(retw1, retw2, sep=" | ")

#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Implements decorators/wrappers for simple use-cases of jobmanager.
"""
from __future__ import division, print_function

import multiprocessing as mp
import time


from . import progress

__all__ = ["SingleProgressBar"]

class SingleProgressBar(object):
    """ A wrapper/decorator with a text-based progress bar.
    
    Methods:
    - __init__
    - __call__
    
    
    Examples
    --------
    
    >>> from jobmanager.decorators import SingleProgressBar
    >>> import time
    >>> 
    >>> @SingleProgressBar
    >>> def my_func(arg, kwarg="1", count=None, max_count=None):
    >>>     maxval = 100
    >>>     if max_count is not None:
    >>>         max_count.value = maxval
    >>>         
    >>>     for i in range(maxval):
    >>>         if count is not None:
    >>>             count.value += 1
    >>>         time.sleep(0.05)
    >>> 
    >>>     return arg+kwarg
    >>> 
    >>> my_func_1("one argument", kwarg=" second argument")
    # The progress of my_func is monitored on stdout.
    one argument second argument
    
    
    Notes
    -----
    You can also use this class as a wrapper and tune parameters of the
    progress bar.
    
    >>> wrapper = SingleProgressBar(my_func, interval=.1)
    >>> result = wrapper("wrapped function", kwarg=" test")
    
    """
    def __init__(self, func, *args, **kwargs):
        """ Initiates the wrapper objet.
        
        A function can be wrapped by decorating it with
        `SingleProgressBar` or by instantiating `SingleProgressBar` and
        subsequently calling it with the arguments for `func`.
        
        
        Parameters
        ----------
        func : callable
            The method that is wrapped/decorated. It must accept the
            two keyword-arguments `count` and `max_count`. The method
            `func` increments `count.value` up to `max_count.value`.
        *args : list
            Arguments for `jobmanager.ProgressBar`.
        **kwargs : dict
            Keyword-arguments for `jobmanager.ProgressBar`.
        
        
        Notes
        -----
        `func` must accept `count` and `max_count` and properly set
        their `.value` properties. This wrapper automatically creates
        the necessary `multiprocessing.Value` objects.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs
        # works with Python 2.7 and 3.3
        valid = func.__code__.co_varnames[:func.__code__.co_argcount]
        if not ( "count" in valid and "max_count" in valid ):
            raise ValueError(
                  "The wrapped function `{}` ".format(func.func_name)+
                  "must accept the arguments `count` and `max_count`,"+
                  " but it only accepts {}.".format(valid))
        
        
    def __call__(self, *args, **kwargs):
        """ Calls `func` - previously defined in `__init__`.
        
        Parameters
        ----------
        *args : list
            Arguments for `func`.
        **kwargs : dict
            Keyword-arguments for `func`.
        """
        count = progress.UnsignedIntValue()
        max_count = progress.UnsignedIntValue(1000)
        with progress.ProgressBar(count, max_count, 
                                       *self.args, **self.kwargs) as pb:
            pb.start()
            return self.func(*args, count=count, max_count=max_count,
                        **kwargs)



@SingleProgressBar
def _my_func_1(arg, kwarg="1", count=None, max_count=None):
    maxval = 100
    if max_count is not None:
        max_count.value = maxval
        
    for i in range(maxval):
        if count is not None:
            count.value += 1
            
        time.sleep(0.05)

    return arg+kwarg


def _my_func_2(arg, kwarg="2", count=None, max_count=None):
    maxval = 100
    if max_count is not None:
        max_count.value = maxval
        
    for i in range(maxval):
        if count is not None:
            count.value += 1
            
        time.sleep(0.05)

    return arg+kwarg


def _test_SingleProgressBar():
    result1 = _my_func_1("decorated function", kwarg=" 1")
    print(result1)
    
    wrapper = SingleProgressBar(_my_func_2, interval=.1)
    result2 = wrapper("wrapped function", kwarg=" 2")
    print(result2)


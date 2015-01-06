#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Implements decorators/wrappers for simple use-cases of jobmanager.
"""
from __future__ import division, print_function

import multiprocessing as mp
import time


from . import progress

from .jobmanager import getCountKwargs, validCountKwargs

__all__ = ["ProgressBar"]



class ProgressBar(object):
    """ A wrapper/decorator with a text-based progress bar.
    
    Methods:
    - __init__
    - __call__
    
    
    Examples
    --------
    
    >>> from jobmanager.decorators import ProgressBar
    >>> import time
    >>> 
    >>> @ProgressBar
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
    
    >>> wrapper = ProgressBar(my_func, interval=.1)
    >>> result = wrapper("wrapped function", kwarg=" test")
    
    """
    def __init__(self, func, *args, **kwargs):
        """ Initiates the wrapper objet.
        
        A function can be wrapped by decorating it with
        `ProgressBar` or by instantiating `ProgressBar` and
        subsequently calling it with the arguments for `func`.
        
        
        Parameters
        ----------
        func : callable
            The method that is wrapped/decorated. It must accept the
            two keyword-arguments `count` and `max_count` (or `c` and
            `m`). The method `func` increments `count.value` up to
            `max_count.value` (`c.value`, `m.value`).
        *args : list
            Arguments for `jobmanager.ProgressBar`.
        **kwargs : dict
            Keyword-arguments for `jobmanager.ProgressBar`.
        
        
        Notes
        -----
        `func` must accept `count` and `max_count` (or `c`, `m`) and 
        properly set their `.value` properties. This wrapper
        automatically creates the necessary `multiprocessing.Value`
        objects.
        """
        self.__name__ = func.__name__ # act like the function
        self.func = func
        self.args = args
        self.kwargs = kwargs
        # works with Python 2.7 and 3.3
        valid = func.__code__.co_varnames[:func.__code__.co_argcount]
        # Check arguments
        self.cm = getCountKwargs(func)
        if self.cm is None:
            raise ValueError(
                  "The wrapped function `{}` ".format(func.func_name)+
                  "must accept one of the folling pairs of "+
                  "keyword arguments:{}".format(validCountKwargs))

        
    def __call__(self, *args, **kwargs):
        """ Calls `func` - previously defined in `__init__`.
        
        Parameters
        ----------
        *args : list
            Arguments for `func`.
        **kwargs : dict
            Keyword-arguments for `func`.
        """
        # check if the kwarg is already given 
        # (e.g. by a function that is nested.
        if not kwargs.has_key(self.cm[0]) or kwargs[self.cm[0]] is None:
            # count
            kwargs[self.cm[0]] = progress.UnsignedIntValue(0)
        if not kwargs.has_key(self.cm[1]) or kwargs[self.cm[1]] is None:
            # max_count
            kwargs[self.cm[1]] = progress.UnsignedIntValue(0)
        with progress.ProgressBar(kwargs[self.cm[0]], kwargs[self.cm[1]],
                                  *self.args, **self.kwargs) as pb:
            pb.start()
            return self.func(*args, **kwargs)



def decorate_module_ProgressBar(module, **kwargs):
    """ Decorates all decoratable functions in a module with a
    ProgressBar.
    
    You can prevent wrapping of a function by not specifying the keyword
    arguments as defined in `jobmanager.jobmanager.validCountKwargs` or
    by defining a function `_jm_decorate_{func}".
    
    **kwargs are keyword arguments for ProgressBar
    
    Note that decorating all functions in a module might lead to
    strange behavior of the progress bar for nested functions.
    """
    vdict = module.__dict__
    for key in list(vdict.keys()):
        if hasattr(vdict[key], "__call__"):
            if getCountKwargs(vdict[key]) is not None:
                newid = "_jm_decorate_{}".format(key)
                if hasattr(module, newid):
                    warings.warn("Wrapping of {} prevented by module.".
                                 format(key))
                else:
                    # copy old function
                    setattr(module, newid, vdict[key])
                    # create new function
                    wrapper = ProgressBar(getattr(module, newid), **kwargs)
                    # set new function
                    setattr(module, key, wrapper)
                    if (kwargs.has_key("verbose") and
                        kwargs["verbose"] > 0):
                        print("Jobmanager wrapped {}.{}".format(
                                                  module.__name__, key))
                    

@ProgressBar
def _my_func_1(arg, kwarg="1", count=None, max_count=None):
    maxval = 100
    if max_count is not None:
        max_count.value = maxval
        
    for i in range(maxval):
        if count is not None:
            count.value += 1
            
        time.sleep(0.02)

    return arg+kwarg


def _my_func_2(arg, c, m, kwarg="2"):
    maxval = 100
    m.value += maxval
        
    for i in range(maxval):
        c.value += 1
            
        time.sleep(0.02)

    return arg+kwarg


def _test_ProgressBar():
    result1 = _my_func_1("decorated function", kwarg=" 1")
    print(result1)
    
    wrapper = ProgressBar(_my_func_2, interval=.1)
    result2 = wrapper("wrapped function", kwarg=" 2")
    print(result2)


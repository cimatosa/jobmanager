#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Implements decorators/wrappers for simple use-cases of jobmanager.
"""
from __future__ import division, print_function

from inspect import getcallargs
import warnings

from . import progress
from .jobmanager import getCountKwargs, validCountKwargs

#__all__ = ["ProgressBar", "ProgressBarOverrideCount"]


class ProgressBar(object):
    """ A wrapper/decorator with a text-based progress bar.
    
    Methods:
    - __init__
    - __call__
    
    The idea is to add a status bar for a regular
    function just by wrapping the function via
    python's decorator syntax.
    
    In order to do so, the function needs to provide some
    extra information, namely the current state 'count' and
    the final state 'max_count'. Simply expand your function
    by these two additional keyword arguments (or other pairs
    specified in jobmanager.validCountKwargs) and set their
    values during the calculation (see example 1 below). In that
    manner the decorated function as well as the not decorated
    function can simple be called as one would not care about
    any status information.
    
    Alternatively one could explicitly set count and max_count
    in the function call, which circumvents the need to change
    the value of max_count AFTER instantiation of the progressBar.    
    
    
    Example 1
    ---------
    
    >>> from jobmanager.decorators import ProgressBar
    >>> from jobmanager.decorators.progress import UnsignedIntValue
    >>> import time
    >>> 
    >>> @ProgressBar
    >>> def my_func_1(arg, 
    >>>               kwarg     = "1", 
    >>>               count     = UnsignedIntValue(val=0), 
    >>>               max_count = UnsignedIntValue(val=1)):
    >>>     # max_count should as default always be set to a value > 0 
    >>>     maxval = 100
    >>>     max_count.value = maxval
    >>> 
    >>>     for i in range(maxval):
    >>>         count.value += 1
    >>>         time.sleep(0.02)
    >>> 
    >>>     return arg+kwarg
    >>> 
    >>> my_func_1("one argument", kwarg=" second argument")
    # The progress of my_func is monitored on stdout.
    one argument second argument
    
    Example 2
    ---------
    
    >>> from jobmanager.decorators import ProgressBar
    >>> from jobmanager.decorators.progress import UnsignedIntValue
    >>> import time
    >>> 
    >>> @ProgressBar
    >>> def my_func(c, m):
    >>>     for i in range(m.value):
    >>>         c.value = i
    >>>         time.sleep(0.02)
    >>>             
    >>> c = progress.UnsignedIntValue(val=0)
    >>> m = progress.UnsignedIntValue(val=100)
    >>> my_func(c, m)
    
    Notes
    -----
    You can also use this class as a wrapper and tune parameters of the
    progress bar.
    
    >>> wrapper = ProgressBar(my_func, interval=.1)
    >>> result = wrapper("wrapped function", kwarg=" test")
    
    """
    def __init__(self, func, **kwargs):
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
        self.__doc__ = func.__doc__ # copy doc string
        self.func = func
        self.kwargs = kwargs
        # Check arguments
        self.cm = getCountKwargs(func)
        if self.cm is None:
            raise ValueError(
                  "The wrapped function `{}` ".format(func.func_name)+
                  "must accept one of the following pairs of "+
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
        
        # Bind the args and kwds to the argument names of self.func
        callargs = getcallargs(self.func, *args, **kwargs)
        
        count = callargs[self.cm[0]]
        max_count = callargs[self.cm[1]]
         
        with progress.ProgressBar(count     = count, 
                                  max_count = max_count,
                                  prepend   = "{} ".format(self.__name__),
                                  **self.kwargs) as pb:
            pb.start()
            return self.func(**callargs)



class ProgressBarExtended(ProgressBar):
    """
        extends the ProgressBar such that
        
        on can turn of the ProgressBar by giving an extra argument,
        namely 'progress_bar_off' and set its value to 'True'.
        
        further there will be an additional argument passed to the function
        called 'progress_bar' which allows to stop the progress bar from
        within the function. note that there will be an function signature error
        if the function does not accept the extra argument 'progress_bar'. So a
        general **kwargs at the end of the functions arguments will help.
        That is also the reason why the extended version comes in an extra class
        because it might otherwise break compatibility.
        
        Example
        -------
        
        >>> import jobmanager as jm
        
        >>> c = jm.progress.UnsignedIntValue(val=0)
        >>> m = jm.progress.UnsignedIntValue(val=20)
    
        >>> @jm.decorators.ProgressBarExtended    # choose 'ProgressBarExtended'
        >>> def my_func_kwargs(c, m, **kwargs):   # simply add '**kwargs' here
        >>>     for i in range(m.value):
        >>>         c.value = i+1
        >>>         time.sleep(0.1)
        
        >>> # same as when using ProgressBar
        >>> my_func_kwargs(c, m)
        
        >>> # a simple kwarg will switch the progressBar off
        >>> my_func_kwargs(c, m, progress_bar_off=True)
    """
    def __call__(self, *args, **kwargs):
        # Bind the args and kwds to the argument names of self.func
        callargs = getcallargs(self.func, *args, **kwargs)
        
        progress_bar_off = False
        try:
            progress_bar_off = callargs['progress_bar_off']
        except KeyError:
            pass
    
        try:
            progress_bar_off = callargs['kwargs']['progress_bar_off']
        except KeyError:
            pass
                    
        if progress_bar_off:
            return self.func(**callargs)
        
        
        count = callargs[self.cm[0]]
        max_count = callargs[self.cm[1]]
         
        with progress.ProgressBar(count     = count, 
                                  max_count = max_count,
                                  prepend   = "{} ".format(self.__name__),
                                  **self.kwargs) as pb:
            pb.start()
            callargs['progress_bar'] = pb
            return self.func(**callargs)



class ProgressBarOverrideCount(ProgressBar):
    def __call__(self, *args, **kwargs):
        """ Calls `func` - previously defined in `__init__`.
        
        same as in ProgressBar class except that the default
        value `None` of count and max_count will cause
        count to be set to `UnsignedIntValue(val=0)`
        and max_count to `UnsignedIntValue(val=1)`.
        
        So even if the function to be decorated
        will be called with arguments c = None and m = None
        the actual call due to the modification of the decorator
        will be with arguments c = UIV(0) and m = UIV(1).
        
        Parameters
        ----------
        *args : list
            Arguments for `func`.
        **kwargs : dict
            Keyword-arguments for `func`.
            
        Example
        -------
        
        see tests/test_decorators.py
        """
        
        # Bind the args and kwds to the argument names of self.func
        callargs = getcallargs(self.func, *args, **kwargs)
        
        count = callargs[self.cm[0]]
        if count is None:
            count = progress.UnsignedIntValue(val=0)
            callargs[self.cm[0]] = count
        
        max_count = callargs[self.cm[1]]
        if max_count is None:
            max_count = progress.UnsignedIntValue(val=1)
            callargs[self.cm[1]] = max_count
         
        with progress.ProgressBar(count     = count, 
                                  max_count = max_count,
                                  prepend   = "{} ".format(self.__name__),
                                  **self.kwargs) as pb:
            pb.start()
            return self.func(**callargs)    

    
    
def decorate_module_ProgressBar(module, decorator=ProgressBar, **kwargs):
    """ Decorates all decoratable functions in a module with a
    ProgressBar.
    
    You can prevent wrapping of a function by not specifying the keyword
    arguments as defined in `jobmanager.jobmanager.validCountKwargs` or
    by defining a function `_jm_decorate_{func}".

    Parameters
    ----------
    module : Python module
        The module whose functions should be decorated.
    decorator : bool
        Specifies a decorator in jobmanager.decorators that should be
        used.
    **kwargs : dict
        Keyword arguments to the ProgressBar.
    
    Notes
    -----
    Decorating nested functions in a module might lead to unexpected
    behavior.
    """
    if kwargs.has_key("override_count"):
        warnings.warn("`override_count` will be removed. Please use "+\
                  "`decorator=jm.decorators.ProgressBarOverrideCount`.",
                  FutureWarning)
        if kwargs["override_count"]:
            decorator = ProgressBarOverrideCount
        else:
            decorator = ProgressBar
        kwargs.pop("override_count")
    
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
                    wrapper = decorator(getattr(module, newid), **kwargs)
                    # set new function
                    setattr(module, key, wrapper)
                    if (kwargs.has_key("verbose") and
                        kwargs["verbose"] > 0):
                        print("Jobmanager wrapped {}.{}".format(
                                                  module.__name__, key))


#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The clients module

This module provides special subclasses of the JobManager_Client
"""

import os
import sys
import traceback

from .jobmanager import JobManager_Client
from . import ode_wrapper



class Integration_Client_CPLX(JobManager_Client):
    """
        A JobManager_Client subclass to integrate a set of complex valued ODE.
        
        'arg' as well as 'const_arg' which are passed from the JobManager_Server
        to the JobManager_Client's function 'func' must be hashable dictionaries
        (see for example jobmanager.HashDict). The updated dictionary kwargs
         
            kwargs = {}
            kwargs.update(const_arg)
            kwargs.update(arg)
        
        will hold the keyword arguments passed to ode_wrapper.integrate_cplx.
        This implies that the keys of kwargs MUST include
            
            t0            : initial time
            t1            : final time
            N             : number of time steps for the solution x(t)
                            t = linspace(t0, t1, N)
            f             : function holding the derivatives 
            args          : additional positional arguments passed to f(t, x, *args)
            x0            : initial value
            integrator    : type of integration method
                              'zvode': complex version of vode,
                                       in case of stiff ode, f needs to be analytic
                                       see also scipy.integrate.ode -> 'zvode'
                                       most efficient for complex ODE
                              'vode', 'lsoda': both do automatic conversion from the
                                       complex ODE to the doubly dimensioned real
                                       system of ODE, and use the corresponding real
                                       integrator methods.
                                       Might be of the order of one magnitude slower
                                       that 'zvode'. Consider using Integration_Client_REAL
                                       in the first place.  
                                       
        optional keys are:
            verbose        : default 0
            integrator related arguments (see the scipy doc ODE)
            
        As the key 'args' itself has a tuple as value, it's composition used
        instead of a simple update. So 
        
            kwargs['args'] = arg['args'] + const_arg['args']
            
        which means that the call signature of f has to be
        f(t, x, arg_1, arg_2, ... const_arg_1, const_arg_2, ...) 
    """
    def __init__(self, **kwargs):
        super(Integration_Client_CPLX, self).__init__(**kwargs)
        
    @staticmethod
    def func(arg, const_arg, c, m):
        # construct the arguments passed to the DGL defining function
        # from the two tuples coming from arg and const_arg
        # e.g. 
        #     arg['args'] = (2, 'low')
        #     const_arg['args'] = (15, np.pi)
        # f will be called with
        # f(t, x, 2, 'low', 15, np.pi)
        args_dgl = tuple()
        try:
            args_dgl += arg['arg']
        except KeyError:
            pass
        try:
            args_dgl += const_arg['arg']
        except KeyError:
            pass
        
        # construct the rest of the arguments needed to call the integrator
        # by interpreting arg and const_arg ad kwargs
        kwargs = {}
        kwargs.update(const_arg)
        kwargs.update(arg)
        
        m.value = kwargs['N']
        
        # remove key 'args' since it has been constructed explicitly
        if 'arg' in kwargs: 
            kwargs.pop('arg')
            
        # t0, t1, N, f, args, x0, integrator, verbose, c, **kwargs
        return ode_wrapper.integrate_cplx(c=c, args=args_dgl, **kwargs)
    
    
    
class Integration_Client_REAL(JobManager_Client):
    """
        A JobManager_Client subclass to integrate a set of complex real ODE.
        
        same behavior as described for Integration_Client_CPLX except
        that 'vode' and 'lsoda' do not do any wrapping, so there is no
        performance issue and 'zvode' is obviously not supported. 
    """
    def __init__(self, **kwargs):
        super(Integration_Client_REAL, self).__init__(**kwargs)
        
    @staticmethod
    def func(arg, const_arg, c, m):
        # construct the arguments passed to the DGL defining function
        # from the two tuples coming from arg and const_arg
        # e.g. 
        #     arg['args'] = (2, 'low')
        #     const_arg['args'] = (15, np.pi)
        # f will be called with
        # f(t, x, 2, 'low', 15, np.pi)
        args_dgl = tuple()
        try:
            args_dgl += arg['arg']
        except KeyError:
            pass
        try:
            args_dgl += const_arg['arg']
        except KeyError:
            pass
        
        # construct the rest of the arguments needed to call the integrator
        # by interpreting arg and const_arg ad kwargs
        kwargs = {}
        kwargs.update(const_arg)
        kwargs.update(arg)
        
        m.value = kwargs['N'] 
        
        # remove key 'args' since it has been constructed explicitly
        if 'arg' in kwargs: 
            kwargs.pop('arg')
            
        try:
            const_arg['call_back'](arg, const_arg)
        except KeyError:
            pass
            
        # t0, t1, N, f, args, x0, integrator, verbose, c, **kwargs
        return ode_wrapper.integrate_real(c=c, args=args_dgl, **kwargs)
    
    
    
class FunctionCall_Client(JobManager_Client):
    @staticmethod
    def func(arg, const_arg, c, m):
        f = const_arg['f']
        f_kwargs = {}
        f_kwargs.update(arg)
        f_kwargs.update(const_arg)
        f_kwargs.pop('f')
        f_kwargs['__c'] = c
        f_kwargs['__m'] = m
        
        return f(**f_kwargs)
        
        
    

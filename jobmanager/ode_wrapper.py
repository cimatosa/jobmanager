#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy as np
import warnings
from time import time
import logging
import sys

# taken from here: https://mail.python.org/pipermail/python-list/2010-November/591474.html
class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        _str = logging.Formatter.format(self, record)
        header = _str.split(record.message)[0]
        _str = _str.replace('\n', '\n' + ' '*len(header))
        return _str
   

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
console_hand = logging.StreamHandler(stream = sys.stderr)
console_hand.setLevel(logging.DEBUG)
fmt = MultiLineFormatter('%(asctime)s %(name)s %(levelname)s : %(message)s')
console_hand.setFormatter(fmt)
log.addHandler(console_hand)



try:
    from scipy.integrate import ode
except ImportError as e:
    warnings.warn("Submodule 'ode_wrapper' will not work. Reason: {}.".format(e))



class Dummy_c(object):
    def __init__(self):
        self.value = 0
        pass

def complex_to_real(vc):
    return np.hstack([np.real(vc), np.imag(vc)])

def real_to_complex(vr):
    n = len(vr)//2
    return vr[:n] + 1j*vr[n:]

def wrap_complex_intgeration(f_complex):
    """
    if f: R x C^n -> C^n
    then this functions returns the real equivalent
    f_prime R x R^n x R^n -> R^n x R^n
    
    such that a complex vector
        cc = [vc_1, ... vc_n]
    translates to
        cr = [RE(vc_1), ... RE(vc_n), IM(vc_1), ... IM(vc_n)] 
    """
    def f_real(t, yr):
        return complex_to_real( f_complex(t, real_to_complex(yr)) ) 
    
    return f_real
    

def integrate_cplx(c, t0, t1, N, f, args, x0, integrator, verbose=0, res_dim=None, x_to_res=None, **kwargs):
    f_partial_complex = lambda t, x: f(t, x, *args)
    if integrator == 'zvode':
        # define complex derivative
        f_ = f_partial_complex
        x0_ = x0
    elif (integrator == 'vode') | (integrator == 'lsoda') | (integrator == 'dopri5') | (integrator == 'dop853'): 
        # define real derivative (separation for real and imaginary part)
        f_ = lambda t, x: wrap_complex_intgeration(f_partial_complex)(t, x)
        x0_ = complex_to_real(x0)
        log.warning("PERFORMANCE WARNING, avoid using 'vode' or 'lsoda' for complex ode's")
    else:
        raise RuntimeError("unknown integrator '{}'".format(integrator))
    
    r = ode(f_)
    
    if (integrator == 'dopri5') | (integrator == 'dop853'):
        if 'order' in kwargs:
            del kwargs['order']
    
    kws = list(kwargs.keys())
    for kw in kws:
        if kwargs[kw] is None:
            del kwargs[kw]
    
    r.set_integrator(integrator, **kwargs)
    
    # x0_ might be the mapping from C to R^2
    r.set_initial_value(x0_, t0)
    
    t = np.linspace(t0, t1, N)
    
    if res_dim is None:
        res_dim = (len(x0), )
    else:
        try:
            res_list_len = len(res_dim)
            assert res_list_len == len(x_to_res)
        except TypeError:
            res_list_len = None 
    
    if x_to_res is None:
        x_to_res = lambda t_, x_: x_
        
    # the usual case with only one result type
    if res_list_len is None: 
    
        # complex array for result
        x = np.empty(shape=(N,) + res_dim, dtype=np.complex128)
        x[0] = x_to_res(t0, x0)
        
    #         print(args.eta._Z)
    
        t_int = 0
        t_conv = 0
        
        i = 1        
        while r.successful() and i < N:
            _t = time()
            r.integrate(t[i])
            t_int += (time()-_t)
            
            _t = time()
            if integrator == 'zvode':
                # complex integration -> yields complex values
                x[i] = x_to_res(r.t, r.y)
            else:
                # real integration -> mapping from R^2 to C needed
                x[i] = x_to_res(r.t, real_to_complex(r.y))
            t_conv += (time()-_t)            
                
            t[i] = r.t
            c.value = i
            i += 1
    
        if not r.successful():
            log.warning("INTEGRATION WARNING, NOT successful!")
            
    # having to compute multiple result types
    else:
        # complex array for result
        x = []
        for a in range(res_list_len):
            x.append(np.empty(shape=(N,) + res_dim[a], dtype=np.complex128))
            x[-1][0] = x_to_res[a](t0, x0)
        
    #         print(args.eta._Z)
    
        t_int = 0
        t_conv = 0
        
        i = 1        
        while r.successful() and i < N:
            _t = time()
            r.integrate(t[i])
            t_int += (time()-_t)
            
            _t = time()
            if integrator == 'zvode':
                # complex integration -> yields complex values
                for a in range(res_list_len):
                    x[a][i] = x_to_res[a](r.t, r.y)
            else:
                # real integration -> mapping from R^2 to C needed
                for a in range(res_list_len):
                    x[a][i] = x_to_res[a](r.t, real_to_complex(r.y))
            t_conv += (time()-_t)
            log.debug("step {}: integration:{:.2%} conversion:{:.2%}".format(i, t_int / (t_int + t_conv), t_conv / (t_int + t_conv)))            
                
            t[i] = r.t
            c.value = i
            i += 1
    
        if not r.successful():
            log.warning("INTEGRATION WARNING, NOT successful!")        
    
    log.info("integration summary\n"+
             "integration     time {:.2g}s ({:.2%})\n".format(t_int, t_int / (t_int + t_conv))+
             "data conversion time {:.2g}s ({:.2%})\n".format(t_conv, t_conv / (t_int + t_conv)))
    return t, x
        
def integrate_real(c, t0, t1, N, f, args, x0, integrator, verbose=0, res_dim=None, x_to_res=None, **kwargs):
    f_partial = lambda t, x: f(t, x, *args)
    if integrator == 'zvode':
        # define complex derivative
        raise RuntimeError("'zvode' can not be used for real integration")
    elif (integrator == 'vode') | (integrator == 'lsoda'): 
        pass
    else:
        raise RuntimeError("unknown integrator '{}'".format(integrator))
    
    r = ode(f_partial)
    
    kws = list(kwargs.keys())
    for kw in kws:
        if kwargs[kw] is None:
            del kwargs[kw]
    
    r.set_integrator(integrator, **kwargs)
    
    # x0_ might be the mapping from C to R^2
    r.set_initial_value(x0, t0)
    
    t = np.linspace(t0, t1, N)
    
    if res_dim is None:
        res_dim = (len(x0), )
    
    if x_to_res is None:
        x_to_res = lambda t_, x_: x_ 
    
    # float array for result
    x = np.empty(shape=(N,) + res_dim, dtype=np.float64)
    x[0] = x_to_res(t0, x0)
    
    t_int = 0
    t_conv = 0
    
    i = 1        
    while r.successful() and i < N:
        _t = time()
        r.integrate(t[i])
        t_int += (time()-_t)
        
        _t = time()
        x[i] = x_to_res(r.t, r.y)
        t_conv += (time()-_t)
        log.debug("step {}: integration:{:.2%} conversion:{:.2%}".format(i, t_int / (t_int + t_conv), t_conv / (t_int + t_conv)))
        
        t[i] = r.t
        c.value = i
        i += 1

    if not r.successful():
        log.warning("INTEGRATION WARNING, NOT successful!")

    log.info("integration summary\n"+
             "integration     time {:.2g}s ({:.2%})\n".format(t_int, t_int / (t_int + t_conv))+
             "data conversion time {:.2g}s ({:.2%})\n".format(t_conv, t_conv / (t_int + t_conv)))    
        
    return t, x

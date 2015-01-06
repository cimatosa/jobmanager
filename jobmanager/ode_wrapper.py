from scipy.integrate import ode
import numpy as np

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
    elif (integrator == 'vode') | (integrator == 'lsoda'): 
        # define real derivative (separation for real and imaginary part)
        f_ = lambda t, x: wrap_complex_intgeration(f_partial_complex)(t, x)
        x0_ = complex_to_real(x0)
        if verbose > 0:
            print("PERFORMANCE WARNING, avoid using 'vode' or 'lsoda' for complex ode's")
    else:
        raise RuntimeError("unknown integrator '{}'".format(integrator))
    
    r = ode(f_)
    r.set_integrator(integrator, **kwargs)
    
    # x0_ might be the mapping from C to R^2
    r.set_initial_value(x0_, t0)
    
    t = np.linspace(t0, t1, N)
    
    if res_dim is None:
        res_dim = (len(x0), )
    
    if x_to_res is None:
        x_to_res = lambda t_, x_: x_ 
    
    # complex array for result
    x = np.empty(shape=(N,) + res_dim, dtype=np.complex128)
    x[0] = x_to_res(t0, x0)
    
#         print(args.eta._Z)
    
    i = 1        
    while r.successful() and i < N:
        r.integrate(t[i])
        if integrator == 'zvode':
            # complex integration -> yields complex values
            x[i] = x_to_res(r.t, r.y)
        else:
            # real integration -> mapping from R^2 to C needed
            x[i] = x_to_res(r.t, real_to_complex(r.y))
            
        t[i] = r.t
        c.value = i
        i += 1

    if not r.successful():
        print("INTEGRATION WARNING, NOT successful!")
    
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
    
    i = 1        
    while r.successful() and i < N:
        r.integrate(t[i])
        x[i] = x_to_res(r.t, r.y)
        t[i] = r.t
        c.value = i
        i += 1

    if not r.successful():
        print("INTEGRATION WARNING, NOT successful!")        
    return t, x

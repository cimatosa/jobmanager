import jobmanager as jm
import numpy as np
from scipy.special import gamma as scipy_gamma_func
from scipy.optimize import minimize
import time
import matplotlib.pyplot as plt

def alpha_func(tau, eta, Gamma, s):
    return eta * (Gamma / (1 + 1j*tau))**(s+1) * scipy_gamma_func(s)

def x_to_g_w(x):
    n = len(x) // 3
    g_re = x[0*n:1*n]
    g_im = x[1*n:2*n]
    w    = x[2*n:3*n]
    return g_re + 1j*g_im, w, n

def alpha_apprx(tau, g, w):
    """
        alpha = sum_i   g_i * exp(-w_i * tau)
    """
    n_tau = len(tau)
    tau = tau.reshape((n_tau,1))
    
    res = np.sum(g * np.exp(-w * tau), axis=1)
    return res

def alpha_apprx_x(tau, x):
    g, w, n = x_to_g_w(x)
    g = g.reshape((1,n))
    w = w.reshape((1,n))
    return alpha_apprx(tau, g, w)
    


def diff(x, tau, alpha_true, p):
    alpha = alpha_apprx_x(tau, x)
    rel_diff = np.abs(alpha_true - alpha)/np.abs(alpha_true)
    max_diff = np.max(rel_diff)
    mean_rel_diff = np.mean(rel_diff)
    return p*mean_rel_diff + (1-p)*max_diff
    

class FitFunc_Client(jm.JobManager_Client):
    def __init__(self):
        super(FitFunc_Client, self).__init__(ip="localhost", 
                                             authkey='fit function example', 
                                             port = 42524, 
                                             nproc = 0, 
                                             nice=19, 
                                             no_warings=True, 
                                             verbose=1)
        
    @staticmethod
    def func(args, const_args):
        eta, Gamma, s, p, tau_max, tau_n = const_args
        tau = np.linspace(0, tau_max, tau_n)
        alpha_true = alpha_func(tau, eta, Gamma, s)
        
        f_min = lambda x: diff(x, tau, alpha_true, p)
        
        res = minimize(fun=f_min, 
                       x0=np.array(args),
                       method="BFGS")
        
        return res.x, res.fun
        

class FitFunc_Server(jm.JobManager_Server):
    def __init__(self, const_args, num_samples, n, g_max, w_max):
        
        # setup init parameters for the ancestor class
        authkey = 'fit function example'
        fname_for_final_result_dump = 'FitFunc_final_result'
        const_args = const_args
        port = 42524 
        verbose = 2
        msg_interval = 1
        fname_for_args_dump = 'auto'
        fname_for_fail_dump = 'auto'
        
        # init ancestor class
        super(FitFunc_Server, self).__init__(authkey=authkey,
                                       fname_for_final_result_dump=fname_for_final_result_dump,
                                       const_args = const_args,
                                       port = port,
                                       verbose = verbose,
                                       msg_interval = msg_interval,
                                       fname_for_args_dump = fname_for_args_dump,
                                       fname_for_fail_dump = fname_for_fail_dump)

        self.final_result = None
        for i in range(num_samples):
            g_re = (np.random.rand(n)*2 - 1)*g_max
            g_im = (np.random.rand(n)*2 - 1)*g_max
            w = (np.random.rand(n)*2 - 1)*w_max
            x0 = tuple(g_re) + tuple(g_im) + tuple(w)   # need tuple instead if numpy ndarray
                                                        # here because it needs to be hashable 
            self.put_arg(x0)
            
        
    def process_new_result(self, arg, result):
        x, fun = result
        g, w, n = x_to_g_w(x)
        if ((self.final_result == None) or
            (self.final_result[0] > fun)):
            
            self.final_result = (fun, g, w)
            
    def process_final_result(self):
        if self.final_result != None:
            eta, Gamma, s, p, tau_max, tau_n = self.const_args
            (fun, g, w) = self.final_result
            
            tau = np.linspace(0, tau_max, tau_n)
            alpha_true = alpha_func(tau, eta, Gamma, s)
            alpha = alpha_apprx(tau, g, w)
            
            plt.plot(tau, np.real(alpha_true), c='k', label='true')
            plt.plot(tau, np.imag(alpha_true), c='k', ls='--')
            plt.plot(tau, np.real(alpha), c='r', label='approx')
            plt.plot(tau, np.imag(alpha), c='r', ls='--')
            
            plt.legend()
            plt.grid()
            plt.show()
        

if __name__ == "__main__":
    debug = False

    eta = 1
    Gamma = 1
    s = 0.7
    p = 0.5
    tau_max = 2 
    tau_n = 500
    fitfunc_server = FitFunc_Server(const_args=(eta, Gamma, s, p, tau_max, tau_n),
                                    num_samples=500, 
                                    n=5,
                                    g_max=100, 
                                    w_max=10)  
   
    if debug:
        # for debug reasons only
        # note the server does not get started, but as the init
        # function of the subclass generates the arguments
        # we can check if they can be process by the 
        # clinet's static function func
    
        arg0 = fitfunc_server.job_q.get()
        x, fun = FitFunc_Client.func(arg0, const_args=(eta, Gamma, s, p))
        print("arg0 :", arg0)
        print("x    :", x)
        print("fmin :", fun)
    else:
        fitfunc_server.start()
    

    
    
    

            
    
        

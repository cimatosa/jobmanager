#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import matplotlib.pyplot as plt
import numpy as np
import os
from os.path import split, dirname, abspath
from scipy.optimize import minimize
import sys

sys.path.append(split(split(dirname(abspath(__file__)))[0])[0])

import jobmanager as jm
from calculations import *


class FitFunc_Client(jm.JobManager_Client):
    def __init__(self):
        super(FitFunc_Client, self).__init__(server="localhost", 
                                             authkey='fit function example', 
                                             port = 42524, 
                                             nproc = 0, 
                                             nice=19, 
                                             no_warnings=True, 
                                             verbose=2)
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
        fname_dump = None
        const_args = const_args
        port = 42524 
        verbose = 1
        msg_interval = 1
        
        # init ancestor class
        super(FitFunc_Server, self).__init__(authkey=authkey,
                                             const_arg = const_args,
                                             port = port,
                                             verbose = verbose,
                                             msg_interval = msg_interval,
                                             fname_dump = fname_dump)

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
            print("\nnew result {}".format(fun))
            
            self.final_result = (fun, g, w)
            
    def process_final_result(self):
        print("final Res")
        if self.final_result != None:
            eta, Gamma, s, p, tau_max, tau_n = self.const_arg
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
        
args = {}

args['eta'] = 1
args['Gamma'] = 1
args['s'] = 0.7
args['p'] = 0.99
args['tau_max'] = 2 
args['tau_n'] = 500
args['num_samples'] = 300
args['n'] = 5
args['g_max'] = 10 
args['w_max'] = 5

const_args = (args['eta'], 
              args['Gamma'], 
              args['s'], 
              args['p'], 
              args['tau_max'], 
              args['tau_n'])

def FitFunc_Server_from_args():
    return  FitFunc_Server(const_args = const_args,
                           num_samples = args['num_samples'],
                           n = args['n'],
                           g_max = args['g_max'], 
                           w_max = args['w_max'])

if __name__ == "__main__":
    fitfunc_server = FitFunc_Server_from_args()
   
    # note the server does not get started, but as the init
    # function of the subclass generates the arguments
    # we can check if they can be process by the 
    # clinet's static function func

    arg0 = fitfunc_server.job_q.get()
    x, fun = FitFunc_Client.func(arg0, const_args=const_args)
    print("arg0 :", arg0)
    print("x    :", x)
    print("fmin :", fun)

    

    
    
    

            
    
        

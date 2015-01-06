#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function

import numpy as np
from scipy.special import gamma as scipy_gamma_func

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

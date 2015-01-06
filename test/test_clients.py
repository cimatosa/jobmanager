#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname, split

# Add parent directory to beginning of path variable
sys.path = [split(dirname(abspath(__file__)))[0]] + sys.path

import jobmanager as jm

from scipy.integrate import ode
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
import matplotlib.pyplot as plt
from scipy.special import mathieu_sem, mathieu_cem, mathieu_a, mathieu_b
import multiprocessing as mp
import time


def dgl_mathieu(t, f, a, q):
    f1, f2 = f[0], f[1]
    f1_dot = f2
    f2_dot = -(a - 2*q*np.cos(2*t))*f1
    return [f1_dot, f2_dot]


def solve_mathiue_dgl(t0, tmax, N, m, q):
    a = mathieu_a(m, q)
    y0 = mathieu_cem(m, q, 0)
    
    t = np.linspace(t0, tmax, N, endpoint=True)
    res = np.empty(shape=(3,N))
    res[0,0] = t[0]
    res[1,0], res[2,0] = y0
    
    r = ode(dgl_mathieu)
    r.set_integrator('lsoda', atol=1e-10, rtol=1e-10)
    r.set_initial_value(y=y0, t=t0)
    r.set_f_params(a, q)
     
    for i in range(1, N):
        r.integrate(t[i])
        res[0,i] = r.t
        res[1,i], res[2,i] = r.y
    return res
    

def test_mathiue_dgl(plot=False):
    t0 = 0
    tmax = 2*np.pi
    N = 401
    m = 3
    q = 5

    t1 = time.time()    
    res = solve_mathiue_dgl(t0, tmax, N, m, q)
    t = res[0,:]
    
    t2 = time.time()
    
    print((t2-t1)*500)
    
    y, yp = mathieu_cem(m, q, t*360/2/np.pi)
    
    
    rel_diff_y = np.abs(y - res[1,:])/np.abs(y)
    idx_sel = np.where(yp != 0)[0] 
    
    rel_diff_yp = np.abs(yp[idx_sel] - res[2,idx_sel])/np.abs(yp[idx_sel])
    
    assert np.max(rel_diff_y < 1e-4)
    assert np.max(rel_diff_yp < 1e-4)
    
    if plot:
    
        fig, ax = plt.subplots(nrows=2, ncols=1, sharex=True)
        
        ax[0].plot(t, y, c='k')
        ax[0].plot(t, res[1], c='r')
        ax[0].plot(t, yp, c='k')
        ax[0].plot(t, res[2], c='r')
        ax[0].grid()
        
        ax[1].plot(t, rel_diff_y)
        ax[1].plot(t[idx_sel], rel_diff_yp)
        
        ax[1].set_yscale('log')
        ax[1].grid()
        
        plt.show()
    
def test_distributed_mathieu():
    q_min = 0
    q_max = 15
    q_N = 50
    m = 3
    
    t0 = 0
    t1 = 2*np.pi
    
    N = q_N
    
    # t0, t1, N, f, args, x0, integrator, verbose
    const_arg = jm.hashDict()
    const_arg['t0'] = t0
    const_arg['t1'] = t1
    const_arg['N'] = N
    const_arg['f'] = dgl_mathieu
    const_arg['integrator'] = 'vode'
    const_arg['atol'] = 1e-10
    const_arg['rtol'] = 1e-10
    const_arg['verbose'] = 0
    
    authkey = 'integration_jm'
    
    with jm.JobManager_Local(client_class = jm.clients.Integration_Client_REAL,
                             authkey = authkey,
                             const_arg = const_arg,
                             nproc=1,
                             verbose_client=2,
                             niceness_clients=0,
                             show_statusbar_for_jobs=False) as jm_int:
        q_list = np.linspace(q_min, q_max, q_N)
        for q in q_list:
            arg = jm.hashDict()
            a = mathieu_a(m, q)
            arg['arg'] = (a, q)
            arg['x0'] = mathieu_cem(m, q, 0) # gives value and its derivative
            jm_int.put_arg(a=arg)        
        
        jm_int.start()
        
    data = np.empty(shape=(3, q_N*N), dtype=np.float64)
    
    t_ref = np.linspace(t0, t1, N)
    
    tot_time = 0
    
    max_diff_x = 0
    max_diff_x_dot = 0
    
        
    for i, f in enumerate(jm_int.final_result):
        arg = f[0]
        res = f[1]
        
        a, q = arg['arg']
        t, x_t = f[1]
        
        assert np.max(np.abs(t_ref - t)) < 1e-15
        time1 = time.time()
        res = solve_mathiue_dgl(t0, t1, N, m, q)
        time2 = time.time()
        tot_time += (time2 - time1)
        
        max_diff_x = max(max_diff_x, np.max(np.abs(x_t[:,0] - res[1,:])))
        max_diff_x_dot = max(max_diff_x_dot, np.max(np.abs(x_t[:,1] - res[2,:])))        
        
        data[0, i*q_N: (i+1)*q_N] = t
        data[1, i*q_N: (i+1)*q_N] = q
        data[2, i*q_N: (i+1)*q_N] = np.real(x_t[:,0])

    assert max_diff_x < 1e-6, max_diff_x       
    assert max_diff_x_dot < 1e-6, max_diff_x_dot 
    print("time normal integration:", tot_time)
    

#     fig = plt.figure(figsize=(15,10))
#     ax = fig.gca(projection='3d')
#     
#     ax.plot_trisurf(data[0], data[1], data[2], cmap=cm.jet, linewidth=0.2)
# 
#     plt.show()

class MYO(object):
    def __init__(self):
        self.a = (1,2,3)
        self.b = (56, -8.4)
        
    def __eq__(self, other):
        return False

from collections import namedtuple as nt
N1 = nt('N1', ['x', 'y'])

def test_tuple_equal():

            
    myo1 = MYO()
    
    t1 = (myo1, myo1)
    
    myo2 = MYO()
    
    t2 = (myo2, myo2)
   
    def f(x):
        return x**2
    
    def g(x):
        return x**2
    
    g = f
    import copy
    
    h = copy.deepcopy(f)
    
    print(g is f)
    print(h == f)
    

    

    na = N1(y=2., x=4)
    nb = N1(4,2)
    
    print(na == nb)
    
    import sqlitedict
    
    d = sqlitedict.SqliteDict(filename='tmp.db', tablename='test', autocommit=True)
    
    hash1 = hash(na)
    
    print(hash1, type(hash1))
    d[hash1] = na
    
    
    print(hash1 in d)
    print(str(hash1) in d)
    
    s = str(hash1)
    print(s, type(s))
    
    print(d[s])
    
    import pickle
    
    key_tuple = (2,3,4,'s', na)
    key_bytes = pickle.dumps(key_tuple)
    print(key_bytes)
    
    d[key_bytes] = 'tuple'
    
    print(d[key_bytes])
    
    
if __name__ == "__main__":
#     test_mathiue_dgl(plot=False)
#     test_distributed_mathieu()
    test_tuple_equal()


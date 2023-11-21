import numpy as np
import warnings
from time import perf_counter
import logging
import traceback
import copy

log = logging.getLogger(__name__)

from scipy.integrate import ode


class Dummy_c(object):
    def __init__(self):
        self.value = 0
        pass


def complex_to_real(vc):
    return np.hstack([np.real(vc), np.imag(vc)])


def real_to_complex(vr):
    n = len(vr) // 2
    return vr[:n] + 1j * vr[n:]


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
        return complex_to_real(f_complex(t, real_to_complex(yr)))

    return f_real


def timed_cnt_f(f, time_as_list):
    def new_f(t, x):
        t0 = perf_counter()
        res = f(t, x)
        t1 = perf_counter()
        time_as_list[0] += t1 - t0
        time_as_list[1] += 1
        return res

    return new_f


def integrate_cplx(
    c,
    t0,
    t1,
    N,
    f,
    args,
    x0,
    integrator,
    res_dim=None,
    x_to_res=None,
    scale_function=None,
    scale_condition=None,
    **kwargs
):
    f_partial_complex = lambda t, x: f(t, x, *args)
    if integrator == "zvode":
        # define complex derivative
        f_ = f_partial_complex
        x0_ = x0
    elif (
        (integrator == "vode")
        | (integrator == "lsoda")
        | (integrator == "dopri5")
        | (integrator == "dop853")
    ):
        # define real derivative (separation for real and imaginary part)
        f_ = lambda t, x: wrap_complex_intgeration(f_partial_complex)(t, x)
        x0_ = complex_to_real(x0)
        log.warning(
            "PERFORMANCE WARNING, avoid using 'vode' or 'lsoda' for complex ode's"
        )
    else:
        raise RuntimeError("unknown integrator '{}'".format(integrator))

    time_as_list = [0.0, 0]

    f__ = timed_cnt_f(f_, time_as_list)

    r = ode(f__)

    if (integrator == "dopri5") | (integrator == "dop853"):
        if "order" in kwargs:
            del kwargs["order"]

    kws = list(kwargs.keys())
    for kw in kws:
        if kwargs[kw] is None:
            del kwargs[kw]

    r.set_integrator(integrator, **kwargs)

    # x0_ might be the mapping from C to R^2
    r.set_initial_value(x0_, t0)

    t = np.linspace(t0, t1, N)

    if res_dim is None:
        res_dim = (len(x0),)
        res_list_len = None
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
        with warnings.catch_warnings():
            warnings.filterwarnings("error")
            try:
                while i < N:
                    _t = perf_counter()
                    r.integrate(t[i])
                    t_int += perf_counter() - _t

                    if not r.successful():
                        msg = "INTEGRATION WARNING: NOT successful!"
                        log.warning(msg)
                        raise Warning(msg)

                    _t = perf_counter()
                    r_y = copy.copy(r.y)
                    is_scaled = False
                    if scale_condition:
                        if scale_condition(r_y):
                            r_y = scale_function(r_y)
                            is_scaled = True

                    if integrator == "zvode":
                        # complex integration -> yields complex values
                        x[i] = x_to_res(r.t, r_y)
                    else:
                        # real integration -> mapping from R^2 to C needed
                        x[i] = x_to_res(r.t, real_to_complex(r_y))
                    t_conv += perf_counter() - _t

                    if abs(t[i] - r.t) > 1e-13:
                        msg = "INTEGRATION WARNING: time mismatch (diff at step {}: {:.3e})".format(
                            i, abs(t[i] - r.t)
                        )
                        log.warning(msg)
                        raise Warning(msg)
                    t[i] = r.t
                    c.value = i

                    if is_scaled:
                        del r
                        r = ode(f__)
                        r.set_integrator(integrator, **kwargs)
                        r.set_initial_value(y=r_y, t=t[i])

                    i += 1

            except Exception as e:
                trb = traceback.format_exc()
                return t[:i], x[:i], (e, trb)

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
        with warnings.catch_warnings():
            warnings.filterwarnings("error")
            try:
                while r.successful() and i < N:
                    _t = perf_counter()
                    r.integrate(t[i])
                    t_int += perf_counter() - _t

                    if not r.successful():
                        msg = "INTEGRATION WARNING: NOT successful!"
                        log.warning(msg)
                        raise Warning(msg)

                    _t = perf_counter()
                    r_y = copy.copy(r.y)
                    is_scaled = False
                    if scale_condition:
                        if scale_condition(r_y):
                            r_y = scale_function(r_y)
                            is_scaled = True
                    if integrator == "zvode":
                        # complex integration -> yields complex values
                        for a in range(res_list_len):
                            x[a][i] = x_to_res[a](r.t, r_y)
                    else:
                        # real integration -> mapping from R^2 to C needed
                        for a in range(res_list_len):
                            x[a][i] = x_to_res[a](r.t, real_to_complex(r_y))
                    t_conv += perf_counter() - _t
                    if abs(t[i] - r.t) > 1e-13:
                        msg = "INTEGRATION WARNING: time mismatch (diff at step {}: {:.3e})".format(
                            i, abs(t[i] - r.t)
                        )
                        log.warning(msg)
                        raise Warning(msg)
                    t[i] = r.t
                    c.value = i

                    if is_scaled:
                        del r
                        r = ode(f__)
                        r.set_integrator(integrator, **kwargs)
                        r.set_initial_value(y=scale_function(copy.copy(r_y)), t=t[i])

                    i += 1

            except Exception as e:
                trb = traceback.format_exc()
                return t[:i], [xa[:i] for xa in x], (e, trb)

    log.info(
        "integration summary\n"
        + "integration     time {:.3g}ms ({:.2%})\n".format(
            t_int * 1000, t_int / (t_int + t_conv)
        )
        + "    f_dot eval       {:.3g}ms ({:.2%})\n".format(
            time_as_list[0] * 1000, time_as_list[0] / (t_int + t_conv)
        )
        + "    f_dot eval cnt   {} -> average time per eval {:.3g}ms\n".format(
            time_as_list[1], time_as_list[0] * 1000 / time_as_list[1]
        )
        + "data conversion time {:.3g}ms ({:.2%})\n".format(
            t_conv * 1000, t_conv / (t_int + t_conv)
        )
    )
    return t, x, None


def integrate_real(
    c,
    t0,
    t1,
    N,
    f,
    args,
    x0,
    integrator,
    verbose=0,
    res_dim=None,
    x_to_res=None,
    **kwargs
):
    f_partial = lambda t, x: f(t, x, *args)
    if integrator == "zvode":
        # define complex derivative
        raise RuntimeError("'zvode' can not be used for real integration")
    elif (integrator == "vode") | (integrator == "lsoda"):
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
        res_dim = (len(x0),)

    if x_to_res is None:
        x_to_res = lambda t_, x_: x_

    # float array for result
    x = np.empty(shape=(N,) + res_dim, dtype=np.float64)
    x[0] = x_to_res(t0, x0)

    t_int = 0
    t_conv = 0

    i = 1
    with warnings.catch_warnings():
        warnings.filterwarnings("error")
        try:
            while r.successful() and i < N:
                _t = perf_counter()
                r.integrate(t[i])
                t_int += perf_counter() - _t

                _t = perf_counter()
                x[i] = x_to_res(r.t, r.y)
                t_conv += perf_counter() - _t
                if abs(t[i] - r.t) > 1e-13:
                    msg = "INTEGRATION WARNING: time mismatch (diff at step {}: {:.3e})".format(
                        i, abs(t[i] - r.t)
                    )
                    log.warning(msg)
                    raise Warning(msg)
                t[i] = r.t
                c.value = i
                i += 1

            if not r.successful():
                msg = "INTEGRATION WARNING: NOT successful!"
                log.warning(msg)
                raise Warning(msg)
        except Exception as e:
            trb = traceback.format_exc()
            return t[:i], x[:i], (e, trb)

    log.info(
        "integration summary\n"
        + "integration     time {:.2g}s ({:.2%})\n".format(
            t_int, t_int / (t_int + t_conv)
        )
        + "data conversion time {:.2g}s ({:.2%})\n".format(
            t_conv, t_conv / (t_int + t_conv)
        )
    )

    return t, x, None

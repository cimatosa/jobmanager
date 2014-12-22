#!/usr/bin/env python
# -*- coding: utf-8 -*-
__version__ = "0.1.0"

from .jobmanager import *

from . import clients
from . import servers
from . import progress

# ode_wrapper requires scipy
try:
    from . import ode_wrapper
except ImportError:
    warnings.warn("Submodule 'ode_wrapper' is not available."+\
                  " Reason: {}.".format(sys.exc_info()[1].message))

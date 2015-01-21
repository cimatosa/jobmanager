#!/usr/bin/env python
# -*- coding: utf-8 -*-
import warnings

from .jm_version import __version__

from .jobmanager import *

from . import clients
from . import decorators
from . import progress
from . import servers

# persistentData requires sqlitedict
try:
    from . import persistentData
except ImportError as e:
    warnings.warn("Submodule 'persistentData' is not available. Reason: {}.".format(e))

# ode_wrapper requires scipy
try:
    from . import ode_wrapper
except ImportError:
    warnings.warn("Submodule 'ode_wrapper' is not available. Reason: {}.".format(e))
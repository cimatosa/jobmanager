"""
.. include:: ../doc/description.txt

.. currentmodule:: jobmanager.jobmanager

Scheduling across different processes/machines is implemented in the
core modules :mod:`jobmanager.jobmanager`, :mod:`jobmanager.servers`,
and :mod:`jobmanager.clients`.

.. autosummary:: 
    JobManager_Client
    JobManager_Server

.. figure::  ../doc/artwork/server_client_communication.png
   :align:   center


Progress classes are implemented in the :mod:`jobmanager.progress`
submodule. Intuitive access to progress bars is facilitated with
decorators (:mod:`jobmanager.decorators`).


Fork `jobmanager on GitHib <https://github.com/cimatosa/jobmanager/>`_.

"""

import warnings

from .jobmanager import *

from . import clients
from . import servers
from . import ode_wrapper

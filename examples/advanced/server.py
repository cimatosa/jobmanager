#!/usr/bin/env python
# -*- coding: utf-8 -*-


from my_jobmanager_classes import FitFunc_Server_from_args

with FitFunc_Server_from_args() as fitfunc_server:
    fitfunc_server.start()

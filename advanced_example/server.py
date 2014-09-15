from example import FitFunc_Server_from_args

with FitFunc_Server_from_args() as fitfunc_server:
    fitfunc_server.start()
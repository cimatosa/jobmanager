import pickle

from .jobmanager import JobManager_Server
from .jobmanager import log


class PersistentData_Server(JobManager_Server):
    def __init__(
        self,
        persistent_data_structure,
        authkey,
        const_arg=None,
        port=42524,
        verbose=None,
        msg_interval=1,
        fname_dump=None,
        speed_calc_cycles=50,
        overwrite=False,
        return_args=True,
        **kwargs
    ):

        JobManager_Server.__init__(
            self,
            authkey,
            const_arg=const_arg,
            port=port,
            verbose=verbose,
            msg_interval=msg_interval,
            fname_dump=fname_dump,
            speed_calc_cycles=speed_calc_cycles,
            **kwargs
        )
        self.pds = persistent_data_structure
        self.overwrite = overwrite
        self.return_args = return_args
        if self.overwrite:
            log.info("overwriting existing data is ENABLED")
        else:
            log.info("overwriting existing data is DISABLED")

    def process_new_result(self, arg, result):
        """
        use arg.id as key to store the pair (arg, result) in the data base

        return True, if a new data set was added (key not already in pds)
        otherwise false
        """
        key = pickle.dumps(arg.id, protocol=2)
        has_key = key in self.pds
        if not self.return_args:
            arg = None
        self.pds[key] = (arg, result)
        return not has_key

    def put_arg(self, a):
        a_bin = pickle.dumps(a.id, protocol=2)
        if self.overwrite:
            log.debug("add arg (overwrite True)")
            JobManager_Server.put_arg(self, a)
            return True

        if not a_bin in self.pds:
            log.debug("add arg (arg not found in db)")
            JobManager_Server.put_arg(self, a)
            return True

        log.debug("skip arg (arg found in db)")
        return False

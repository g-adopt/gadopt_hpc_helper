from . import HPCExecutor


class MpiexecExecutor(HPCExecutor):
    """
    Extend the HPCExecutor class to provide a definition for the get() function
    that allows construction of more complex binding options
    """
    def get(self) -> str:
        if self.nprocs == 1:
            return ""
        elif self.cores_per_node == self.procs_per_node:
            return self.cmd
        else:
            return "Not yet"


Mpiexec = MpiexecExecutor(cmd="mpiexec")

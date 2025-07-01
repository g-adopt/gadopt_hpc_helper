from . import HPCExecutor
import math


class MpiexecExecutor(HPCExecutor):
    """
    Extend the HPCExecutor class to provide a definition for the get() function
    that allows construction of more complex binding options
    """

    def get(self) -> str:
        nodes = math.ceil(self.nprocs / self.cores_per_node)
        if self.nprocs == 1:
            return ""
        if nodes == 1:
            return self.cmd
        if self.nprocs % self.cores_per_node == 0:
            return self.cmd
        return f"{self.cmd} -np {self.nprocs} --map-by ppr:{math.ceil(self.nprocs / nodes / self.numa_per_node)}:numa --rank-by core"


Mpiexec = MpiexecExecutor(cmd="mpiexec")

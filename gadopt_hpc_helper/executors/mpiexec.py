from . import HPCExecutor


class MpiexecExecutor(HPCExecutor):
    def get(self) -> str:
        if self.nprocs == 1:
            return ""
        elif self.cores_per_node == self.procs_per_node:
            return self.cmd
        else:
            return "Not yet"


Mpiexec = MpiexecExecutor(cmd="mpiexec")

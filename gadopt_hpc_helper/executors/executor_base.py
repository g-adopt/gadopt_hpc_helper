from dataclasses import dataclass


@dataclass
class HPCExecutor:
    cmd: str

    def set_job_size_specific_flags(self, nprocs: int, ppn: int, cores_per_node: int, numa_per_node: int) -> None:
        """
        Set the cores/node parameters required for this particular job. Executor args can vary in complexity, for
        instance, 'srun', the slurm executor derives its process binding characteristics from the slurm
        job flags, meaning that the executor is always 'srun' with no additional binding options. In
        contrast, mpiexec can require fairly complex binding arguments in order to distribute and bind
        processes correctly across nodes.

        Args:
          nprocs:
            The total number of processes to be launched by this job
          ppn:
            The number of processes to launch per node.
          cores_per_node:
            The number of physical cores on each compute node
          numa_per_node:
            The number of NUMA zones per node
        """
        self.nprocs = nprocs
        self.procs_per_node = ppn
        self.cores_per_node = cores_per_node
        self.numa_per_node = numa_per_node

    def get(self) -> str:
        """
        Return the executor command for this job
        """
        if self.nprocs == 1:
            return ""
        else:
            return self.cmd

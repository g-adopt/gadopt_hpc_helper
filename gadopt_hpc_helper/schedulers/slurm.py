"""
Define the properties of the Slurm scheduler
"""

from . import HPCScheduler, h_m_s_formatter
from ..executors import Srun


class SlurmScheduler(HPCScheduler):
    """
    Extend the HPCScheduler class to provide a definition for job_size_specific_flags
    """

    def job_size_specific_flags(self) -> list[str]:
        """
        Add the --exclusive flag if this job is going to use more than half of a node
        """
        if self.nprocs >= self.procs_per_node / 2:
            return ["--exclusive"]
        return []


Slurm = SlurmScheduler(
    subcmd="sbatch",
    directive_prefix="#SBATCH",
    executor=Srun,
    time_formatter=h_m_s_formatter,
    spec={
        "var": "--export {comma_sep_vars}",
        "block": "--wait",
        "name": "-J {jobname}",
        "time": "-t {walltime}",
        "procs": ["--ntasks={cores}", "--nodes={nodes}"],
        "stderr": "-e {errname}",
        "stdout": "-o {outname}",
        "acct": "-A {project}",
        "queue": "-p {queue}",
    },
)

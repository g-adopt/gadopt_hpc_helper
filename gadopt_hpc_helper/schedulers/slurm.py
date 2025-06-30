"""
Define the properties of the Slurm scheduler
"""

from . import HPCScheduler, h_m_s_formatter
from ..executors import Srun


class SlurmScheduler(HPCScheduler):
    """
    Extend the HPCScheduler class to provide a definition for job_size_specific_flags
    """

    def job_size_specific_flags(self, procs: int = 1, procs_per_node: int = 1, *args) -> str:
        """
        Add the --exclusive flag if this job is going to use more than half of a node
        """
        if procs >= procs_per_node / 2:
            return "--exclusive"
        return ""


Slurm = SlurmScheduler(
    subcmd="sbatch",
    var_spec="--export {comma_sep_vars}",
    block_spec="--wait",
    name_spec="-J {jobname}",
    time_spec="-t {walltime}",
    time_formatter=h_m_s_formatter,
    procs_spec="--ntasks={cores} --nodes={nodes}",
    mem_spec="",
    stderr_spec="-e {errname}",
    stdout_spec="-o {outname}",
    acct_spec="-A {project}",
    queue_spec="-p {queue}",
    local_storage_spec="",
    extras="",
    directive_prefix="#SBATCH",
    executor=Srun,
)

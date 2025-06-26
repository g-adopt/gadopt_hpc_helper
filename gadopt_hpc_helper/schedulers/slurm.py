from . import HPCScheduler, h_m_s_formatter
from ..executors import Srun


class SlurmScheduler(HPCScheduler):
    def job_size_specific_flags(self, procs: int = 1, procs_per_node: int = 1, *args) -> str:
        if procs >= procs_per_node / 2:
            return "--exclusive"
        else:
            return ""


Slurm = SlurmScheduler(
    subcmd="sbatch",
    var_spec="--export {comma_sep_vars}",
    block_spec="--wait",
    name_spec="-N {jobname}",
    time_spec="-t {walltime}",
    time_formatter=h_m_s_formatter,
    procs_spec="--ntasks={cores} --nnodes={nodes}",
    mem_spec="",
    stderr_spec="-e {errname}",
    stdout_spec="-o {outname}",
    acct_spec="-A {project}",
    queue_spec="-p {queue}",
    local_storage_spec="",
    extras="",
    executor=Srun,
)

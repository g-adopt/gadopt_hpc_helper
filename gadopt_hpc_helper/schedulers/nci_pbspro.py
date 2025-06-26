from . import HPCScheduler, h_m_s_formatter
from ..executors import Mpiexec

Nci_pbspro = HPCScheduler(
    subcmd="qsub",
    var_spec="-v {comma_sep_vars}",
    block_spec="-Wblock=true",
    name_spec="-N {jobname}",
    time_spec="-lwalltime={walltime}",
    time_formatter=h_m_s_formatter,
    procs_spec="-lncpus={cores}",
    mem_spec="-lmem={mem}GB",
    stderr_spec="-e {errname}",
    stdout_spec="-o {outname}",
    acct_spec="-P {project}",
    queue_spec="-q {queue}",
    local_storage_spec="-ljobfs={local_storage}GB",
    extras="-lstorage=gdata/{project}+scratch/{project}+gdata/fp50 -lwd",
    executor=Mpiexec,
)

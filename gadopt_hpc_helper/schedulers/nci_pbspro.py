"""
Define the properties of PBSPro with NCI's site-specific modifications
"""

from . import HPCScheduler, h_m_s_formatter
from ..executors import Mpiexec

Nci_pbspro = HPCScheduler(
    subcmd="qsub",
    directive_prefix="#PBS",
    executor=Mpiexec,
    time_formatter=h_m_s_formatter,
    spec={
        "var": "-v {comma_sep_vars}",
        "block": "-Wblock=true",
        "name": "-N {jobname}",
        "time": "-lwalltime={walltime}",
        "procs": "-lncpus={rounded_up_cores}",
        "mem": "-lmem={mem}GB",
        "stderr": "-e {errname}",
        "stdout": "-o {outname}",
        "acct": "-P {project}",
        "queue": "-q {queue}",
        "local_storage": "-ljobfs={local_storage}GB",
        "extras": ["-lstorage=gdata/{project}+scratch/{project}+gdata/fp50", "-lwd"],
    },
)

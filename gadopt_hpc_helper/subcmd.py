"""Job submit command constructor

This module contains a single function that constructs the batch
submission command for a G-ADOPT job. We prefer to construct
resource requests on the command line rather than in a script
as it significantly simplifies the script templates required.
"""

from shlex import split
import os
import math

from .systems import HPCSystem
from .config import HPCHelperConfig, PreserveFormatDict


def build_sub_cmd(system: HPCSystem, cfg: HPCHelperConfig) -> list[str]:
    """Construct the batch job submission command

    The command itself is constructed as a string, then formatted using the a
    PreserveFormatDict such that any brace-enclosed variables not defined in
    the dict remain as-is. This string is then split with the `shlex.split()`
    function in the hope of preserving any escaped input arguments.

    Args:
      system:
        The HPCSystem object derived on import of the module
      cfg:
        The `HPCHelperConfig` object for this job

    Returns
      list[str]: The formatted command tokenised by `shlex.split()`
    """
    cmd_str = system.scheduler.subcmd + " "
    cmd_str += system.scheduler.var_spec + " " if cfg.env else " "
    cmd_str += system.scheduler.block_spec + " "
    cmd_str += system.scheduler.name_spec + " " if cfg.jobname else " "
    cmd_str += system.scheduler.procs_spec + " "
    cmd_str += system.scheduler.time_spec + " "
    cmd_str += system.scheduler.mem_spec + " "
    cmd_str += system.scheduler.local_storage_spec + " "
    cmd_str += (
        system.scheduler.job_size_specific_flags(
            cfg.nprocs, cfg.ppn, system.queues[cfg.queue].cores_per_node, system.queues[cfg.queue].numa_per_node
        )
        + " "
    )
    cmd_str += system.scheduler.extras + " "
    cmd_str += system.scheduler.queue_spec + " "
    cmd_str += system.scheduler.acct_spec
    cmd_str += " " + system.scheduler.stdout_spec if cfg.outfile else ""
    cmd_str += " " + system.scheduler.stderr_spec if cfg.errfile else ""

    return split(
        cmd_str.format_map(
            PreserveFormatDict(
                jobname=cfg.jobname,
                comma_sep_vars=cfg.env,
                cores=cfg.nprocs,
                ppn=cfg.ppn,
                nodes=math.ceil(cfg.nprocs / cfg.ppn),
                walltime=system.scheduler.time_formatter(cfg.walltime),
                mem=cfg.mem,
                local_storage=system.queues[cfg.queue].local_disk_per_node,
                queue=cfg.queue,
                project=os.environ[system.project_var],
                outname=cfg.outfile,
                errname=cfg.errfile,
            )
        )
    )

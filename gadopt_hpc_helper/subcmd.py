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


def build_sub_cmd(system: HPCSystem, cfg: HPCHelperConfig, opts_style: str) -> list[str]:
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
      opts_style:
        Whether the majority of batch options go in the command
        line or in the job script

    Returns
      list[str]: The formatted batch submission command`
    """
    cmd = [system.scheduler.subcmd]
    if cfg.env:
        cmd.extend(split(system.scheduler.var_spec))
    cmd.append(system.scheduler.block_spec)
    if opts_style == "cmdline":
        if cfg.jobname:
            cmd.extend(split(system.scheduler.name_spec))
        cmd.extend(
            [
                *split(system.scheduler.procs_spec),
                *split(system.scheduler.time_spec),
                *split(system.scheduler.mem_spec),
                *split(system.scheduler.local_storage_spec),
                *split(
                    system.scheduler.job_size_specific_flags(
                        cfg.nprocs,
                        cfg.ppn,
                        system.queues[cfg.queue].cores_per_node,
                        system.queues[cfg.queue].numa_per_node,
                    )
                ),
                *split(system.scheduler.extras),
                *split(system.scheduler.queue_spec),
                *split(system.scheduler.acct_spec),
            ]
        )
        if cfg.outfile:
            cmd.extend(split(system.scheduler.stdout_spec))
        if cfg.errfile:
            cmd.extend(split(system.scheduler.stderr_spec))

    return [
        c.format_map(
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
        for c in cmd
    ]

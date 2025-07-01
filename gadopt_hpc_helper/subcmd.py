"""Job submit command constructor

This module contains a single function that constructs the batch
submission command for a G-ADOPT job. We prefer to construct
resource requests on the command line rather than in a script
as it significantly simplifies the script templates required.
"""

from shlex import split
from typing import Callable

from .systems import HPCSystem
from .schedulers import format_batch_arg_spec
from .config import HPCHelperConfig


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
      list[str]: The formatted batch submission command
    """
    attribs_order: list[str | Callable] = ["var", "block"]
    if opts_style == "cmdline":
        attribs_order.extend(
            [
                "name",
                "procs",
                "time",
                "mem",
                "local_storage",
                system.scheduler.job_size_specific_flags,
                "extras",
                "queue",
                "acct",
                "stdout",
                "stderr",
            ]
        )

    cmd = [system.scheduler.subcmd]
    for attrib in attribs_order:
        if isinstance(attrib, str):
            if attrib in system.scheduler.spec:
                var_test = getattr(cfg, f"do_{attrib}") if hasattr(cfg, f"do_{attrib}") else True
                for f in format_batch_arg_spec(var_test, system.scheduler.spec[attrib], cfg.global_format_dict):
                    cmd.extend(split(f))
        else:
            for f in attrib():
                cmd.extend(split(f))

    return cmd

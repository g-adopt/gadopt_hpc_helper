from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Any

from ..executors import HPCExecutor


def h_m_s_formatter(t: timedelta) -> str:
    """
    Format a timedelta object to a string in HH:MM:SS format
    """
    return f"{(t.seconds // 3600 + t.days * 24):02}:{((t.seconds // 60) % 60):02}:{(t.seconds % 60):02}"


@dataclass
class HPCScheduler:
    """
    A class encapsulating the properties of an HPC scheduler. All fields
    are required and intended to be provided by a static object initialised
    in its own file. These objects should be re-exported through __init__.py
    such that they can be used and/or extended by `HPCSystem` objects.
    """

    subcmd: str
    directive_prefix: str
    executor: HPCExecutor
    time_formatter: Callable[[timedelta], str]
    spec: dict[str, str | list[str]]

    def job_size_specific_flags(self) -> list[str]:
        return []

    def set_job_size_specific_flags(self, nprocs: int, ppn: int, cores_per_node: int, numa_per_node: int) -> None:
        """
        Set the cores/node parameters required for this particular job. Batch submission args can
        vary in complexity. Slurm, for instance, derives all of its layout options from
        the batch submission command, so the full command cannot be known until the size of
        the job has been determined

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


def format_batch_arg_spec(do: bool, spec: str | list[str], format_dict: dict[str, Any], prefix="") -> list[str]:
    """Batch argument formatter

    Takes a spec string, a formatting dictionary and a flag to indicate whether
    the anything should be returned. Returns a list of formatted strings, or an
    empty list if 'do' is False

    Args:
      do:
        A flag to determine if there should be any output
      spec:
        A string or list of strings with brace-enclosed {format} strings
      format_dict:
        A dict (ideally a PreserveFormatDict) that contains k-v pairs for
        any {format} strings that may be encountered.

    Returns:
      list[str]: Formatted strings
    """
    if do:
        if isinstance(spec, str):
            return [f"{prefix} {spec.format_map(format_dict)}"]
        else:
            return [f"{prefix} {s.format_map(format_dict)}" for s in spec]
    else:
        return []

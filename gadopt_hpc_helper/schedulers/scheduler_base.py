from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

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
    var_spec: str
    block_spec: str
    name_spec: str
    time_spec: str
    time_formatter: Callable[[timedelta], str]
    procs_spec: str
    mem_spec: str
    stdout_spec: str
    stderr_spec: str
    acct_spec: str
    queue_spec: str
    local_storage_spec: str
    extras: str
    executor: HPCExecutor

    def job_size_specific_flags(self, *args) -> str:
        return ""

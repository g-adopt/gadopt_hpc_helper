from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

from ..executors import HPCExecutor


def h_m_s_formatter(t: timedelta) -> str:
    return f"{(t.seconds // 3600 + t.days * 24):02}:{((t.seconds // 60) % 60):02}:{(t.seconds % 60):02}"


@dataclass
class HPCScheduler:
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

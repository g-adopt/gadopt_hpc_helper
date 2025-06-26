from dataclasses import dataclass
from typing import Callable
from ..schedulers import HPCScheduler

default_job_template = """{header}

{prescript}

{executor} {command}
"""


@dataclass
class HPCQueue:
    cores_per_node: int
    numa_per_node: int
    local_disk_per_node: int
    default_processes_per_node: int
    memory_per_node: float
    max_job_cores: int


@dataclass
class HPCSystem:
    name: str
    is_this_system: Callable[[], bool]
    queues: dict[str, HPCQueue]
    default_queue: str
    job_template: str
    header: str
    prescript: str
    scheduler: HPCScheduler
    project_var: str
    required_env: set[str]

from dataclasses import dataclass
from typing import Callable
from ..schedulers import HPCScheduler

default_job_template = """{header}

{prescript}

{executor} {command}
"""


@dataclass
class HPCQueue:
    """
    A class encapsulating the properties of a queue on an HPC system
    """
    cores_per_node: int
    numa_per_node: int
    local_disk_per_node: int
    default_processes_per_node: int
    memory_per_node: float
    max_job_cores: int


@dataclass
class HPCSystem:
    """
    A class encapsulating the properties of an HPC system. All fields
    are required and are intended to be provided by a static object
    initialised in its own file. The initialised object should then
    be imported into __init__.py such that the its is_this_system()
    method can be run on import of the gadopt_hpc_helper module.
    """
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

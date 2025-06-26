import socket
from .system_base import HPCQueue, HPCSystem, default_job_template
from ..schedulers import Slurm

_setonix_queues = {
    "work": HPCQueue(128, 8, -1, 70, 230.0, 176128),
    "long": HPCQueue(128, 8, -1, 70, 230.0, 1024),
    "highmem": HPCQueue(128, 8, -1, 70, 980.0, 1024),
}

_setonix_gadopt_setup = """
module use /software/projects/pawsey0821/modules
module load firedrake
"""

_setonix_job_header = """#!/bin/bash -i
#SBATCH --exclude=nid00[2024-2055],nid00[2792-2823]
"""

Setonix = HPCSystem(
    name="Setonix",
    is_this_system=lambda: socket.getfqdn().endswith("setonix.pawsey.org.au"),
    queues=_setonix_queues,
    default_queue="work",
    job_template=default_job_template,
    header=_setonix_job_header,
    prescript=_setonix_gadopt_setup,
    scheduler=Slurm,
    project_var="PAWSEY_PROJECT",
    required_env={"MY_GADOPT"},
)

import socket
from .system_base import HPCSystem, HPCQueue, default_job_template
from ..schedulers import Nci_pbspro

_gadi_queues = {
    "normal": HPCQueue(48, 4, 400, 48, 190.0, 20736),
    "express": HPCQueue(48, 4, 400, 48, 190.0, 3168),
    "hugemem": HPCQueue(48, 4, 400, 48, 1470.0, 192),
    "megamem": HPCQueue(48, 4, 400, 48, 2990.0, 96),
    "copyq": HPCQueue(48, 4, 400, 48, 190.0, 1),
    "normalsr": HPCQueue(104, 8, 400, 104, 500.0, 10400),
    "expresssr": HPCQueue(104, 8, 400, 104, 500.0, 2080),
}

_gadi_job_header = "#!/usr/bin/env bash"

_gadi_gadopt_setup = """
module use /g/data/fp50/modules
module load firedrake
"""

Gadi = HPCSystem(
    name="Gadi",
    is_this_system=lambda: socket.gethostname().startswith("gadi"),
    queues=_gadi_queues,
    default_queue="normalsr",
    job_template=default_job_template,
    header=_gadi_job_header,
    prescript=_gadi_gadopt_setup,
    scheduler=Nci_pbspro,
    required_env={"MY_GADOPT", "PROJECT"},
)

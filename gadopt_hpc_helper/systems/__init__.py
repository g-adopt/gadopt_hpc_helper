"""
Defines the `get_system` function, which is run when gadopt_hpc_helper
is imported
"""

import os

from .system_base import HPCSystem
from .gadi import Gadi
from .setonix import Setonix

__all__ = ["get_system", "HPCSystem"]


def get_system() -> HPCSystem:
    """Determine the current system G-ADOPT is being run on.

    Runs the ``is_this_system()`` function belonging to any ``HPCSystem``
    objects known to this module or by reading the value of the
    environment variable ``GADOPT_HPC_HELPER_SYSTEM_OVERRIDE``

    Returns:
      HPCSystem: Object corresponding to the detected system
    Raises:
      ImportError: No ``is_this_system()`` calls returned true
        or the system provided by the override variable is not
        known.
    """
    if "GADOPT_HPC_HELPER_SYSTEM_OVERRIDE" in os.environ:
        sn = os.environ["GADOPT_HPC_HELPER_SYSTEM_OVERRIDE"]
        try:
            sys = globals()[sn]
        except KeyError:
            raise ImportError(
                f"System override from environment {sn} does not match any HPC system known to this software"
            )
        if not isinstance(sys, HPCSystem):
            raise ImportError(
                f"System override from environment {sn} does not match any HPC system known to this software"
            )
        return sys

    for var in globals():
        sys = globals()[var]
        if isinstance(sys, HPCSystem):
            if sys.is_this_system():
                return sys
    raise ImportError("Cannot import gadopt_hpc_helper. Unable to determine identity of current system")

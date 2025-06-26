"""Batch script constructor.

This module contains a context manager that creates a script to submit
to the batch system on the detected HPC system.
"""

import tempfile
from shlex import join
from .config import HPCHelperConfig, PreserveFormatDict


class Gadopt_HPC_Script:
    """Context manager for HPC batch scripts

    This class provides a context manager that constructs and then destroys
    temporary scripts to be submitted to batch systems. The scripts are
    expected to use curly-brace enclosed variables for formatting. Four variables
    are used in constructing these scripts:

    `{header}`:
      Usually the interpreter and any additional system-specific job flags that
      are constant on all jobs

    `{prescript}`:
      Usually loading modules and any other pre-G-ADOPT setup

    `{executor}`:
      The method of invoking multi-core jobs on this system (e.g. srun, mpiexec).
      Determined by the `HPCExecutor` object associated with this system

    `{command}`:
      The command to run

    Any other brace-enclosed variables are left as-is, as we assume that these will
    be expanded from environment variables at runtime. The template is provided by
    user input, or by the system default.
    """

    def __init__(self, cfg: HPCHelperConfig, executor: str, cmd: list[str]):
        """Construct a temporary file and write the formatted template to it

        Args:
          cfg:
            The `HPCHelperConfig` object for this job
          executor:
            The result of `executor.get()`, a string that will be written to the
            script in order to launch the job.
          cmd:
            The command to run within the script. Generally constructed from the
            'left over' command-line arguments, hence a list of strings
        """
        self.script_file = tempfile.NamedTemporaryFile()
        with open(self.script_file.name, "w") as f:
            f.write(
                cfg.template.format_map(
                    PreserveFormatDict(header=cfg.header, prescript=cfg.prescript, executor=executor, command=join(cmd))
                )
            )

    def __enter__(self) -> str:
        return self.script_file.name

    def __exit__(self, type, value, traceback):
        """Closes(and therefore deletes) the script file"""
        self.script_file.close()

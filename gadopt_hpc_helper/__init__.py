"""Main program for gadopt_hpc_helper

Run a G-ADOPT job on an HPC system. Attempts to detect the system on which
we're attempting to run, and construct a batch script and job submission
command for that system. Provides the command-line entry point as the
function main(), a python entrypoint gadopt_hpcrun() which is a synchronous
wrapper to the async entrypoint gadopt_hpcrun_async().

Typical usage example:

  Command line:
    $ gadopt_hpcrun -n 4 python3 gadopt_job.py

  Synchronous:
    rc = gadopt_hpc_helper.gadopt_hpcrun(4, header="#PBS -lstorage=gdata/ab12", ["python3","gadopt_job.py"])

  Asynchronous:
    for paramstr in params:
      procs.append(gadopt_hpc_helper.gadopt_hpcrun_async(4, ["python3","gadopt_job.py",paramstr]))
    rcs = asyncio.gather(procs)
"""

import sys
import asyncio
import argparse
import datetime
import dateutil
import dateutil.parser
from typing import Any

from .logging import logger

from .systems import get_system
from .config import HPCHelperConfig, default_walltime
from .subcmd import build_sub_cmd
from .script import Gadopt_HPC_Script

system = get_system()
logger.info("Detected system: %s", system.name)


class NothingToDoError(Exception):
    """Raised when no command is provided."""


def gadopt_hpcrun(*args, **kwargs) -> int:
    """
    Runs a blocking version of gadopt_hpcrun_async. See the documentation
    of gadopt_hpcrun_async for more information
    """
    loop = asyncio.get_event_loop()
    rc = loop.run_until_complete(gadopt_hpcrun_async(*args, **kwargs))
    if rc is not None:
        return rc
    raise Exception("Asyncio loop has completed but return status is None")


async def gadopt_hpcrun_async(
    nprocs: int,
    extra_env: str | dict[str, Any] = "",
    queue: str = "",
    ppn: int = -1,
    walltime: datetime.timedelta | None = None,
    outfile: str = "",
    errfile: str = "",
    jobname: str = "",
    template: str = "",
    mem: int = -1,
    prescript: str = "",
    header: str = "",
    cmd: list[str] = [],
) -> int | None:
    """The main entrypoint for running G-ADOPT using a driver script.

    This function takes more configuration options than the command line
    interface, and is intended for more complex changes from the default
    settings. The script template, script header and pre-job script can
    all be modified by this function, but not directly from the command
    line interface. Submits a blocking job to the system's batch queue
    and returns control to the event loop until the job finishes. Returns
    the jobs exit status on completion of the coroutine.

    Args:
      nprocs:
        The total number of processes to be launched by this job
      extra_env (optional):
        Any extra environment required for this job, accepts either
        a single string of comma separated key=value pairs or a dict
      queue (optional):
        The queue on which to submit this job. If not specified, the
        system default queue will be used
      ppn (optional):
        The number of processes to launch per node. If not specified
        the default for the specified queue will be used.
      walltime (optional):
        Walltime as a datetime.timedelta object. If not specified, the
        default (4 hours) will be used
      outfile (optional):
        The name of the stdout file for the job. If not specified, the
        scheduler default will be used
      errfile (optional):
        The name of the stderr file for the job. If not specified, the
        scheduler default will be used
      jobname (optional):
        The name of the job. If not specified, the scheduler default
        will be used
      mem (optional):
        The amount of memory to request with the job. If not specified
        memory will be requested in proportion to the available memory
        on the nodes of the specified queue
      prescript (optional):
        The script to run before the main G-ADOPT job is run. If not
        specified, the system default will be used
      template (optional):
        The job template. If not provided, the default for the system
        will be used
      header (optional):
        The job header. If not provided, the default for the system
        will be used
      cmd (optional):
        A list containing the command to run. Note that this argument
        isn't really optional as this function will raise an exception
        if cmd is empty

    Returns:
      int: Return status of the batch job

    Raises:
      NothingToDoError: Not given anything command to run

    """
    if not cmd:
        raise NothingToDoError("No command given to run")
    logger.info("Will run the following command:\n%s", " ".join(cmd))

    cfg = HPCHelperConfig(
        system,
        nprocs,
        extra_env,
        ppn,
        walltime,
        mem,
        prescript,
        outfile,
        errfile,
        jobname,
        queue,
        template,
        header,
    )
    # First construct the qsub command
    subcmd = build_sub_cmd(system, cfg)
    logger.info("Will use the following batch submission command:\n%s", " ".join(subcmd))
    # Set parameters on the executor
    system.scheduler.executor.set_job_size_specific_flags(cfg.nprocs, cfg.ppn, cfg.cores_per_node, cfg.numa_per_node)
    executor = system.scheduler.executor.get()

    with Gadopt_HPC_Script(cfg, executor, cmd) as script_file:
        logger.info("Submitting the following script:\n%s", open(script_file).read())
        subcmd.append(script_file)
        logger.debug(subcmd)
        proc = await asyncio.create_subprocess_exec(*subcmd)
        await proc.wait()
        return proc.returncode


def file_to_str(path: str) -> str:
    """Read a file into a string

    If the path is empty, just return an empty string

    Args:
      path: Path to file

    Returns:
      string: Contents of file
    """
    if path == "":
        return ""
    with open(path, "r") as f:
        return f.read()


def str_to_interval(string: str) -> datetime.timedelta:
    """Convert a string into a ``datetime.timedelta`` object.

    Attempts a few different conversions before giving up.
    First tries a straight float conversion and interprets this as
    a number of seconds. Next tries to split the string on colons,
    remove any decimal points and interpret each field as an integer
    in [[DD:]HH:]MM:SS format. If that fails, try passing to
    dateutil.parser and subtracting todays date.

    Args:
      string: String to attempt to interpret as a timedelta

    Returns:
      datetime.timedelta: The time interval deduced from ``string``

    Raises:
      ValueError:
    """
    # integer/float number of seconds
    try:
        return datetime.timedelta(seconds=float(string))
    except ValueError:
        pass

    # DD:MM:HH:SS format
    try:
        return datetime.timedelta(
            **dict(
                zip(
                    ["seconds", "minutes", "hours", "days"], reversed([int(i.split(".")[0]) for i in string.split(":")])
                )
            )
        )
    except ValueError:
        pass

    # Last resort
    try:
        return dateutil.parser.parse(string) - datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    except dateutil.parser.ParserError:
        # raise a more useful exception
        raise ValueError(f"Do not know how to convert {string} to a time interval")


def main():
    """Command-line entrypoint for gadopt_hpc_helper"""
    parser = argparse.ArgumentParser(prog="gadopt_hpc_helper")
    parser.add_argument("-n", "--np", required=True, type=int, help="Number of CPU cores for job")
    parser.add_argument(
        "-v", "--env", required=False, type=str, default="", help="Additional environment to pass to job"
    )
    parser.add_argument("--ppn", required=False, type=int, default=-1, help="Number of processes per node")
    parser.add_argument("-o", "--outfile", required=False, type=str, default="", help="Path to job stdout")
    parser.add_argument("-e", "--errfile", required=False, type=str, default="", help="Path to job stderr")
    parser.add_argument("-N", "--jobname", required=False, type=str, default="", help="Name of batch job")
    parser.add_argument("-q", "--queue", required=False, type=str, default="", help="HPC queue to submit job to")
    parser.add_argument(
        "-t",
        "--walltime",
        required=False,
        type=str_to_interval,
        default=default_walltime,
        help="Job walltime specification in seconds or [[DD:]HH:]MM:SS format",
    )
    parser.add_argument(
        "--template-file",
        required=False,
        type=file_to_str,
        default="",
        help="Path to a file that contains a template to use for this job",
    )
    # Undocumented and buggy as per https://bugs.python.org/issue43343#msg387812
    # but the only way to get GNU C-like argument parsing without manually looping
    # through args
    parser.add_argument("rest", nargs=argparse.REMAINDER)
    ns = parser.parse_args(sys.argv[1:])

    sys.exit(
        gadopt_hpcrun(
            ns.np,
            ns.env,
            ns.queue,
            ns.ppn,
            ns.walltime,
            ns.outfile,
            ns.errfile,
            ns.jobname,
            ns.template_file,
            cmd=ns.rest,
        )
    )


if __name__ == "__main__":
    main()

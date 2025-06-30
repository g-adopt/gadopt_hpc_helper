"""Configuration module for gadopt_hpc_helper

This module contains the main configuration driver for gadopt_hps_helper.
It provides the HPCHelperConfig class, which determines the configuration
settings for the job being run. It also provides the 'PreserveFormatDict'
class, which is used when formatting job templates.
"""

import os
from datetime import timedelta
import math
from typing import Any

from .systems import HPCSystem

default_walltime = timedelta(hours=4)


class PreserveFormatDict(dict):
    """Simple extension of a dict

    This class overrides the __missing__ function of the
    dict class such that '{key}' is returned when `key`
    is not present. This is used when formatting job scripts
    as shell variables (e.g. ${var}) may not be defined in our
    format dicts and must be left untouched to work at runtime
    """

    def __missing__(self, key) -> str:
        return f"{{{key}}}"


class HPCHelperConfig:
    """Job configuration class

    This class is responsible for taking arguments passed to the main
    gadopt_hpcrun_async function and combining those with the default
    settings for a given system. Every attribute in the config object
    is set by the `__init__` method.
    """

    def __init__(
        self,
        system: HPCSystem,
        nprocs: int,
        extra_env: str | dict[str, Any],
        ppn: int,
        walltime: timedelta | None,
        mem: int,
        prescript: str,
        outfile: str,
        errfile: str,
        jobname: str,
        queue: str,
        template: str,
        header: str,
        print_script: bool,
        save_script: str,
    ):
        """Intialises the config object

        This function combines system defaults with user overrides in order to
        provide all required information to construct submit commands and job
        scripts

        Args:
          system:
            The HPCSystem object derived on import of the module
          nprocs:
            The total number of processes to be launched by this job
          extra_env:
            Any extra environment required for this job, accepts either
            a single string of comma separated key=value pairs or a dict
          ppn:
            The number of processes to launch per node. If not specified
            the default for the specified queue will be used.
          walltime:
            Walltime as a datetime.timedelta object. If not specified, the
            default (4 hours) will be used
          mem:
            The amount of memory to request with the job. If not specified
            memory will be requested in proportion to the available memory
            on the nodes of the specified queue
          prescript:
            The script to run before the main G-ADOPT job is run. If not
            specified, the system default will be used
          outfile:
            The name of the stdout file for the job. If not specified, the
            scheduler default will be used
          errfile:
            The name of the stderr file for the job. If not specified, the
            scheduler default will be used
          jobname:
            The name of the job. If not specified, the scheduler default
            will be used
          queue:
            The queue on which to submit this job. If not specified, the
            system default queue will be used
          template:
            The job template. If not provided, the default for the system
            will be used
          header:
            The job header. If not provided, the default for the system
            will be used
          print_script:
            Print the final script to stdout instead of running
          save_script:
            Path to save script file to
        """
        self.nprocs = nprocs
        required_env = system.required_env | set((system.project_var,))
        try:
            self.env = ",".join([f"{k}={os.environ[k]}" for k in required_env])
        except KeyError as e:
            nl = "\n"
            print(
                f"Error: {e.args[0]} is not present in the environment. The following environment variables must be set when running on {system.name}:{nl}{nl.join(k for k in required_env)}"
            )
            exit(1)

        if extra_env:
            if isinstance(extra_env, dict):
                self.env = ",".join([self.env] + [f"{k}={v}" for k, v in extra_env.items()])
            else:
                self.env = ",".join([self.env, extra_env])

        if prescript:
            self.prescript = prescript
        else:
            self.prescript = system.prescript

        if queue:
            if queue not in system.queues:
                nl = "\n"
                print(
                    f"{queue} is not a valid queue for system {system.name}.{nl}Valid queues are:{nl}{nl.join(q for q in system.queues)}"
                )
                exit(1)
            self.queue = queue
        else:
            self.queue = system.default_queue

        if walltime:
            self.walltime = walltime
        else:
            self.walltime = default_walltime

        if template:
            self.template = template
        else:
            self.template = system.job_template

        if header:
            self.header = header
        else:
            self.header = system.header

        if ppn == -1:
            self.ppn = system.queues[self.queue].default_processes_per_node
        else:
            self.ppn = ppn

        if mem == -1:
            nnodes = math.ceil(self.nprocs / self.ppn)
            if nnodes == 1:
                self.mem = math.floor(
                    self.nprocs / system.queues[self.queue].cores_per_node * system.queues[self.queue].memory_per_node
                )
            else:
                self.mem = math.floor(nnodes * system.queues[self.queue].memory_per_node)
        else:
            self.mem = mem

        self.extras = ""
        self.print_script = print_script

        self.save_script = save_script
        if self.save_script:
            self.save = True

        self.cores_per_node = system.queues[self.queue].cores_per_node
        self.numa_per_node = system.queues[self.queue].numa_per_node

        self.outfile = outfile
        self.errfile = errfile
        self.jobname = jobname

    def set_directives(self, system: HPCSystem, opts_style: str):
        directives = []
        if opts_style == "directive":
            dir_prefix = system.scheduler.directive_prefix

            if self.jobname:
                directives.append(f"{dir_prefix} {system.scheduler.name_spec}")
            if system.scheduler.procs_spec:
                directives.append(f"{dir_prefix} {system.scheduler.procs_spec}")
            if system.scheduler.time_spec:
                directives.append(f"{dir_prefix} {system.scheduler.time_spec}")
            if system.scheduler.mem_spec:
                directives.append(f"{dir_prefix} {system.scheduler.mem_spec}")
            if system.scheduler.local_storage_spec:
                directives.append(f"{dir_prefix} {system.scheduler.local_storage_spec}")
            if system.scheduler.acct_spec:
                directives.append(f"{dir_prefix} {system.scheduler.acct_spec}")
            if system.scheduler.queue_spec:
                directives.append(f"{dir_prefix} {system.scheduler.queue_spec}")
            size_flags = system.scheduler.job_size_specific_flags(
                self.nprocs, self.ppn, system.queues[self.queue].cores_per_node, system.queues[self.queue].numa_per_node
            )
            if size_flags:
                directives.append(f"{dir_prefix} {size_flags}")
            if system.scheduler.extras:
                directives.append(f"{dir_prefix} {system.scheduler.extras}")
            if self.outfile:
                directives.append(f"{dir_prefix} {system.scheduler.stdout_spec}")
            if self.errfile:
                directives.append(f"{dir_prefix} {system.scheduler.stderr_spec}")
        self.directives = "\n".join(
            [
                d.format_map(
                    PreserveFormatDict(
                        jobname=self.jobname,
                        cores=self.nprocs,
                        ppn=self.ppn,
                        nodes=math.ceil(self.nprocs / self.ppn),
                        walltime=system.scheduler.time_formatter(self.walltime),
                        mem=self.mem,
                        local_storage=system.queues[self.queue].local_disk_per_node,
                        queue=self.queue,
                        project=os.environ[system.project_var],
                        outname=self.outfile,
                        errname=self.errfile,
                    )
                )
                for d in directives
            ]
        )

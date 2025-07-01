"""Configuration module for gadopt_hpc_helper

This module contains the main configuration driver for gadopt_hps_helper.
It provides the HPCHelperConfig class, which determines the configuration
settings for the job being run. It also provides the 'PreserveFormatDict'
class, which is used when formatting job templates.
"""

import os
from datetime import timedelta
import math
from typing import Any, Callable

from .systems import HPCSystem
from .schedulers import format_batch_arg_spec
from .logging import logger

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
        logger.debug("nprocs: %s", nprocs)
        required_env = system.required_env | set((system.project_var,))
        try:
            self.env = ",".join([f"{k}={os.environ[k]}" for k in required_env])
        except KeyError as e:
            logger.error(
                "Error: %s is not present in the environment. The following environment variables must be set when running on %s:\n%s",
                e.args[0],
                system.name,
                "\n".join(k for k in required_env),
            )
            exit(1)

        if extra_env:
            if isinstance(extra_env, dict):
                self.env = ",".join([self.env] + [f"{k}={v}" for k, v in extra_env.items()])
            else:
                self.env = ",".join([self.env, extra_env])

        if prescript:
            self.prescript = prescript
            logger.debug("User-provided prescript %s", self.prescript)
        else:
            self.prescript = system.prescript
            logger.debug("Default prescript %s", self.prescript)

        if queue:
            if queue not in system.queues:
                logger.error(
                    "%s is not a valid queue for system %s.\nValid queues are:\n%s",
                    queue,
                    system.name,
                    "\n".join(q for q in system.queues),
                )
                exit(1)
            self.queue = queue
            logger.debug("User-provided queue %s", self.queue)
        else:
            self.queue = system.default_queue
            logger.debug("Default queue %s", self.queue)

        if walltime:
            self.walltime = walltime
            logger.debug("User-provided walltime %s", self.walltime)
        else:
            self.walltime = default_walltime
            logger.debug("Default walltime %s", self.walltime)

        if template:
            self.template = template
            logger.debug("User-provided template %s", self.template)
        else:
            self.template = system.job_template
            logger.debug("Default template\n%s", self.template)

        if header:
            self.header = header
            logger.debug("User-provided header %s", self.header)
        else:
            self.header = system.header
            logger.debug("Default header %s", self.header)

        if ppn == -1:
            self.ppn = system.queues[self.queue].default_processes_per_node
            logger.debug("Default procs per node %s", self.ppn)
        else:
            self.ppn = ppn
            logger.debug("User-provided procs per node %s", self.ppn)

        if mem == -1:
            nnodes = math.ceil(self.nprocs / self.ppn)
            if nnodes == 1:
                self.mem = math.floor(
                    self.nprocs / system.queues[self.queue].cores_per_node * system.queues[self.queue].memory_per_node
                )
            else:
                self.mem = math.floor(nnodes * system.queues[self.queue].memory_per_node)
            logger.debug("Default memory request %sGB", self.mem)
        else:
            self.mem = mem
            logger.debug("User-provided procs per node %s", self.ppn)

        self.extras = ""
        self.print_script = print_script
        logger.debug("Print script? %s", self.print_script)

        self.save_script = save_script
        logger.debug("Save script name: %s", save_script)

        self.cores_per_node = system.queues[self.queue].cores_per_node
        self.numa_per_node = system.queues[self.queue].numa_per_node

        self.outfile = outfile
        self.errfile = errfile
        self.jobname = jobname
        self.directives = ""

        self.do_name = self.jobname != ""
        self.do_stdout = self.outfile != ""
        self.do_stderr = self.errfile != ""

        if self.nprocs < self.ppn:
            rounded_up_cores = self.nprocs
        else:
            rounded_up_cores = math.ceil(self.nprocs / self.ppn) * self.ppn

        self.global_format_dict = PreserveFormatDict(
            jobname=self.jobname,
            comma_sep_vars=self.env,
            cores=self.nprocs,
            rounded_up_cores=rounded_up_cores,
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

    def set_directives(self, system: HPCSystem):
        """Batch directive setter

        In the case where --opts-style directive has been specified, write
        the batch directives line-by-line to the self.directives class
        variable. This is a best effort and not intended to be used to
        write production-ready scripts. Instead, it is more of a guide as
        to what a batch script for a particular problem will look like.
        When actually submitting jobs, the script is kept as simple as
        possible and all directives are specified on the commandline in
        order to reduce the likelihood of submitting a bad script.

        Args:
          system:
            The HPCSystem object detected at import time

        """
        directives: list[str] = []
        attribs_order: list[str | Callable] = [
            "name",
            "procs",
            "time",
            "mem",
            "local_storage",
            "acct",
            "queue",
            system.scheduler.job_size_specific_flags,
            "extras",
            "stdout",
            "stderr",
        ]

        for attrib in attribs_order:
            if isinstance(attrib, str):
                if attrib in system.scheduler.spec:
                    var_test = getattr(self, f"do_{attrib}") if hasattr(self, f"do_{attrib}") else True
                    for f in format_batch_arg_spec(
                        var_test,
                        system.scheduler.spec[attrib],
                        self.global_format_dict,
                        system.scheduler.directive_prefix,
                    ):
                        directives.append(f)
            else:
                for f in attrib():
                    directives.append(f"{system.scheduler.directive_prefix} {f}")

            self.directives = "\n".join(directives)

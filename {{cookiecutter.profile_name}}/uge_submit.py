#!/usr/bin/env python3
"""Creates a submitter class with job details and submits job to UGE"""
import sys
from subprocess import CalledProcessError
from pathlib import Path
from typing import List, Union, Optional
from math import ceil
from snakemake.utils import read_job_properties

if not __name__.startswith("tests.src."):
    sys.path.append(str(Path(__file__).parent.absolute()))
    from CookieCutter import CookieCutter
    from OSLayer import OSLayer
    from uge_config import Config
    from memory_units import Unit, Memory
else:
    from .CookieCutter import CookieCutter
    from .OSLayer import OSLayer
    from .uge_config import Config
    from .memory_units import Unit, Memory


PathLike = Union[str, Path]


class QsubInvocationError(Exception):
    """Error for catching problems with qsub."""


class JobidNotFoundError(Exception):
    """Error for when external job ID isn't found."""


class Submitter:
    """Submitter that extracts job properties from jobscript, formats them into
    a qsub command, submits them to UGE"""

    def __init__(
        self,
        jobscript: PathLike,
        cluster_args: List[str] = None,
        memory_units: Unit = Unit.GIGA,
        uge_config: Optional[Config] = None,
    ):
        if cluster_args is None:
            cluster_args = []
        if uge_config is None:
            uge_config = Config()

        self.jobscript = jobscript
        self.cluster_args = cluster_args
        self.memory_units = memory_units
        self.uge_config = uge_config

    @property
    def job_properties(self) -> dict:
        """Read from a properties line snakemake fills in."""
        return read_job_properties(self.jobscript)

    @property
    def resources(self) -> dict:
        """Resource job properties"""
        return self.job_properties.get("resources", dict())

    @property
    def cluster(self) -> dict:
        """Cluster properties"""
        return self.job_properties.get("cluster", dict())

    # Start building resource command
    @property
    def threads(self) -> int:
        """Threads to use for job."""
        return self.job_properties.get("threads", CookieCutter.get_default_threads())

    @property
    def per_thread(self) -> Memory:
        """Total mem_mb. Defaults to job, cluster, and CookieCutter in that order."""
        mem_value = self.resources.get("mem_mb")
        if not mem_value:
            mem_value = self.cluster.get("mem_mb")
        if not mem_value:
            mem_value = CookieCutter.get_default_mem_mb()
        mem_total = Memory(mem_value, unit=Unit.MEGA).to(self.memory_units)
        if self.threads > 1:
            per_thread = ceil(mem_total.value / self.threads)
        else:
            per_thread = ceil(mem_total.value)
        return per_thread

    @property
    def resource_cmd(self) -> str:
        """Format resource properties for qsub command"""
        res_cmd = ""
        if self.threads > 1:
            p_env = self.resources.get("pe", "def_slot")
            res_cmd += f"-pe {p_env} {self.threads} "
        runtime = self.cluster.get("runtime")
        if runtime:
            runtime = int(runtime)
            hours = runtime // 60
            mins = runtime % 60
            res_cmd += f"-l d_rt={hours}:{mins}:00 -l s_rt={hours}:{mins}:00 "
        res_cmd += f"-l s_vmem={self.per_thread}G -l mem_req={self.per_thread}G"
        return res_cmd

    # Start building jobinfo cmd
    @property
    def is_group_jobtype(self) -> bool:
        """Check if grouped job"""
        return self.job_properties.get("type", "") == "group"

    @property
    def rulename(self) -> str:
        """Get rulename from rule."""
        if self.is_group_jobtype:
            return self.job_properties.get("groupid", "group")
        return self.job_properties.get("rule", "rulename")

    @property
    def jobid(self) -> str:
        """Snakemake jobid"""
        if self.is_group_jobtype:
            return self.job_properties.get("jobid", "").split("-")[0]
        return str(self.job_properties.get("jobid"))

    @property
    def jobname(self) -> str:
        """Create unique jobname based on rule, ids, group and jobid."""
        if self.is_group_jobtype:
            return f"groupid_{self.rulename}.jobid_{self.jobid}"
        else:
            wildcards = self.job_properties.get("wildcards", {}).values()
            wildcards = ".".join(f"{w}" for w in wildcards)
            wildcards = wildcards.replace("/", "-") or "unique"
        return self.cluster.get(
            "jobname",
            f"smk.{self.rulename}.{wildcards}",
        )

    @property
    def logdir(self) -> Path:
        """Get log directory from Cookiecutter."""
        project_logdir = CookieCutter.get_log_dir()
        return Path(project_logdir) / self.rulename

    @property
    def outlog(self) -> Path:
        """Get name of outlog."""
        return self.logdir / f"{self.jobname}.out"

    @property
    def errlog(self) -> Path:
        """Get name of outlog."""
        return self.logdir / f"{self.jobname}.err"

    @property
    def jobinfo_cmd(self) -> str:
        """Format part of command with logs and name."""
        return f'-o "{self.outlog}" -e "{self.errlog}" -N "{self.jobname}"'

    @property
    def optional_cmd(self) -> str:
        """Format optional parts of command"""
        opt_cmd = ""
        queue = self.cluster.get("queue", CookieCutter.get_default_queue())
        rule_params = self.uge_config.params_for_rule(self.rulename)
        if queue:
            opt_cmd += f" -l {queue}"
        if rule_params:
            opt_cmd += f" {rule_params}"
        if self.cluster_args:
            opt_cmd += f" {' '.join(self.cluster_args)}"
        return opt_cmd

    @property
    def submit_cmd(self) -> str:
        """Format entire command for submitting job"""
        sub_cmd = "qsub -cwd -terse -S /bin/bash "
        if not CookieCutter.get_use_singularity():
            sub_cmd += "-V "
        sub_cmd += f"{self.resource_cmd} {self.jobinfo_cmd}"
        if self.optional_cmd:
            sub_cmd += self.optional_cmd
        sub_cmd += f" {self.jobscript}"
        return sub_cmd

    def _create_logdir(self):
        OSLayer.mkdir(self.logdir)

    def _submit_cmd_and_get_external_job_id(self) -> int:
        output_stream = OSLayer.run_process(self.submit_cmd)[1]
        if not output_stream:
            raise JobidNotFoundError(f"External Job ID not found {self.submit_cmd}")
        return int(output_stream)

    def submit(self):
        """Submit the job"""
        self._create_logdir()

        try:
            external_job_id = self._submit_cmd_and_get_external_job_id()
            OSLayer.print(external_job_id)
        except CalledProcessError as error:
            raise QsubInvocationError from error
        except AttributeError as error:
            raise JobidNotFoundError from error


if __name__ == "__main__":
    workdir = Path().resolve()
    config_file = workdir / "uge.yaml"
    if config_file.exists():
        with config_file.open() as stream:
            uge_conf = Config.from_stream(stream)
    else:
        uge_conf = Config()

    JOBSCRIPT = sys.argv[-1]
    clus_args = sys.argv[1:-1]
    uge_submit = Submitter(
        jobscript=JOBSCRIPT,
        uge_config=uge_conf,
        cluster_args=clus_args,
    )
    uge_submit.submit()

#!/usr/bin/env python3
"""Submits jobs to UGE"""
import math
import subprocess
import sys
from pathlib import Path
from typing import List, Union, Optional

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
    pass


class JobidNotFoundError(Exception):
    """Error for when external job ID isn't found."""
    pass


class Submitter:
    """Submitter that takes in jobscript and gets submit commands"""
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

        self._jobscript = jobscript
        self._cluster_cmd = " ".join(cluster_args)
        self._memory_units = memory_units
        self._job_properties = read_job_properties(self._jobscript)
        self.uge_config = uge_config

    @property
    def jobscript(self) -> str:
        """Path to jobscript."""
        return self._jobscript

    @property
    def job_properties(self) -> dict:
        """Read from a properties line snakemake fills in."""
        return self._job_properties

    @property
    def cluster(self) -> dict:
        """Cluster specific commands."""
        return self.job_properties.get("cluster", dict())

    @property
    def threads(self) -> int:
        """Threads to use for job."""
        return self.job_properties.get(
            "threads", CookieCutter.get_default_threads())

    @property
    def resources(self) -> dict:
        """Values for job resources (set by defaults and rules.)"""
        return self.job_properties.get("resources", dict())

    @property
    def mem_mb(self) -> Memory:
        """Total mem_mb. Defaults to job, cluster, and CookieCutter in that order."""
        mem_value = self.resources.get("mem_mb")
        if not mem_value:
            mem_value = self.cluster.get("mem_mb")
        if not mem_value:
            mem_value = CookieCutter.get_default_mem_mb()
        return Memory(mem_value, unit=Unit.MEGA)

    @property
    def memory_units(self) -> Unit:
        """Memory units (defaut MB)"""
        return self._memory_units

    @property
    def runtime(self) -> int:
        """Max run time for job on cluster."""
        rtime = self.cluster.get("runtime", None)
        if rtime:
            rtime = int(rtime)
        return rtime

    @property
    def penv(self) -> str:
        """Parallel environment: default def_slot."""
        return self.resources.get("pe", "def_slot")

    @property
    def resources_cmd(self) -> str:
        """Build resource command"""
        mem_in_cluster_units = self.mem_mb.to(self.memory_units)
        if self.threads > 1:
            res_cmd = f"-pe {self.penv} {self.threads} "
            per_thread = round(mem_in_cluster_units.value / self.threads, 2)
            per_thread = math.ceil(per_thread)
        else:
            res_cmd = ""
            per_thread = math.ceil(mem_in_cluster_units.value)
        if CookieCutter.get_use_singularity():
            per_thread = max(4,per_thread)
        # res_cmd += f"-l h_vmem={per_thread}G "
        # res_cmd += f"-l m_mem_free={per_thread}G "
        res_cmd += f"-l s_vmem={per_thread}G "
        res_cmd += f"-l mem_req={per_thread}G"
        if self.runtime:
            hours = self.runtime // 60
            mins = self.runtime % 60
            res_cmd += f" -l h_rt={hours}:{mins}:00"
        return res_cmd

    @property
    def wildcards(self) -> dict:
        """Get rule wildcards"""
        return self.job_properties.get("wildcards", dict())

    @property
    def wildcards_str(self) -> str:
        """Join wildcards with '.'"""
        return (
            (".".join(f"{v}" for v in self.wildcards.values())).replace(
                '/', '-')
            or "unique"
        )

    @property
    def rule_name(self) -> str:
        """Get rulename from rule."""
        if not self.is_group_jobtype:
            return self.job_properties.get("rule", "rule_name")
        return self.groupid

    @property
    def groupid(self) -> str:
        """Get groupname if job is grouped"""
        return self.job_properties.get("groupid", "group")

    @property
    def is_group_jobtype(self) -> bool:
        """Check if grouped job"""
        return self.job_properties.get("type", "") == "group"

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
            return f"{self.groupid}_{self.jobid}"
        return self.cluster.get(
            "jobname",
            f"smk.{self.rule_name}.{self.wildcards_str}",
        )

    @property
    def logdir(self) -> Path:
        """Get log directory from Cookiecutter."""
        project_logdir = CookieCutter.get_log_dir()
        return Path(project_logdir) / self.rule_name

    @property
    def outlog(self) -> Path:
        """Get name of outlog."""
        if self.is_group_jobtype:
            return self.logdir / f"groupid{self.groupid}_jobid{self.jobid}.out"
        return self.logdir / f"{self.jobname}.out"

    @property
    def errlog(self) -> Path:
        """Get name of errlog."""
        if self.is_group_jobtype:
            return self.logdir / f"groupid{self.groupid}_jobid{self.jobid}.err"
        return self.logdir / f"{self.jobname}.err"

    @property
    def jobinfo_cmd(self) -> str:
        """Format part of command with logs and name."""
        return f'-o "{self.outlog}" -e "{self.errlog}" -N "{self.jobname}"'

    @property
    def queue(self) -> str:
        """Get default queue."""
        return self.cluster.get("queue", CookieCutter.get_default_queue())

    @property
    def queue_cmd(self) -> str:
        """Format queue command"""
        return f"-l {self.queue}" if self.queue else ""

    @property
    def rule_specific_params(self) -> str:
        """Get specific to rule parameters."""
        return self.uge_config.params_for_rule(self.rule_name)

    @property
    def cluster_cmd(self) -> str:
        """Get cluster command"""
        return self._cluster_cmd

    @property
    def submit_cmd(self) -> str:
        """Format entire command for submitting job"""
        params = [
            "qsub",
            self.resources_cmd,
            self.jobinfo_cmd,
            self.queue_cmd,
            self.cluster_cmd,
            self.rule_specific_params,
            self.jobscript,
        ]
        return " ".join(p for p in params if p)

    def _create_logdir(self):
        OSLayer.mkdir(self.logdir)

    def _remove_previous_logs(self):
        OSLayer.remove_file(self.outlog)
        OSLayer.remove_file(self.errlog)

    def _submit_cmd_and_get_external_job_id(self) -> int:
        returncode, output_stream, error_stream = OSLayer.run_process(self.submit_cmd)
        if not output_stream:
            raise JobidNotFoundError(f"Job ID not found error for submit command {self.submit_cmd}")
        return int(output_stream)

    def submit(self):
        """Submit the job"""
        self._create_logdir()
        self._remove_previous_logs()
        try:
            external_job_id = self._submit_cmd_and_get_external_job_id()
            OSLayer.print(external_job_id)
        except subprocess.CalledProcessError as error:
            raise QsubInvocationError(error)
        except AttributeError as error:
            raise JobidNotFoundError(error)


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

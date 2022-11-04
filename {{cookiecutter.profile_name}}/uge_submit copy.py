"""Creates a submitter class with job details and submits job to UGE"""
import sys
from pathlib import Path
from typing import List, Union, Optional
from math import ceil
from snakemake.utils import read_job_properties
from .CookieCutter import CookieCutter
from .uge_config import Config
from .memory_units import Unit, Memory

PathLike = Union[str, Path]

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
        self.cluster_cmd = " ".join(cluster_args)
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
        if CookieCutter.get_use_singularity():
            per_thread = max(4, per_thread)
        return per_thread

    @property
    def resource_cmd(self) -> str:
        """Format resource properties for qsub command"""
        res_cmd = ""
        if self.threads > 1:
            p_env = self.resources.get("pe", "def_slot")
            res_cmd += f"-pe {p_env} {self.threads} "
        res_cmd += f"-l s_vmem={self.per_thread}G -l mem_req={self.per_thread}G "
        runtime = self.cluster.get("runtime")
        if runtime:
            runtime = int(runtime)
            hours = runtime // 60
            mins = runtime % 60
            res_cmd += f"-l h_rt={hours}:{mins}:00 -l s_rt={hours}:{mins}:00 "
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
            return f"groupid{self.rulename}_jobid{self.jobid}"
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
            opt_cmd += f"-l {queue}"
        if rule_params:
            opt_cmd += rule_params
        
        # queue cluster Cookiecutter
        # queue_cmd queue
        # rule_specific_params uge_config rulename
        # cluster_cmd cluster_cmd
        # submit_cmd resources_cmd jobinfo_cmd queue_cmd cluster_cmd rule_specific_params jobscript


        # create_logdir logdir

        # remove previous logs outlog errlog

        # submit cmd and get external jobid submit_cmd

        # submit create_logdir remove previous_logs submitfmd_and_get_external_job_id




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

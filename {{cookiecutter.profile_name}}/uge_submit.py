#!/usr/bin/env python3
import math
import re
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
    pass


class JobidNotFoundError(Exception):
    pass


class Submitter:
    def __init__(
        self,
        jobscript: PathLike,
        cluster_cmds: List[str] = None,
        memory_units: Unit = Unit.GIGA,
        uge_config: Optional[Config] = None,
    ):
        if cluster_cmds is None:
            cluster_cmds = []
        if uge_config is None:
            uge_config = Config()

        self._jobscript = jobscript
        self._cluster_cmd = " ".join(cluster_cmds)
        self._memory_units = memory_units
        self._job_properties = read_job_properties(self._jobscript)
        self.uge_config = uge_config

    @property
    def jobscript(self) -> str:
        return self._jobscript

    @property
    def job_properties(self) -> dict:
        return self._job_properties

    @property
    def cluster(self) -> dict:
        return self.job_properties.get("cluster", dict())

    @property
    def threads(self) -> int:
        return self.job_properties.get(
            "threads", CookieCutter.get_default_threads())

    @property
    def resources(self) -> dict:
        return self.job_properties.get("resources", dict())

    @property
    def mem_mb(self) -> Memory:
        mem_value = self.resources.get("mem_mb")
        if not mem_value:
            mem_value = self.cluster.get("mem_mb")
        if not mem_value:
            mem_value = CookieCutter.get_default_mem_mb()
        if {{cookiecutter.use_singularity}}:
            mem_value = max(4096 * self.threads, mem_value)

        return Memory(mem_value, unit=Unit.MEGA)

    @property
    def memory_units(self) -> Unit:
        return self._memory_units

    @property
    def runtime(self) -> int:
        rt = self.cluster.get("runtime", None)
        if rt:
            rt = int(rt)
        return rt

    @property
    def resources_cmd(self) -> str:
        mem_in_cluster_units = self.mem_mb.to(self.memory_units)
        if self.threads > 1:
            res_cmd = f"-pe mpi {self.threads} "
            per_thread = round(mem_in_cluster_units.value / self.threads, 2)
            per_thread = math.ceil(per_thread)
        else:
            res_cmd = ""
            per_thread = math.ceil(mem_in_cluster_units.value)
        res_cmd += f"-l h_vmem={per_thread}G "
        res_cmd += f"-l m_mem_free={per_thread}G "
        res_cmd += f"-l s_vmem={per_thread}G "
        res_cmd += f"-l mem_req={per_thread}G"
        if self.runtime:
            hours = self.runtime // 60
            mins = self.runtime % 60
            res_cmd += f" -l h_rt={hours}:{mins}:00"
        return res_cmd

    @property
    def wildcards(self) -> dict:
        return self.job_properties.get("wildcards", dict())

    @property
    def wildcards_str(self) -> str:
        return (
            (".".join(f"{v}" for v in self.wildcards.values())).replace(
                '/', '-')
            or "unique"
        )

    @property
    def rule_name(self) -> str:
        if not self.is_group_jobtype:
            return self.job_properties.get("rule", "rule_name")
        return self.groupid

    @property
    def groupid(self) -> str:
        return self.job_properties.get("groupid", "group")

    @property
    def is_group_jobtype(self) -> bool:
        return self.job_properties.get("type", "") == "group"

    @property
    def jobname(self) -> str:
        if self.is_group_jobtype:
            return f"{self.groupid}_{self.jobid}"
        return self.cluster.get(
            "jobname",
            f"smk.{self.rule_name}.{self.wildcards_str}",
        )

    @property
    def jobid(self) -> str:
        if self.is_group_jobtype:
            return self.job_properties.get("jobid", "").split("-")[0]
        return str(self.job_properties.get("jobid"))

    @property
    def logdir(self) -> Path:
        project_logdir = CookieCutter.get_log_dir()
        return Path(project_logdir) / self.rule_name

# HERE IS THE PROBLEM REMOVE OUTLOG FROM EVERYTHING
# Here it's not a problem, it's just the name of a file like errlog
    @property
    def outlog(self) -> Path:
        if self.is_group_jobtype:
            return self.logdir / f"groupid{self.groupid}_jobid{self.jobid}.out"
        return self.logdir / f"{self.jobname}.out"

    @property
    def errlog(self) -> Path:
        if self.is_group_jobtype:
            return self.logdir / f"groupid{self.groupid}_jobid{self.jobid}.err"
        return self.logdir / f"{self.jobname}.err"

# HERE IS THE PROBLEM REMOVE OUTLOG FROM EVERYTHING
# Here should also not be a problem.
    @property
    def jobinfo_cmd(self) -> str:
        return f'-o "{self.outlog}" -e "{self.errlog}" -N "{self.jobname}"'

    @property
    def queue(self) -> str:
        return self.cluster.get("queue", CookieCutter.get_default_queue())

    @property
    def queue_cmd(self) -> str:
        return f"-l {self.queue}" if self.queue else ""

    @property
    def rule_specific_params(self) -> str:
        return self.uge_config.params_for_rule(self.rule_name)

    @property
    def cluster_cmd(self) -> str:
        return self._cluster_cmd

    @property
    def submit_cmd(self) -> str:
        params = [
            "qsub -cwd -terse -V",
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
            raise JobidNotFoundError("Job ID not found error.")
        return int(output_stream)
# HERE IS THE PROBLEM REMOVE OUTLOG FROM EVERYTHING
    # def _get_parameters_to_status_script(self, external_job_id: int) -> str:
    #     return "{external_job_id} {outlog}".format(
    #         external_job_id=external_job_id, outlog=self.outlog
    #     )

    def submit(self):
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
            uge_config = Config.from_stream(stream)
    else:
        uge_config = Config()

    jobscript = sys.argv[-1]
    cluster_cmds = sys.argv[1:-1]
    uge_submit = Submitter(
        jobscript=jobscript,
        uge_config=uge_config,
        cluster_cmds=cluster_cmds,
    )
    uge_submit.submit()

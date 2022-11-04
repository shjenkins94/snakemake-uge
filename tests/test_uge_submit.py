"""Module for testing uge_submit."""
from json import dumps
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List
from pytest_cases import parametrize, fixture
from .src.CookieCutter import CookieCutter
from .src.uge_submit import Submitter


def uge_sub(j_properties: str, clus_args: List[str] = None):
    """Fixture that creates a jobscript using j_properties and returns a Submitter"""
    j_script = Path(NamedTemporaryFile(delete=False, suffix=".sh").name)

    j_script.write_text(
        "#!/bin/sh\n# " f"properties = {j_properties}\necho something", encoding="utf-8"
    )

    return Submitter(jobscript=str(j_script), cluster_args=clus_args), str(j_script)


@fixture(unpack_into="uge_bare,jobscript_bare")
def sub_bare():
    """Submitter with only mandatory parameters"""
    properties = dumps(
        {
            "type": "single",
            "rule": "search",
            "jobid": 2,
        }
    )
    return uge_sub(properties)


@fixture(unpack_into="uge_full,jobscript_full")
def sub_full():
    """Submitter with every optional parameter"""
    properties = dumps(
        {
            "type": "single",
            "rule": "search",
            "wildcards": {"i": "0", "messy": "fee/foo"},
            "params": {"threshold": 1.0, "cache_mem_gb": 2},
            "threads": 4,
            "resources": {"mem_mb": 5000, "runtime": 120, "pe": "mpi"},
            "jobid": 2,
            "cluster": {"queue": "q1", "runtime": 5000},
        }
    )

    clus_args = [
        "cluster_opt_1",
        "cluster_opt_2",
        "cluster_opt_3",
    ]
    return uge_sub(properties, clus_args=clus_args)


@fixture(unpack_into="uge_high,jobscript_high")
def sub_group():
    """Submitter for group job with high memory requirements"""
    properties = dumps(
        {
            "type": "group",
            "groupid": "mygroup",
            "rule": "search",
            "wildcards": {"i": "0", "messy": "fee/foo"},
            "params": {"threshold": 1.0, "cache_mem_gb": 2},
            "threads": 4,
            "resources": {"mem_mb": 36000},
            "jobid": "a9722c33-51ba-5ac4-9f17-bab04c68bc3d",
        }
    )
    return uge_sub(properties)


@fixture
def mocked_cookie(mocker, sing):
    """Fixture that mocks a Snakemake Cookiecutter object"""
    mocker.patch.object(CookieCutter, "get_log_dir", return_value="logdir")
    mocker.patch.object(CookieCutter, "get_default_mem_mb", return_value=1000)
    mocker.patch.object(CookieCutter, "get_use_singularity", return_value=sing)
    mocker.patch.object(CookieCutter, "get_default_threads", return_value=1)
    mocker.patch.object(CookieCutter, "get_default_queue", return_value="")


@fixture
def log_path():
    """Get the path for the logdir once so it doesn't clutter up all the tests"""
    return Path("logdir")


class TestSubmitterProperties:
    """Test that Submitter properties are correct"""

    @parametrize(
        "sing,per_thread,var",
        [(False, 2, "-V "), (True, 4, "")],
        ids=["no_sing", "sing"],
    )
    def test_full(
        self, uge_full, jobscript_full, sing, per_thread, var, log_path, mocked_cookie
    ):
        """Test a jobscript for a single job with all properties"""
        assert uge_full.jobscript == jobscript_full
        assert uge_full.threads == 4
        assert uge_full.per_thread == per_thread
        assert (
            uge_full.resource_cmd == "-pe mpi 4 -l h_rt=83:20:00 -l s_rt=83:20:00 "
            f"-l s_vmem={per_thread}G -l mem_req={per_thread}G"
        )
        assert uge_full.is_group_jobtype is False
        assert uge_full.jobid == "2"
        rulename = "search"
        jobname = "smk.search.0.fee-foo"
        assert uge_full.rulename == rulename
        assert uge_full.jobname == jobname
        assert uge_full.logdir == log_path / rulename
        assert uge_full.outlog == log_path / rulename / f"{jobname}.out"
        assert uge_full.errlog == log_path / rulename / f"{jobname}.err"
        assert (
            uge_full.jobinfo_cmd == f'-o "{log_path / rulename}/{jobname}.out" '
            f'-e "{log_path / rulename}/{jobname}.err" '
            f'-N "{jobname}"'
        )
        assert (
            uge_full.optional_cmd == " -l q1 cluster_opt_1 cluster_opt_2 cluster_opt_3"
        )
        assert (
            uge_full.submit_cmd == f"qsub -cwd -terse -S bin/bash {var}"
            "-pe mpi 4 -l h_rt=83:20:00 -l s_rt=83:20:00 "
            f"-l s_vmem={per_thread}G -l mem_req={per_thread}G "
            f'-o "{log_path / rulename}/{jobname}.out" '
            f'-e "{log_path / rulename}/{jobname}.err" -N "{jobname}" '
            f"-l q1 cluster_opt_1 cluster_opt_2 cluster_opt_3 "
            f"{jobscript_full}"
        )

    @parametrize(
        "sing,per_thread,var",
        [(False, 1, "-V "), (True, 4, "")],
        ids=["no_sing", "sing"],
    )
    def test_bare(
        self, uge_bare, jobscript_bare, sing, per_thread, var, log_path, mocked_cookie
    ):
        """Test a jobscript for a single job with all properties"""
        assert uge_bare.jobscript == jobscript_bare
        assert uge_bare.threads == 1
        assert uge_bare.per_thread == per_thread
        assert (
            uge_bare.resource_cmd == f"-l s_vmem={per_thread}G -l mem_req={per_thread}G"
        )
        rulename = "search"
        jobname = "smk.search.unique"
        assert (
            uge_bare.submit_cmd == f"qsub -cwd -terse -S bin/bash {var}"
            f"-l s_vmem={per_thread}G -l mem_req={per_thread}G "
            f'-o "{log_path / rulename}/{jobname}.out" '
            f'-e "{log_path / rulename}/{jobname}.err" -N "{jobname}" {jobscript_bare}'
        )

    @parametrize(
        "sing,per_thread,var",
        [(False, 9, "-V "), (True, 9, "")],
        ids=["no_sing", "sing"],
    )
    def test_high(
        self, uge_high, jobscript_high, sing, per_thread, var, log_path, mocked_cookie
    ):
        """Test a jobscript for a single job with all properties"""
        assert uge_high.jobscript == jobscript_high
        assert uge_high.threads == 4
        assert uge_high.per_thread == per_thread
        assert (
            uge_high.resource_cmd
            == f"-pe def_slot 4 -l s_vmem={per_thread}G -l mem_req={per_thread}G"
        )
        assert uge_high.is_group_jobtype is True
        assert uge_high.jobid == "a9722c33"
        rulename = "mygroup"
        jobname = "groupid_mygroup.jobid_a9722c33"
        assert uge_high.rulename == rulename
        assert uge_high.jobname == jobname
        assert uge_high.logdir == log_path / rulename
        assert uge_high.outlog == log_path / rulename / f"{jobname}.out"
        assert uge_high.errlog == log_path / rulename / f"{jobname}.err"
        assert (
            uge_high.jobinfo_cmd == f'-o "{log_path / rulename}/{jobname}.out" '
            f'-e "{log_path / rulename}/{jobname}.err" '
            f'-N "{jobname}"'
        )
        assert uge_high.optional_cmd == ""
        assert (
            uge_high.submit_cmd == f"qsub -cwd -terse -S bin/bash {var}"
            f"-pe def_slot 4 -l s_vmem={per_thread}G -l mem_req={per_thread}G "
            f'-o "{log_path / rulename}/{jobname}.out" '
            f'-e "{log_path / rulename}/{jobname}.err" -N "{jobname}" '
            f"{jobscript_high}"
        )

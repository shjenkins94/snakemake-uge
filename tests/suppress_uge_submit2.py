"""Module for testing uge_submit."""
from pytest_cases import parametrize, fixture
from .src.CookieCutter import CookieCutter
from .src.uge_submit import Submitter


@fixture
def uge_sub(js_suff):
    """Fixture that creates Submitters from suffixes"""
    jscript = f"tests/real_jobscript{js_suff}.sh"
    clus_arg = ["cluster_opt_1", "cluster_opt_2", "cluster_opt_3",]
    return Submitter(jobscript=jscript, cluster_cmds=clus_arg)

@fixture
def mocked_cookie(mocker, sing):
    """Fixture that mocks a Snakemake Cookiecutter object"""
    mocker.patch.object(CookieCutter, "get_log_dir", return_value="logdir")
    mocker.patch.object(CookieCutter, "get_default_mem_mb", return_value=1000)
    mocker.patch.object(CookieCutter, "get_use_singularity", return_value=sing)
    mocker.patch.object(CookieCutter, "get_default_threads", return_value=8)


class TestSubmitterProperties:
    """Test that Submitter properties are correct"""
    @parametrize("js_suff", ["", "_bare", "_high"])
    def test_jobscript(self, js_suff, uge_sub):
        """jobscript"""
        assert uge_sub.jobscript == f"tests/real_jobscript{js_suff}.sh"
    # job_properties
    # cluster
    @parametrize("js_suff,sing,expect", [("", False, 4), ("_bare", False, 8)],
    ids=["mem_mb_in_jobscript", "mem_mb_not_in_jobscript"])
    def test_threads(self, js_suff, sing, expect, uge_sub, mocked_cookie):
        """threads"""
        assert uge_sub.threads == expect
    # resources
    # @parametrize("js_suff,sing,expect", [("", False, 4), ("_bare", False, 8)],
    # ids=["mem_mb_in_jobscript", "mem_mb_not_in_jobscript"])
    # def test_pe(self, js_suff, expect, uge_sub):
    #     """parallel environment"""
    #     assert uge_sub.threads == expect
    # @parametrize("suff,sing,expect",
    #              [("", False, 4),
    #               ("_bare", False, 8)],
    #               ids=["mem_mb_in_jobscript",
    #                    "mem_mb_not_in_jobscript"])
    # def case_threads(self, suff, sing, expect, mocked_cookie):
    #     sub = make_sub(suff)
    #     return sub, "threads", expect


# class TestGetSubmitter:
#     # A bunch of these only need to be checked once
#     @parametrize("suff,attr,expect",
#                 [("", "cluster_cmd", "cluster_opt_1 cluster_opt_2 cluster_opt_3"),
#                  ("", "jobid", "2"),
#                  ("", "wildcards_str", "0"),
#                  ("", "rule_name", "search_fasta_on_index"),
#                  ("", "is_group_jobtype", False),
#                  ("", "jobname", "smk.search_fasta_on_index.0"),])
#     def test_get_once(self, suff, attr, expect):
#         sub = make_sub(suff)
#         assert getattr(sub, attr) == expect
    
#     @parametrize("suff", ["", "_bare", "_high"], ids=["full", "bare", "high"])
#     def test_get_jobscript(self, suff):
#         js = f"tests/real_jobscript{suff}.sh"
#         sub = make_sub(suff)
#         assert sub.jobscript == js
    
#     @parametrize("suff,pe", [("", "mpi"), ("_bare", "def_slot")], ids=["full", "bare"])
#     def test_get_pe(self, suff, pe):
#         sub = make_sub(suff)
#         assert sub.pe == pe
    
#     @parametrize("suff,sing,expect",
#                  [("", False, Memory(5000, Unit.MEGA)),
#                   ("_bare", False, Memory(1000, Unit.MEGA))],
#                   ids=["mem_mb_in_jobscript",
#                        "mem_mb_not_in_jobscript"])
#     def test_get_mem_mb(self, suff, sing, expect, mocked_cookiecutter):
#         sub = make_sub(suff)
#         assert sub.mem_mb == expect
    



    
    ## Simplify to that next time
# jobid
# jobscript

# wildcards
# jobscript

# rule_name
# jobscript

# group
# jobscript

# resources cmd
# different based on jobscript, cookiecutter

# jobname
# jobscript

# logdir
# jobscript

# outlog
# jobscript

# errlog
# jobscript

# jobinfo
# jobscrpt

#queue
# jobscript
# cookiecutter

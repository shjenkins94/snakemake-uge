#!/usr/bin/env python3

import sys
import time
from pathlib import Path

if not __name__.startswith("tests.src."):
    sys.path.append(str(Path(__file__).parent.absolute()))
    from OSLayer import OSLayer
    from CookieCutter import CookieCutter
else:
    from .CookieCutter import CookieCutter
    from .OSLayer import OSLayer


class QstatError(Exception):
    pass


class UnknownStatusLine(Exception):
    pass


class StatusChecker:
    SUCCESS = "success"
    RUNNING = "running"
    FAILED = "failed"
    STATUS_TABLE = {
        "r": RUNNING,
        "x": RUNNING,
        "t": RUNNING,
        "s": RUNNING,
        "R": RUNNING,
        "qw": RUNNING,
        "d": FAILED,
        "E": FAILED,
        "FAIL": FAILED,
        "1": FAILED,
        "SUCCESS": SUCCESS,
        "0": SUCCESS
    }

    """
    From man qstat:
        the  status  of  the  job  -  one  of  d(eletion),  E(rror), h(old),
        r(unning), R(estarted), s(uspended),  S(uspended),  e(N)hanced  sus-
        pended, (P)reempted, t(ransfering), T(hreshold) or w(aiting).

    """
    def __init__(
        self,
        jobid: int,
    ):
        self._jobid = jobid

    @property
    def jobid(self) -> int:
        return self._jobid

    @property
    def statlog(self) -> str:
        return f"{CookieCutter.get_stat_dir()}/{self.jobid}.exit"

    @property
    def latency_wait(self) -> int:
        return CookieCutter.get_latency_wait()

    @property
    def max_status_checks(self) -> int:
        return CookieCutter.get_max_qstat_checks()

    @property
    def wait_between_tries(self) -> float:
        return CookieCutter.get_time_between_qstat_checks()

    @property
    def log_status_checks(self) -> bool:
        return CookieCutter.get_log_status_checks()

    @property
    def qstatj_query_cmd(self) -> str:
        return "qstat -j {jobid}".format(jobid=self.jobid)

    @property
    def qdel_cmd(self) -> str:
        return f"qdel -j {self.jobid}"

    def _qstat_job_state(self, output_stream) -> str:
        state = ""
        if output_stream:
            for line in output_stream.split("\n"):
                if str(self.jobid) == line.split()[0]:
                    state = line.split()[4]
                    break  # exit for loop
        return state

    def _query_status_using_qstat(self) -> str:
        returncode, output_stream, error_stream = OSLayer.run_process("qstat")
        status = self._qstat_job_state(output_stream)
        if not status:
            raise QstatError(
                "qstat failed on job {jobid} with empty output".format(
                    jobid=self.jobid,
                )
            )
        if status not in self.STATUS_TABLE.keys():
            raise KeyError(
                "Unknown job status '{status}' for {jobid}".format(
                    status=status, jobid=self.jobid)
            )
        return self.STATUS_TABLE[status]

    def _query_status_using_cluster_log(self) -> str:
        try:
            lastline = OSLayer.tail(self.statlog, num_lines=1)
        except (FileNotFoundError, ValueError):
            return self.STATUS_TABLE["r"]

        status = lastline[0].strip().decode("utf-8")
        if status not in self.STATUS_TABLE.keys():
            raise KeyError(
                            "Unknown job status '{status}' for {jobid}".format(
                                status=status, jobid=self.jobid)
                            )
        else:
            return self.STATUS_TABLE[status]

    def get_status(self) -> str:
        status = None
        for _ in range(self.max_status_checks):
            try:
                status = self._query_status_using_qstat()
                break  # succeeded on getting the status
            except QstatError as error:
                if self.log_status_checks:
                    if error is None:
                        print("No result for Qstat.", file=sys.stderr)
                    else:
                        print(
                            "[Predicted exception] "
                            "QstatError: {error}".format(error=error),
                            file=sys.stderr,
                            )
                    print("Resuming...", file=sys.stderr)
                time.sleep(self.wait_between_tries)

            except KeyError as error:
                if self.log_status_checks:
                    print(
                        "[Predicted exception] {error}".format(
                            error=error
                        ),
                        file=sys.stderr,
                    )
                    print("Resuming...", file=sys.stderr)
                time.sleep(self.wait_between_tries)

        if status is None or status == "finished":
            if self.log_status_checks:
                if status is None:
                    print(
                        "qstat for job {jobid} failed "
                        "{try_times} times.".format(
                            jobid=self.jobid,
                            try_times=self.max_status_checks
                        ),
                        file=sys.stderr,
                    )
                if status == "finished":
                    print(
                            "Job {jobid} finished, check status via "
                            "cluster status log".format(
                                jobid=self.jobid),
                            file=sys.stderr
                        )
                print(
                        "Checking exit status for job {jobid} via "
                        "cluster status log".format(
                            jobid=self.jobid),
                        file=sys.stderr
                        )
            status = self._query_status_using_cluster_log()
        return status


if __name__ == "__main__":
    jobid = sys.argv[1]
    uge_status_checker = StatusChecker(jobid)
    try:
        print(uge_status_checker.get_status())
    except KeyboardInterrupt:
        sys.exit(0)

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
        return f"qstat -j {self.jobid}"

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

    def _status_key_check(self, status) -> str:
        if status not in self.STATUS_TABLE.keys():
            raise KeyError(
                f"Unknown job status '{status}' for {self.jobid}"
            )
        return self.STATUS_TABLE[status]

    def _query_status_using_qstat(self) -> str:
        returncode, output_stream, error_stream = OSLayer.run_process("qstat")
        status = self._qstat_job_state(output_stream)
        if not status:
            raise QstatError(
                f"qstat failed on job {self.jobid} with empty output"
            )
        return status

    def _query_status_using_cluster_log(self) -> str:
        lastline = OSLayer.tail(self.statlog, num_lines=1)
        status = lastline[0].strip().decode("utf-8")
        return status

    def get_status(self) -> str:
        status_key = None
        status = None
        if self.log_status_checks:
            print("Checking for status log...", file=sys.stderr)
        for _ in range(self.max_status_checks):
            try:
                status_key = self._query_status_using_cluster_log()
                if self.log_status_checks:
                    print("Status log found.", file=sys.stderr)
            except FileNotFoundError:
                if self.log_status_checks:
                    print("No status log, trying qstat...", file=sys.stderr)
                try:
                    status_key = self._query_status_using_qstat()
                    if self.log_status_checks:
                        print("Qstat status found.", file=sys.stderr)
                except QstatError as error:
                    if self.log_status_checks:
                        print(
                            f"[Predicted exception] QstatError: {error}",
                            file=sys.stderr,
                            )
                        print("Resuming...", file=sys.stderr)
                    time.sleep(self.wait_between_tries)
                    continue
            try:
                status = self._status_key_check(status_key)
                break  # succeeded on getting the status
            except KeyError as error:
                if self.log_status_checks:
                    print(
                        f"[Predicted exception] {error}",
                        file=sys.stderr,
                    )
                    print("Resuming...", file=sys.stderr)
                time.sleep(self.wait_between_tries)
        if status is None:
            if self.log_status_checks:
                print(
                    f"Failed to get status for {self.jobid} "
                    f"{self.max_status_checks} times",
                    file=sys.stderr,
                )
            status = "failed"
        return status


if __name__ == "__main__":
    jobid = sys.argv[1]
    uge_status_checker = StatusChecker(jobid)
    try:
        print(uge_status_checker.get_status())
    except KeyboardInterrupt:
        sys.exit(0)

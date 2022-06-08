#!/usr/bin/env python3

import sys
import time
import re
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


class QacctError(Exception):
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
        project_statdir = CookieCutter.get_stat_dir()
        return "{statdir}/{jobid}.exit".format(statdir=project_statdir,
                                               jobid=self.jobid)

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
    def qstat_query_cmd(self) -> str:
        return "qstat -j {jobid}".format(jobid=self.jobid)

    @property
    def qdel_cmd(self) -> str:
        return "qdel -j {jobid}".format(jobid=self.jobid)

    def _query_status_using_qstat(self) -> str:
        returncode, output_stream, error_stream = OSLayer.run_process(
            self.qstat_query_cmd
        )
        if returncode != 0:
            if error_stream.startswith("Following jobs do not exist"):
                return "finished"
            raise QstatError(
                    "qstat failed on job {jobid} with: {error}".format(
                        jobid=self.jobid, error=error_stream
                    )
            )

        if not output_stream:
            raise QstatError(
                "qstat failed on job {jobid} with empty output".format(
                    jobid=self.jobid,
                )
            )
        status = self._qstat_job_state(output_stream)
        if status not in self.STATUS_TABLE.keys():
            raise KeyError(
                "Unknown job status '{status}' for {jobid}".format(
                    status=status, jobid=self.jobid)
            )
        return self.STATUS_TABLE[status]

        # hung_status = self._handle_hung_qstat(output_stream)
        # if hung_status or self._qstat_error(output_stream):
        #    status = "failed"
        # else:
        #    status = "running"

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

    @staticmethod
    def _extract_time(line, time_name) -> float:
        """ Extracts time elapsed in seconds from usage line for given name
        """
        result = re.search(f"{time_name}=([^,]+)(,|$,\n)", line)
        if not result:
            return 0
        time_str = re.search(f"{time_name}=([^,]+)(,|$,\n)", line).group(1)
        elapsed_time = 0
        multiplier = 1
        multipliers = (1, 60, 60, 24)
        for t, m in zip(reversed(time_str.split(":")), multipliers):
            elapsed_time += multiplier * m * int(t)
            multiplier *= m
        return elapsed_time

    @staticmethod
    def _qstat_job_state(output_stream) -> str:
        state = ""
        for line in output_stream.split("\n"):
            if line.startswith("job_state"):
                state = line.strip()[-2:].strip()
                break  # exit for loop
        return state

    def _handle_hung_qstat(self, output_stream) -> str:
        for line in output_stream.split("\n"):
            if line.startswith("usage"):
                wallclock = self._extract_time(line, "wallclock")
                if wallclock < self.cpu_hung_min_time * 60:
                    return False
                cpu = self._extract_time(line, "cpu")
                if (cpu / wallclock) < self.cpu_hung_max_ratio:
                    (
                        returncode,
                        output_stream,
                        error_stream,
                    ) = OSLayer.run_process(self.qdel_cmd)
                    return True
                return False
            return False

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
            #time.sleep(self.latency_wait)

            status = self._query_status_using_cluster_log()
        return status

if __name__ == "__main__":
    jobid = sys.argv[1]
    uge_status_checker = StatusChecker(jobid)
    try:
        print(uge_status_checker.get_status())
    except KeyboardInterrupt:
        sys.exit(0)

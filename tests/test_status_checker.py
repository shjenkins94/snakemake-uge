import unittest
from unittest.mock import patch

from tests.src.OSLayer import OSLayer
from tests.src.CookieCutter import CookieCutter
from tests.src.uge_status import StatusChecker, QstatError


def assert_called_n_times_with_same_args(mock, n, args):
    assert mock.call_count == n
    mock_args = [a[0] for a, _ in mock.call_args_list]
    for actual, expected in zip(mock_args, args):
        assert actual == expected


def qstat_line(status) -> str:
    line = (f"  123 0.0 job user  {status}    06/09/2022 14:49:50             "
            "                                                      1        ")
    return line


class TestStatusChecker(unittest.TestCase):
    # Test statlog
    @patch.object(CookieCutter, "get_stat_dir", return_value=".cluster_status")
    def test_statlog(self, *othermocks):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.statlog
        expected = ".cluster_status/123.exit"
        self.assertEqual(actual, expected)

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=3)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(
        OSLayer, "run_process", return_value=(0, qstat_line("r"), ""))
    def test_get_status_qstat_says_process_is_r_job_status_is_running(
        self, run_process_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "running"
        self.assertEqual(actual, expected)
        run_process_mock.assert_called_once_with("qstat")

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=3)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(
        OSLayer, "run_process", return_value=(0, qstat_line("t"), ""))
    def test_get_status_qstat_says_process_is_t_job_status_is_running(
        self, run_process_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "running"
        self.assertEqual(actual, expected)
        run_process_mock.assert_called_once_with("qstat")

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=3)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(
        OSLayer, "run_process", return_value=(0, qstat_line("s"), ""))
    def test_get_status_qstat_says_process_is_s_job_status_is_running(
        self, run_process_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "running"
        self.assertEqual(actual, expected)
        run_process_mock.assert_called_once_with("qstat")

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=3)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(
        OSLayer, "run_process", return_value=(0, qstat_line("qw"), ""))
    def test_get_status_qstat_says_process_is_qw_job_status_is_running(
        self, run_process_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "running"
        self.assertEqual(actual, expected)
        run_process_mock.assert_called_once_with("qstat")

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=3)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(
        OSLayer, "run_process", return_value=(0, qstat_line("d"), ""))
    def test_get_status_qstat_says_process_is_d_job_status_is_failed(
        self, run_process_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "failed"
        self.assertEqual(actual, expected)
        run_process_mock.assert_called_once_with("qstat")

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=3)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(
        OSLayer, "run_process", return_value=(0, qstat_line("E"), ""))
    def test_get_status_qstat_says_process_is_E_job_status_is_failed(
        self, run_process_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "failed"
        self.assertEqual(actual, expected)
        run_process_mock.assert_called_once_with("qstat")

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=3)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(OSLayer, "run_process")
    def test_get_status_qstat_fails_twice_succeeds_third_job_status_is_success(
        self, run_process_mock, *othermocks
    ):
        run_process_mock.side_effect = [
            QstatError,
            KeyError("test"),
            (0, qstat_line("r"), "")
        ]
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "running"
        self.assertEqual(actual, expected)
        assert_called_n_times_with_same_args(
            run_process_mock, 3, ["qstat"] * 3
        )

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=1)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(CookieCutter, "get_latency_wait", return_value=45)
    @patch.object(
        CookieCutter, "get_stat_dir", return_value=".cluster_status")
    @patch.object(OSLayer, "tail", return_value=([b'0']))
    @patch.object(OSLayer, "run_process", return_value=(1, "", ""))
    def test_get_status_qstat_fail_using_log_job_status_is_success(
        self, run_process_mock, tail_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "success"
        self.assertEqual(actual, expected)
        run_process_mock.assert_called_once_with("qstat")
        tail_mock.assert_called_once_with(".cluster_status/123.exit",
                                          num_lines=1)

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=1)
    @patch.object(
        CookieCutter, "get_time_between_qstat_checks", return_value=1)
    @patch.object(CookieCutter, "get_latency_wait", return_value=45)
    @patch.object(
        CookieCutter, "get_stat_dir", return_value=".cluster_status")
    @patch.object(OSLayer, "tail", return_value=([b'1']))
    @patch.object(OSLayer, "run_process", return_value=(1, "", ""))
    def test_get_status_qstat_fail_using_log_job_status_is_failed(
        self, run_process_mock, tail_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        actual = uge_status_checker.get_status()
        expected = "failed"
        self.assertEqual(actual, expected)
        run_process_mock.assert_called_once_with("qstat")
        tail_mock.assert_called_once_with(".cluster_status/123.exit",
                                          num_lines=1)

    @patch.object(CookieCutter, "get_max_qstat_checks", return_value=4)
    @patch.object(OSLayer, "run_process", return_value=(0, "", ""))
    def test_query_status_using_qstat_empty_stdout_raises_QstatError(
        self, run_process_mock, *othermocks
    ):
        uge_status_checker = StatusChecker(123)
        self.assertRaises(
            QstatError, uge_status_checker._query_status_using_qstat)
        run_process_mock.assert_called_once_with("qstat")


if __name__ == "__main__":
    unittest.main()

"""Python wrapper for cookiecutter"""


class CookieCutter:
    """
    Cookie Cutter wrapper
    """

    @staticmethod
    def get_use_singularity() -> bool:
        """Get use_singularity from cookiecutter"""
        return "{{cookiecutter.use_singularity}}" == "True"

    @staticmethod
    def get_default_threads() -> int:
        """Get default threads from cookiecutter"""
        return int("{{cookiecutter.default_threads}}")

    @staticmethod
    def get_default_mem_mb() -> int:
        """Get default memory in MB from cookiecutter"""
        return int("{{cookiecutter.default_mem_mb}}")

    @staticmethod
    def get_log_dir() -> str:
        """Get log directory from cookiecutter"""
        return "{{cookiecutter.default_cluster_logdir}}"

    @staticmethod
    def get_default_queue() -> str:
        """Get default queue from cookiecutter"""
        return "{{cookiecutter.default_queue}}"

    @staticmethod
    def get_log_status_checks() -> bool:
        """Get whether to log status checks from cookiecutter"""
        return "{{cookiecutter.log_status_checks}}" == "True"

    @staticmethod
    def get_latency_wait() -> int:
        """Get latency wait from cookiecutter"""
        return int("{{cookiecutter.latency_wait}}")

    @staticmethod
    def get_max_qstat_checks() -> int:
        """Get max number of status checks from cookiecutter"""
        return int("{{cookiecutter.max_qstat_checks}}")

    @staticmethod
    def get_time_between_qstat_checks() -> float:
        """Get time between qstat checks from cookiecutter"""
        return float("{{cookiecutter.time_between_qstat_checks}}")

    @staticmethod
    def get_stat_dir() -> str:
        """Get cluster status log directory from cookiecutter"""
        return "{{cookiecutter.default_cluster_statdir}}"

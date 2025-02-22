import subprocess
import uuid
from pathlib import Path
from typing import Tuple, List

stdout = str
stderr = str


class TailError(Exception):
    pass


class OSLayer:
    """
    This class provides an abstract layer to communicating with the OS.
    Its main purpose is to enable OS operations mocking, so we don't actually
    need to make file operations or create processes.
    """

    @staticmethod
    def mkdir(directory: Path):
        directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def remove_file(file: Path):
        if file.is_file():
            file.unlink()

    @staticmethod
    def run_process(cmd: str) -> Tuple[stdout, stderr]:
        completed_process = subprocess.run(
            cmd, check=False, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        return (
            completed_process.returncode,
            completed_process.stdout.decode().strip(),
            completed_process.stderr.decode().strip(),
        )

    @staticmethod
    def print(string: str):
        print(string)

    @staticmethod
    def get_uuid4_string() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def checkfile(path: str) -> bool:
        if not Path(path).exists():
            raise FileNotFoundError(f"{path} does not exist.")
        return True

    @staticmethod
    def tail(path: str, num_lines: int = 10) -> List[bytes]:
        if not Path(path).exists():
            raise FileNotFoundError(f"{path} does not exist.")

        process = subprocess.Popen(
            ["tail", "-n", str(num_lines), path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        exit_code = process.wait()
        if exit_code != 0:
            raise TailError(
                f"Failed to execute the tail command on the file {path} due "
                f"to the following error:\n{process.stderr.read().decode()}"
            )
        return process.stdout.readlines()

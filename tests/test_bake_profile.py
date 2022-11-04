"""Test Baking the profile"""
from contextlib import contextmanager
import shlex
import os
import subprocess
from cookiecutter.utils import rmtree


@contextmanager
def bake_in_temp_dir(cookies, *args, **kwargs):
    """
    Delete the temporal directory that is created when executing the tests
    :param cookies: pytest_cookies.Cookies,
        cookie to be baked and its temporal files will be removed
    """
    result = cookies.bake(*args, **kwargs)
    try:
        yield result
    finally:
        rmtree(str(result.project_path))


def test_bake_custom_project(cookies):
    """Test for 'cookiecutter-template'."""
    result = cookies.bake()
    assert result.exit_code == 0
    assert result.exception is None

    assert result.project_path.name == "uge"
    assert result.project_path.is_dir()

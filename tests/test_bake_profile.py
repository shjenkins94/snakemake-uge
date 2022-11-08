"""Test Baking the profile"""


def test_bake_custom_project(cookies):
    """Test for 'cookiecutter-template'."""
    result = cookies.bake()
    assert result.exit_code == 0
    assert result.exception is None

    assert result.project_path.name == "uge"
    assert result.project_path.is_dir()

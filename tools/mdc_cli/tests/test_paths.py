"""Tests for mdc_cli.paths."""

from mdc_cli.paths import (
    REPO_ROOT,
    api_env_file,
    default_etl_profile,
    etl_env_file,
    iter_status_targets,
)


def test_repo_root_is_monorepo_root():
    assert (REPO_ROOT / "api").is_dir()
    assert (REPO_ROOT / "etl_pipeline").is_dir()
    assert (REPO_ROOT / "tools" / "mdc_cli").is_dir()


def test_env_file_paths():
    assert api_env_file("local") == REPO_ROOT / "api" / ".env_api_local"
    assert etl_env_file("clinic") == REPO_ROOT / "etl_pipeline" / ".env_clinic"


def test_default_etl_profile():
    assert default_etl_profile("local") == "load"
    assert default_etl_profile("clinic") == "full"


def test_iter_status_targets_filter():
    all_targets = iter_status_targets()
    assert any(t.component == "api" and t.stage == "demo" for t in all_targets)
    assert any(t.component == "etl" and t.profile == "load" for t in all_targets)

    local_only = iter_status_targets(env_filter="local")
    assert all(t.stage == "local" for t in local_only)
    assert any(t.component == "dbt" for t in local_only)
    assert not any(t.stage == "clinic" for t in local_only)

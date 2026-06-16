"""Unit tests for deploy_dbt_docs module."""

from unittest.mock import patch

import pytest
import typer

from mdc_cli.deploy_dbt_docs import ensure_dbt_docs_generated, upload_dbt_docs_to_s3


def test_ensure_dbt_docs_uses_existing_index(tmp_path, monkeypatch):
    dbt_dir = tmp_path / "dbt_dental_models"
    target = dbt_dir / "target"
    target.mkdir(parents=True)
    (target / "index.html").write_text("<html></html>", encoding="utf-8")

    monkeypatch.setattr("mdc_cli.deploy_dbt_docs.DBT_DIR", dbt_dir)

    result = ensure_dbt_docs_generated("local", skip_generate=False)
    assert result == target


@patch("mdc_cli.deploy_dbt_docs.run_dbt_command", return_value=0)
def test_ensure_dbt_docs_generates_when_missing(mock_run, tmp_path, monkeypatch):
    dbt_dir = tmp_path / "dbt_dental_models"
    target = dbt_dir / "target"
    target.mkdir(parents=True)

    def fake_generate(stage, args):
        (target / "index.html").write_text("<html></html>", encoding="utf-8")
        return 0

    mock_run.side_effect = fake_generate
    monkeypatch.setattr("mdc_cli.deploy_dbt_docs.DBT_DIR", dbt_dir)

    result = ensure_dbt_docs_generated("local", skip_generate=False)
    assert result == target
    mock_run.assert_called_once_with("local", ["docs", "generate"])


def test_ensure_dbt_docs_skip_generate_missing_index(tmp_path, monkeypatch):
    dbt_dir = tmp_path / "dbt_dental_models"
    (dbt_dir / "target").mkdir(parents=True)
    monkeypatch.setattr("mdc_cli.deploy_dbt_docs.DBT_DIR", dbt_dir)

    with pytest.raises(typer.Exit) as exc:
        ensure_dbt_docs_generated("local", skip_generate=True)
    assert exc.value.exit_code == 1


@patch("mdc_cli.deploy_dbt_docs._run", return_value=0)
def test_upload_dbt_docs_to_s3_sync_commands(mock_run, tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    (target / "index.html").write_text("html", encoding="utf-8")
    (target / "manifest.json").write_text("{}", encoding="utf-8")

    upload_dbt_docs_to_s3(target, "my-bucket")

    assert mock_run.call_count == 2
    first_cmd = mock_run.call_args_list[0].args[0]
    second_cmd = mock_run.call_args_list[1].args[0]
    assert first_cmd[4] == "s3://my-bucket/dbt-docs/"
    assert "--exclude" in first_cmd and "*.json" in first_cmd
    assert second_cmd[4] == "s3://my-bucket/dbt-docs/"
    assert "*.json" in second_cmd

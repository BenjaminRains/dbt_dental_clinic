"""Tests for process_util (Windows executable resolution)."""

from mdc_cli.process_util import resolve_cmd


def test_resolve_cmd_expands_npm_when_on_path():
    npm = resolve_cmd(["npm", "run", "dev"])
    assert len(npm) == 3
    assert npm[1:] == ["run", "dev"]
    # When npm is installed, first element should be a concrete path or npm binary
    if npm[0] != "npm":
        assert "npm" in npm[0].lower()

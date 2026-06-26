"""Merge duplicate model-level YAML keys (tests, meta, config) in schema files."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "models"


def _block_end(lines: list[str], start: int, indent: str) -> int:
    end = start + 1
    while end < len(lines) and (
        lines[end].startswith(indent) or lines[end].strip() == ""
    ):
        end += 1
    return end


def remove_duplicate_tests(lines: list[str]) -> bool:
    idx = [i for i, line in enumerate(lines) if line == "    tests:"]
    if len(idx) < 2:
        return False
    start = idx[1]
    end = _block_end(lines, start, "      ")
    del lines[start:end]
    return True


def merge_duplicate_meta(lines: list[str]) -> bool:
    idx = [i for i, line in enumerate(lines) if line == "    meta:"]
    if len(idx) < 2:
        return False
    first, second = idx[0], idx[1]
    second_end = _block_end(lines, second, "      ")
    second_content = lines[second + 1 : second_end]
    if not second_content:
        del lines[second:second_end]
        return True
    insert_at = _block_end(lines, first, "      ")
    lines[insert_at:insert_at] = second_content
    # second block shifted by len(second_content)
    second += len(second_content)
    second_end += len(second_content)
    del lines[second:second_end]
    return True


def merge_duplicate_config(lines: list[str]) -> bool:
    idx = [i for i, line in enumerate(lines) if line == "    config:"]
    if len(idx) < 2:
        return False
    first, second = idx[0], idx[1]
    second_end = _block_end(lines, second, "      ")
    second_content = lines[second + 1 : second_end]
    insert_at = _block_end(lines, first, "      ")
    lines[insert_at:insert_at] = second_content
    second += len(second_content)
    second_end += len(second_content)
    del lines[second:second_end]
    return True


def main() -> None:
    fixes: list[str] = []
    for path in sorted(ROOT.rglob("*.yml")):
        lines = path.read_text(encoding="utf-8").splitlines()
        changed = False
        if remove_duplicate_tests(lines):
            changed = True
            fixes.append(f"{path}: removed duplicate tests")
        if merge_duplicate_meta(lines):
            changed = True
            fixes.append(f"{path}: merged duplicate meta")
        if merge_duplicate_config(lines):
            changed = True
            fixes.append(f"{path}: merged duplicate config")
        if changed:
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    if fixes:
        print("\n".join(fixes))
    else:
        print("no duplicate keys fixed")


if __name__ == "__main__":
    main()

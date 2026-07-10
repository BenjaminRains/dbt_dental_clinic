"""Migrate stg_opendental SQL {{ config() }} into YAML model config, then strip SQL config."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "models" / "staging" / "opendental"
DRY = "--dry-run" in sys.argv

CONFIG_RE = re.compile(r"\{\{\s*config\((.*?)\)\s*\}\}\s*\n?", re.S)
MAT_RE = re.compile(r"materialized\s*=\s*['\"]([^'\"]+)['\"]")
UK_RE = re.compile(r"unique_key\s*=\s*['\"]([^'\"]+)['\"]")


def parse_sql_config(text: str) -> tuple[str | None, dict]:
    m = CONFIG_RE.search(text)
    if not m:
        return None, {}
    body = m.group(1)
    cfg: dict = {}
    if mat := MAT_RE.search(body):
        cfg["materialized"] = mat.group(1)
    if uk := UK_RE.search(body):
        cfg["unique_key"] = uk.group(1)
    return m.group(0), cfg


def has_materialized(yml: str) -> bool:
    return bool(re.search(r"^    config:\n(?:      .+\n)*?      materialized:", yml, re.M))


def merge_or_insert_yaml_config(yml: str, model: str, cfg: dict) -> str:
    if has_materialized(yml):
        return yml

    # Existing model-level config without materialized (often tags only)
    existing = re.search(r"^    config:\n((?:      .+\n)*)", yml, re.M)
    if existing:
        body = existing.group(1)
        insert_lines = [f"      materialized: {cfg['materialized']}\n"]
        if "unique_key" in cfg and "unique_key:" not in body:
            insert_lines.append(f"      unique_key: {cfg['unique_key']}\n")
        start = existing.start(1)
        return yml[:start] + "".join(insert_lines) + yml[start:]

    lines = ["    config:", f"      materialized: {cfg['materialized']}"]
    if "unique_key" in cfg:
        lines.append(f"      unique_key: {cfg['unique_key']}")
    tag = model.replace("stg_opendental__", "")
    lines.append(f"      tags: ['staging', 'opendental', '{tag}']")
    block = "\n".join(lines) + "\n\n"

    pat = re.compile(
        rf"(  - name: {re.escape(model)}\n"
        r"(?:    description: >\n(?:      .*\n)+)?"
        r"(?:    description: .+\n)?"
        r")(\n?)(    (?:meta|columns|tests|config):)",
        re.M,
    )
    m = pat.search(yml)
    if m:
        return yml[: m.end(1)] + "\n" + block + m.group(3) + yml[m.end(3) :]

    name_line = f"  - name: {model}\n"
    idx = yml.find(name_line)
    if idx >= 0:
        insert_at = idx + len(name_line)
        return yml[:insert_at] + block + yml[insert_at:]
    raise ValueError(f"Could not find model {model} in YAML")


def strip_sql_config(text: str) -> str:
    return CONFIG_RE.sub("", text, count=1)


def main() -> None:
    sql_files = sorted(ROOT.glob("stg_opendental__*.sql"))
    changed_sql = changed_yml = 0
    errors: list[str] = []

    for sql_path in sql_files:
        text = sql_path.read_text(encoding="utf-8")
        matched, cfg = parse_sql_config(text)
        if not matched:
            continue
        if "materialized" not in cfg:
            errors.append(f"{sql_path.name}: no materialized in config")
            continue

        model = sql_path.stem
        yml_path = ROOT / f"_{model}.yml"
        if not yml_path.exists():
            errors.append(f"{sql_path.name}: missing {yml_path.name}")
            continue

        yml = yml_path.read_text(encoding="utf-8")
        if has_materialized(yml):
            action = "yml_ok"
        elif re.search(r"^    config:", yml, re.M):
            action = "yml_merge"
        else:
            action = "yml_insert"

        new_yml = merge_or_insert_yaml_config(yml, model, cfg)
        new_sql = strip_sql_config(text)
        new_sql = re.sub(r"\n{3,}", "\n\n", new_sql)
        if not new_sql.endswith("\n"):
            new_sql += "\n"

        print(f"{model}: {cfg.get('materialized')} uk={cfg.get('unique_key')} {action}")

        if DRY:
            continue

        if new_yml != yml:
            yml_path.write_text(new_yml, encoding="utf-8")
            changed_yml += 1
        if new_sql != text:
            sql_path.write_text(new_sql, encoding="utf-8")
            changed_sql += 1

    print(f"\nchanged_yml={changed_yml} changed_sql={changed_sql}")
    if errors:
        print("ERRORS:")
        for e in errors:
            print(" ", e)
        sys.exit(1)


if __name__ == "__main__":
    main()

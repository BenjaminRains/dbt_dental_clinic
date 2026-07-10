"""Move model-level config blocks to immediately after the model name line."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "models" / "staging" / "opendental"
DRY = "--dry-run" in sys.argv

# Model-level config: 4-space indent, includes materialized
MODEL_CONFIG_RE = re.compile(
    r"^    config:\n(?:      .+\n)+",
    re.M,
)


def extract_model_config(text: str) -> re.Match | None:
    for m in MODEL_CONFIG_RE.finditer(text):
        block = m.group(0)
        if re.search(r"^      materialized:", block, re.M):
            return m
    return None


def move_config_to_top(text: str) -> tuple[str, bool]:
    name_m = re.search(r"^  - name: stg_opendental__\w+\n", text, re.M)
    if not name_m:
        return text, False

    cfg_m = extract_model_config(text)
    if not cfg_m:
        return text, False

    # Already at top if config starts right after name (allow one blank line)
    after_name = text[name_m.end() : name_m.end() + 20]
    if after_name.lstrip("\n").startswith("config:"):
        return text, False
    # Also treat as OK if only blank lines then config within first 30 chars
    gap = text[name_m.end() : cfg_m.start()]
    if gap.strip() == "" and cfg_m.start() - name_m.end() <= 2:
        return text, False

    block = cfg_m.group(0)
    # Ensure block ends with newline
    if not block.endswith("\n"):
        block += "\n"

    without = text[: cfg_m.start()] + text[cfg_m.end() :]
    # Clean triple blank lines left behind
    without = re.sub(r"\n{3,}", "\n\n", without)

    # Re-find name in modified text
    name_m2 = re.search(r"^  - name: stg_opendental__\w+\n", without, re.M)
    if not name_m2:
        raise ValueError("lost model name after removal")

    insert_at = name_m2.end()
    new_text = without[:insert_at] + block + "\n" + without[insert_at:]
    new_text = re.sub(r"\n{3,}", "\n\n", new_text)
    return new_text, True


def main() -> None:
    changed = 0
    for path in sorted(ROOT.glob("_stg_opendental__*.yml")):
        text = path.read_text(encoding="utf-8")
        new_text, did = move_config_to_top(text)
        if not did:
            continue
        print(path.name)
        changed += 1
        if not DRY:
            path.write_text(new_text, encoding="utf-8")
    print(f"moved={changed}")


if __name__ == "__main__":
    main()

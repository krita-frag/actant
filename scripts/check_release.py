"""校验发布版本的一致性。

版本号只有一个权威源：根 ``Cargo.toml`` 的 ``[package] version``。Python 包与
workspace 内其余 crate 必须与之相同；打 tag 发布时 tag 与 CHANGELOG 条目也必须对齐。

用法::

    uv run python scripts/check_release.py
    uv run python scripts/check_release.py --tag v0.4.0 --released
    uv run python scripts/check_release.py --notes
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: 所有必须与根 crate 版本一致的清单。
MANIFESTS: tuple[tuple[str, str], ...] = (
    ("Cargo.toml", "package"),
    ("crates/actant-core/Cargo.toml", "package"),
    ("crates/actant-common/Cargo.toml", "package"),
    ("pyproject.toml", "project"),
)

CHANGELOG = ROOT / "CHANGELOG.md"
_VERSION = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)
_UNRELEASED = re.compile(r"^## \[Unreleased\]", re.MULTILINE)


def _manifest_version(relative: str, table: str) -> str:
    text = (ROOT / relative).read_text(encoding="utf-8")
    section = re.search(rf"^\[{re.escape(table)}\]\s*$", text, re.MULTILINE)
    if section is None:
        raise SystemExit(f"{relative}: 缺少 [{table}]")
    match = _VERSION.search(text, section.end())
    if match is None:
        raise SystemExit(f"{relative}: [{table}] 缺少 version")
    return match.group(1)


def _release_section(version: str) -> str:
    text = CHANGELOG.read_text(encoding="utf-8")
    begin = re.search(rf"^## \[{re.escape(version)}\].*$", text, re.MULTILINE)
    if begin is None:
        raise SystemExit(f"CHANGELOG.md: 缺少 ## [{version}] 条目")
    rest = text[begin.end() :]
    end = re.search(r"^## \[", rest, re.MULTILINE)
    return (rest[: end.start()] if end else rest).strip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="发布 tag（vX.Y.Z）；校验其与清单版本一致")
    parser.add_argument("--released", action="store_true", help="要求 CHANGELOG 已有该版本条目")
    parser.add_argument("--notes", action="store_true", help="输出该版本的 CHANGELOG 段落")
    args = parser.parse_args()

    versions = {relative: _manifest_version(relative, table) for relative, table in MANIFESTS}
    unique = set(versions.values())
    if len(unique) != 1:
        detail = "\n".join(f"  {path}: {version}" for path, version in versions.items())
        print(f"版本不一致：\n{detail}", file=sys.stderr)
        return 1

    version = unique.pop()
    changelog = CHANGELOG.read_text(encoding="utf-8")
    if _UNRELEASED.search(changelog) is None:
        print("CHANGELOG.md: 缺少 ## [Unreleased] 小节", file=sys.stderr)
        return 1
    if args.tag is not None:
        tag = args.tag[1:] if args.tag.startswith("v") else args.tag
        if tag != version:
            print(f"tag {args.tag} 与清单版本 {version} 不一致", file=sys.stderr)
            return 1
    released = re.search(rf"^## \[{re.escape(version)}\]", changelog, re.MULTILINE)
    if args.released and released is None:
        print(f"CHANGELOG.md: 发布前需把 Unreleased 拆出 ## [{version}]", file=sys.stderr)
        return 1
    if args.notes:
        sys.stdout.write(_release_section(version))
        return 0

    print(f"OK: 版本 {version} 在 {len(versions)} 个清单中一致")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

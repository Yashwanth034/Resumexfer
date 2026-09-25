#!/usr/bin/env python3
"""Fail CI if public source contains private/local-only material or common secrets."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {
    ".git",
    ".rt_hw",
    ".rt_crosscheck",
    ".repotunnel-tmp",
    "dist",
    "__pycache__",
    ".pytest_cache",
}
TEXT_SUFFIXES = {
    "",
    ".c",
    ".go",
    ".html",
    ".js",
    ".json",
    ".md",
    ".py",
    ".service",
    ".sh",
    ".txt",
    ".yml",
    ".yaml",
}
FORBIDDEN_TRACKED = (
    ".rt_hw/",
    ".rt_crosscheck/",
    ".repotunnel-tmp/",
    "CHECKPOINT_HANDOFF.txt",
    "HANDOFF.md",
    "docs/superpowers/",
)
SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "GitHub token": re.compile(r"\b(?:ghp|gho|ghu|ghs|github_pat)_[A-Za-z0-9_]{20,}\b"),
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "Slack token": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    "generic credential assignment": re.compile(
        r"""(?ix)
        \b(?:api[_-]?key|client[_-]?secret|access[_-]?token|refresh[_-]?token|password)
        \b\s*[:=]\s*["'][^"'\n]{8,}["']
        """
    ),
}
EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
UNIX_HOME = re.compile(r"/(?:home|Users)/([^/:\s\"']+)")
WINDOWS_HOME = re.compile(r"(?i)\b[A-Z]:\\Users\\([^\\\s\"']+)")
SAFE_HOME_NAMES = {"me", "user", "test", "runner", "example", "alice", "bob"}


def public_files() -> tuple[list[Path], bool]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        paths = [
            ROOT / item.decode("utf-8")
            for item in result.stdout.split(b"\0")
            if item
        ]
        return paths, True
    except (OSError, subprocess.CalledProcessError):
        paths: list[Path] = []
        for path in ROOT.rglob("*"):
            if not path.is_file() or any(part in SKIP_DIRS for part in path.parts):
                continue
            paths.append(path)
        return paths, False


def relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def main() -> int:
    files, from_git = public_files()
    issues: list[str] = []

    if from_git:
        for path in files:
            rel = relative(path)
            if rel.endswith(".deb"):
                issues.append(f"{rel}: generated Debian package must not be tracked")
            if any(rel == blocked.rstrip("/") or rel.startswith(blocked) for blocked in FORBIDDEN_TRACKED):
                issues.append(f"{rel}: private/development-only path must not be tracked")

    for path in files:
        rel = relative(path)
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        try:
            if path.stat().st_size > 2 * 1024 * 1024:
                continue
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue

        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(text):
                issues.append(f"{rel}: possible {label}")

        for match in EMAIL.finditer(text):
            email = match.group(0)
            if email.endswith("@users.noreply.github.com") or email.endswith("@example.com"):
                continue
            issues.append(f"{rel}: email address should not be committed ({email})")

        for pattern in (UNIX_HOME, WINDOWS_HOME):
            for match in pattern.finditer(text):
                name = match.group(1).lower()
                if name not in SAFE_HOME_NAMES:
                    issues.append(f"{rel}: personal home-directory path detected")
                    break

    if issues:
        print("PUBLIC SAFETY CHECK: FAIL", file=sys.stderr)
        for issue in sorted(set(issues)):
            print(f"- {issue}", file=sys.stderr)
        return 1

    source = "tracked files" if from_git else "workspace files (Git metadata unavailable)"
    print(f"PUBLIC SAFETY CHECK: PASS — scanned {source}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

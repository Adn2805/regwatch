"""Pre-push scan of reachable/reflog history. Never print matched private values."""

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EMAIL = re.compile(rb"[A-Za-z0-9._%+-]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})")
TOKEN_PATTERNS = [
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    re.compile(rb"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    re.compile(rb"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(rb"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(rb"\bxox[baprs]-[A-Za-z0-9-]{12,}\b"),
]


def git(*arguments: str) -> bytes:
    return subprocess.run(["git", *arguments], cwd=ROOT, check=True, capture_output=True).stdout


def placeholder_domain(domain: str) -> bool:
    return domain in {"example.com", "example.org", "example.net", "localhost"} or domain.endswith(
        (".invalid", ".test", ".example", ".localhost", ".local")
    )


def private_values() -> list[bytes]:
    path = ROOT / ".env"
    if not path.exists():
        return []
    values: list[bytes] = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip("\"'")
        if not re.search(r"PASSWORD|SECRET|TOKEN|API_KEY|CONTACT_EMAIL", key, re.I):
            continue
        if not value or value.startswith("replace-"):
            continue
        if "@" in value and placeholder_domain(value.rsplit("@", 1)[-1].lower()):
            continue
        values.append(value.encode())
    return values


def main() -> int:
    violations: list[dict[str, str]] = []
    ignored = {}
    for path in [".env", "backend/tests/fixtures/local_only/probe.html"]:
        result = subprocess.run(
            ["git", "check-ignore", "--quiet", path], cwd=ROOT, capture_output=True
        )
        ignored[path] = result.returncode == 0
        if result.returncode != 0:
            violations.append({"kind": "not_gitignored", "path": path})
    commits = git("rev-list", "--all", "--reflog").decode().splitlines()
    blobs: dict[str, str] = {}
    for commit in commits:
        for entry in git("ls-tree", "-rz", commit).split(b"\0"):
            if not entry:
                continue
            metadata, raw_path = entry.split(b"\t", 1)
            mode, kind, identity = metadata.decode().split()
            path = raw_path.decode()
            if kind != "blob":
                continue
            name = path.rsplit("/", 1)[-1]
            if name == ".env" or (name.startswith(".env.") and name != ".env.example"):
                violations.append({"kind": "env_in_history", "path": path})
            if "/local_only/" in "/" + path:
                violations.append({"kind": "private_capture_in_history", "path": path})
            blobs.setdefault(identity, path)
    values = private_values()
    for identity, path in blobs.items():
        content = git("cat-file", "blob", identity)
        if any(value in content for value in values):
            violations.append({"kind": "known_private_value", "path": path})
        if any(pattern.search(content) for pattern in TOKEN_PATTERNS):
            violations.append({"kind": "token_or_private_key", "path": path})
        for match in EMAIL.finditer(content):
            address = match.group(0)
            domain = match.group(1).decode().lower()
            if placeholder_domain(domain):
                continue
            # Existing URL-validation tests use literal user:password placeholders.
            prefix = content[max(0, match.start() - 10) : match.start()]
            if address.startswith(b"password@") and prefix.endswith(b"://user:"):
                continue
            violations.append({"kind": "non_placeholder_email", "path": path})
    for commit in commits:
        metadata = git("show", "-s", "--format=%B%n%an%n%ae%n%cn%n%ce", commit)
        if any(value in metadata for value in values):
            violations.append(
                {"kind": "known_private_value_in_commit_metadata", "path": "commit metadata"}
            )
        for match in EMAIL.finditer(metadata):
            if not placeholder_domain(match.group(1).decode().lower()):
                violations.append(
                    {"kind": "non_placeholder_email_in_commit_metadata", "path": "commit metadata"}
                )
    unique = [dict(item) for item in sorted({tuple(sorted(item.items())) for item in violations})]
    print(
        json.dumps(
            {
                "status": "pass" if not unique else "fail",
                "commits_scanned": len(commits),
                "unique_blobs_scanned": len(blobs),
                "ignored": ignored,
                "violations": unique,
                "matched_values_printed": False,
            },
            indent=2,
        )
    )
    return 1 if unique else 0


if __name__ == "__main__":
    raise SystemExit(main())

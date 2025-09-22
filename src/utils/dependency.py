from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version
import re
import shlex
import subprocess
from typing import TypedDict, Tuple, Optional


class ProbeEntry(TypedDict):
    name: str
    probe: str
    version_regex: str


def version_satisfies(version: str, requirement: str) -> bool:
    """Return True if ``version`` satisfies the requirement string."""
    requirement = requirement.strip()
    if not requirement:
        return True

    try:
        spec = SpecifierSet(requirement)
    except InvalidSpecifier as exc:
        raise ValueError(f"Invalid version requirement: {requirement}") from exc

    try:
        parsed_version = Version(version)
    except InvalidVersion as exc:
        raise ValueError(f"Invalid version detected: {version}") from exc

    return parsed_version in spec


def run_local_probe(command: str) -> Tuple[bool, str]:
    try:
        completed = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        return False, str(exc)

    output = completed.stdout.strip() or completed.stderr.strip()
    return completed.returncode == 0, output


def run_remote_probe(command: str, host: str, user: str, key_path: str) -> Tuple[bool, str]:
    remote_command = f"bash -lc {shlex.quote(command)}"
    ssh_cmd = [
        "ssh",
        "-i",
        str(key_path),
        f"{user}@{host}",
        remote_command,
    ]

    try:
        completed = subprocess.run(ssh_cmd, capture_output=True, text=True)
    except Exception as exc:
        return False, str(exc)

    output = completed.stdout.strip() or completed.stderr.strip()
    return completed.returncode == 0, output


def extract_version(output: str, regex: str) -> Optional[str]:
    """Extract a version string from output using regex."""
    if not output:
        return None

    if regex:
        match = re.search(regex, output)
        if match:
            if "version" in match.groupdict():
                return match.group("version").strip()
            if match.groups():
                return match.group(match.lastindex or 1).strip()

    cleaned_output = output.strip()
    return cleaned_output or None


def version_satisfies(version: str, requirement: str) -> bool:
    """Return True if ``version`` satisfies the requirement string."""
    requirement = requirement.strip()
    if not requirement:
        return True

    try:
        spec = SpecifierSet(requirement)
    except InvalidSpecifier as exc:
        raise ValueError(f"Invalid version requirement: {requirement}") from exc

    try:
        parsed_version = Version(version)
    except InvalidVersion as exc:
        raise ValueError(f"Invalid version detected: {version}") from exc

    return parsed_version in spec

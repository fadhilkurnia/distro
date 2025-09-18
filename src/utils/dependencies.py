import json
import re
from pathlib import Path
from typing import Any, Mapping, Optional
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

PROBE_CONFIG_PATH = Path(__file__).with_name("dependency_probes.json")

def load_dependency_probes() -> dict[str, dict[str, str]]:
    """Load probe metadata keyed by dependency name."""
    data = json.loads(PROBE_CONFIG_PATH.read_text(encoding="utf-8"))
    result: dict[str, dict[str, str]] = {}
    for name, entry in data.items():
        if isinstance(entry, str):
            command = entry
            version_regex = ""
        else:
            command = entry.get("probe", "")
            version_regex = entry.get("version_regex", "")

        result[name.strip()] = {
            "probe": command.strip(),
            "version_regex": version_regex.strip(),
        }
    return result

PROBE_METADATA = load_dependency_probes()

def dependency_probes() -> dict[str, str]:
    """Return all known dependency probe commands."""
    return {name: meta["probe"] for name, meta in PROBE_METADATA.items()}

def probe_command_for(name: str) -> Optional[str]:
    """Find a probe command for the given dependency name."""
    cleaned = name.strip()
    if not cleaned:
        return None
    meta = PROBE_METADATA.get(cleaned)
    return meta["probe"] if meta else None

def version_regex_for(name: str) -> Optional[str]:
    """Return the regex used to extract a version from probe output."""
    cleaned = name.strip()
    if not cleaned:
        return None
    meta = PROBE_METADATA.get(cleaned)
    regex = meta.get("version_regex") if meta else None
    return regex or None

def extract_version(name: str, probe_output: str) -> Optional[str]:
    """Extract a version string from probe output using the configured regex."""
    if not probe_output:
        return None

    regex = version_regex_for(name)
    if regex:
        match = re.search(regex, probe_output)
        if match:
            if "version" in match.groupdict():
                return match.group("version").strip()
            if match.groups():
                return match.group(match.lastindex or 1).strip()

    cleaned_output = probe_output.strip()
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


def version_requirement_satisfied(
    name: str,
    requirement: Optional[str],
    probe_output: str,
) -> bool:
    """Check if probe output meets a dependency's version requirement."""
    if not requirement:
        return True

    detected_version = extract_version(name, probe_output)
    if not detected_version:
        return False

    return version_satisfies(detected_version, requirement)


def normalize_dependency_spec(spec: str | Mapping[str, Any]) -> tuple[str, Optional[str]]:
    """Turn a dependency spec into a (name, version) pair."""
    if isinstance(spec, str):
        cleaned = spec.strip()
        if not cleaned:
            raise ValueError("Dependency name cannot be empty")
        return cleaned, None

    if isinstance(spec, Mapping):
        raw_name = spec.get("name", "")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise ValueError("Dependency mapping must include a name")

        version_value = spec.get("version")
        if version_value is None:
            return raw_name.strip(), None
        if isinstance(version_value, str) and version_value.strip():
            return raw_name.strip(), version_value.strip()
        raise ValueError("Dependency version must be a non-empty string")

    raise TypeError("Dependency spec must be a string or mapping")

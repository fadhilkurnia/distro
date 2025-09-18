import json
from pathlib import Path
from typing import Any, Mapping

PROBE_CONFIG_PATH = Path(__file__).with_name("dependency_probes.json")

def load_dependency_probes() -> dict[str, str]:
    """Load probe commands keyed by dependency name."""
    data = json.loads(PROBE_CONFIG_PATH.read_text(encoding="utf-8"))
    result: dict[str, str] = {}
    for name, entry in data.items():
        if isinstance(entry, str):
            command = entry
        else:
            command = entry.get("probe", "")
        result[name.strip()] = command.strip()
    return result

PROBE_COMMANDS = load_dependency_probes()

def dependency_probes() -> dict[str, str]:
    """Return all known dependency probe commands."""
    return dict(PROBE_COMMANDS)

def probe_command_for(name: str) -> str | None:
    """Find a probe command for the given dependency name."""
    cleaned = name.strip()
    if not cleaned:
        return None
    return PROBE_COMMANDS.get(cleaned)

def normalize_dependency_spec(spec: str | Mapping[str, Any]) -> tuple[str, str | None]:
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

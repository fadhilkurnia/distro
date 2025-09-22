from typing import TypedDict
from src.utils.dependency import ProbeEntry


class ProbeConfig(TypedDict):
    """
    Static schema for the dependency probes.
    """
    maven: ProbeEntry
    golang: ProbeEntry


# Add probes here and in ProbeConfig class schema.
# Will allow autocomplete similar to structs.
DEPENDENCIES: ProbeConfig = {
    "maven": {
        "name": "Apache Maven",
        "probe": "mvn -v",
        "version_regex": "Apache Maven (?P<version>[0-9.]+)"
    },
    "golang": {
        "name": "Go",
        "probe": "go version",
        "version_regex": "go version go(?P<version>[0-9.]+)"
    }
}

from typing import TypedDict
from src.utils.dependency import ProbeEntry


class ProbeConfig(TypedDict):
    """
    Static schema for the dependency probes.
    """
    maven: ProbeEntry
    golang: ProbeEntry
    java: ProbeEntry
    docker: ProbeEntry
    fuse: ProbeEntry
    rust: ProbeEntry
    gcc: ProbeEntry
    cmake: ProbeEntry


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
    },
    "java": {
        "name": "Java",
        "probe": "java -version",
        "version_regex": "version \"(?P<version>[0-9.]+)\""
    },
    "docker": {
        "name": "Docker",
        "probe": "docker --version",
        "version_regex": "Docker version (?P<version>[0-9.]+)"
    },
    "fuse": {
        "name": "libfuse3",
        "probe": "fusermount3 --version",
        "version_regex": "fusermount3 version: (?P<version>[0-9.]+)"
    },
    "rust": {
        "name": "rust",
        "probe": "rustc --version",
        "version_regex": "rustc (?P<version>[0-9.]+)"
    },
    "cargo": {
        "name": "cargo",
        "probe": "cargo --version",
        "version_regex": "cargo (?P<version>[0-9.]+)"
    },
    "gcc": {
        "name": "gcc",
        "probe": "gcc -v",
        "version_regex": "gcc version (?P<version>[0-9.]+)"
    },
    "cmake": {
        "name": "cmake",
        "probe": "cmake --version",
        "version_regex": "cmake version (?P<version>[0-9.]+)"
    },
}

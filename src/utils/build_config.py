import re
from typing import Dict, List, Any, Optional


BUILD_CONFIG_SCHEMA = {
    "source": str,
    "commit_hash": str,  # Optional - if not provided, uses the latest commit
    "build_commands": list, 
    "dependencies": list,  # Optional for now
    "remote_workdir": str # Optinal - defaults to "/home/ubuntu" if not specified
}


def validate_build_config(config: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate a BUILD_CONFIG dictionary against the required schema.

    :param config: The build configuration to validate
    :return: Tuple of (is_valid, error_message)
    """
    # Check required fields
    required_fields = {"source", "build_commands"}
    missing_fields = required_fields - set(config.keys())
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"

    # Validate source URL format
    if not isinstance(config["source"], str) or not config["source"].strip():
        return False, "source must be a non-empty string"

    # Validate optional commit hash (40 character hex string)
    if "commit_hash" in config:
        commit_hash = config["commit_hash"]
        if commit_hash is not None and commit_hash.strip():  # Only validate if provided and not empty
            if not isinstance(commit_hash, str) or not re.match(r'^[a-f0-9]{40}$', commit_hash):
                return False, "commit_hash must be a 40-character hexadecimal string"

    # Validate build_commands
    if not isinstance(config["build_commands"], list) or len(config["build_commands"]) == 0:
        return False, "build_commands must be a non-empty list"

    for i, cmd in enumerate(config["build_commands"]):
        if not isinstance(cmd, str) or not cmd.strip():
            return False, f"build_commands[{i}] must be a non-empty string"

    # Validate optional remote_workdir
    if "remote_workdir" in config:
        if config["remote_workdir"] is not None:  # Only validate if provided and not None
            if not isinstance(config["remote_workdir"], str) or not config["remote_workdir"].strip():
                return False, "remote_workdir must be a non-empty string"

    # Validate optional dependencies field
    if "dependencies" in config:
        if not isinstance(config["dependencies"], list):
            return False, "dependencies must be a list"
        for i, dep in enumerate(config["dependencies"]):
            if not isinstance(dep, str) or not dep.strip():
                return False, f"dependencies[{i}] must be a non-empty string"

    return True, None


def get_example_config() -> Dict[str, Any]:
    """
    Return an example BUILD_CONFIG for documentation purposes.
    Shows minimal required fields - commit_hash and remote_workdir are optional.
    """
    return {
        "source": "https://github.com/otoolep/hraftd.git",
        # "commit_hash": "2487de6872a16657063dfc017aaf28b38f1b8a65",  # Optional - uses latest if not specified
        "build_commands": [
            "go install",
            "go build"
        ],
        "dependencies": ["golang"],
        # "remote_workdir": "/home/ubuntu/"  # Optional - defaults to "/home/ubuntu" if not specified
    }
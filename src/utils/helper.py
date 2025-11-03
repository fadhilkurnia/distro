import logging
import json
import os
import sys
import subprocess
from pathlib import Path


def get_positive_num(prompt, default_number) -> int:
    while True:
        try:
            user_input = input(f"\n{prompt} (default = {default_number}): ").strip()

            if not user_input:
                print(f"Using default value of {default_number}")
                return default_number

            num = int(user_input)
            if num > 0:
                return num
            else:
                print("Input must be a positive number (> 0).")
                continue

        except KeyboardInterrupt:
            print("\nExiting program...")
            sys.exit()
        except ValueError:
            pass


def get_option(min, max, opts, header="\nOptions:") -> int:
    """
    Print opts to stdout then gets user number input.

    :param min: Smallest index for the option
    :type min: int
    :param max: Largest index for the option
    :type max: int
    :param opts: List of options
    :type opts: { num: int, text: str }
    """
    while True:
        print(header)
        for opt in opts:
            print(f"{opt['num']} - {opt['text']}")

        try:
            num = int(input("Select: ").strip())

            if min <= num <= max:
                return num

        except KeyboardInterrupt:
            print("\nExiting program...")
            sys.exit()
        except ValueError:
            pass


def get_commit(path):
    cmd = f"cd {path} && git rev-parse HEAD"
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"{path} is not a git repository")

    return result.stdout.strip()


def write_to_json(file, data, project_name, protocol_name, workload, commit):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)

    output = (
        f"{workload["type"]} {workload["name"]} benchmark for "
        f"{project_name}:{protocol_name} ({commit}) "
        f"has been added to {file}."
    )
    print(output)


def validate_nodes(nodes):
    if not isinstance(nodes, list):
        raise TypeError("nodes must be of type list.")

    if len(nodes) == 0:
        raise ValueError("nodes is empty.")

    for item in nodes:
        if not isinstance(item, dict):
            raise TypeError("Each item in nodes must be of type dict.")

        if not ("public_ip" in item and "private_ip" in item):
            raise KeyError(
                "Each dictionary in nodes must contain 'public_ip' and 'private_ip' keys.")

        if not isinstance(item['public_ip'], str):
            raise TypeError(f"node 'public_ip' must be a string, got {type(item['public_ip']).__name__}.")

        if not isinstance(item['private_ip'], str):
            raise TypeError(f"node 'private_ip' must be a string, got {type(item['private_ip']).__name__}.")


def validate_ssh(ssh):
    if not isinstance(ssh, dict):
        raise TypeError("ssh must be of type dict.")

    required_keys = ['key', 'username', 'filename']
    for k in required_keys:
        if k not in ssh:
            raise KeyError(f"ssh missing required key: '{k}'")

    if not isinstance(ssh['key'], Path):
        raise TypeError(f"ssh key 'key' must be of type pathlib.Path, got {type(ssh['key']).__name__} instead.")

    if not isinstance(ssh['username'], str):
        raise TypeError(f"ssh key 'username' must be of type str, got {type(ssh['username']).__name__} instead.")

    if not isinstance(ssh['filename'], str):
        raise TypeError(f"ssh key 'filename' must be of type str, got {type(ssh['filename']).__name__} instead.")


def check_repo_exists(dir_path, github_url):
    if not os.path.isdir(dir_path):
        return False

    try:
        result = subprocess.run(
            "git config --get remote.origin.url",
            cwd=dir_path,
            check=True,
            shell=True,
            capture_output=True,
            text=True
        )

        remote_url = result.stdout.strip()

        if github_url.endswith('.git'):
            github_url = github_url[:-4]
        if remote_url.endswith('.git'):
            remote_url = remote_url[:-4]

        return github_url == remote_url

    except subprocess.CalledProcessError:
        return False


def get_repo_path_in_directory(root_dir, repo_url):
    if not os.path.isdir(root_dir):
        return False

    for item in os.listdir(root_dir):
        item_path = os.path.join(root_dir, item)

        if os.path.isdir(item_path):
            if check_repo_exists(item_path, repo_url):
                logging.debug(f"Found repository '{repo_url}' in '{item_path}'")
                return item_path

    logging.debug(f"Repository '{repo_url}' doesn't exist in '{item_path}'")
    return False


def check_subdir_exists(parent_dir: str, subdir_name: str) -> bool:
    """
    Checks if a subdirectory exists within a parent directory.

    Parameters
    ----------
    parent_dir : str
        The path to the parent directory.
    subdir_name : str
        The name of the subdirectory to check for.

    Returns
    -------
    bool
        True if the subdirectory exists, False otherwise.
    """
    subdir_path = Path(parent_dir) / subdir_name
    return subdir_path.is_dir()

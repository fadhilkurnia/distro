import subprocess
import shlex
from typing import Dict, List, Any, Callable, Tuple, Optional

from .build_config import validate_build_config
from .dependencies import normalize_dependency_spec, probe_command_for


class BuildResult:
    def __init__(self, node_id: str, success: bool, message: str, output: str = ""):
        self.node_id = node_id
        self.success = success
        self.message = message
        self.output = output

    def __repr__(self):
        status = "SUCCESS" if self.success else "FAILURE"
        return f"BuildResult({self.node_id}: {status} - {self.message})"


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


def ensure_dependencies(node_id: str, dependencies: List[Any], probe_runner: Callable[[str], Tuple[bool, str]]) -> Optional[BuildResult]:
    for dep in dependencies:
        name, version = normalize_dependency_spec(dep)
        command = probe_command_for(name)
        if not command:
            return BuildResult(node_id, False, f"Unknown dependency '{name}' in configuration")

        success, output = probe_runner(command)
        if success:
            print(f"[{node_id}] dependency '{name}' satisfied via '{command}'")
            if output:
                for line in output.splitlines():
                    print(f"[{node_id}]   probe output: {line}")
            continue

        version_note = f" (requested {version})" if version else ""
        detail = f"Missing dependency '{name}'{version_note}; probe '{command}' failed"
        if output:
            detail = f"{detail}. Output: {output}"
        return BuildResult(node_id, False, detail)

    return None


def build_on_nodes(build_config: Dict[str, Any], nodes: List[Dict[str, str]], ssh: Dict[str, Any], sut_dir: str = None) -> List[BuildResult]:
    """
    Execute build process on all protocol nodes using the provided build configuration.

    :param build_config: Build configuration dictionary
    :param nodes: List of node dictionaries with public/private IPs
    :param ssh: SSH configuration with key, username, filename
    :param sut_dir: Optional SUT directory path (e.g., "sut/otoolep.hraftd") for local builds
    :return: List of BuildResult objects, one per node
    """
    is_valid, error_msg = validate_build_config(build_config)

    if not is_valid:
        return [BuildResult(f"node{i+1}", False, f"Invalid build config: {error_msg}")
                for i in range(len(nodes))]

    results = []

    local = False

    for i, node in enumerate(nodes):
        if node["public"] == "127.0.0.1":
            local = True
            break
    
    if local:
        results.append(execute_local_build("local", build_config, sut_dir)) 
    else:
        for i, node in enumerate(nodes):
            node_id = f"node{i+1}"
            result = execute_remote_build(node_id, node, build_config, ssh)
            results.append(result)

    return results

def execute_local_build(node_id: str, build_config: Dict[str, Any], sut_dir: str = None) -> BuildResult:
    """
    Execute build process locally (for testing with localhost).
    """
    dependencies = build_config.get("dependencies") or []
    dep_failure = ensure_dependencies(node_id, dependencies, run_local_probe)
    if dep_failure:
        return dep_failure

    try:
        # e.g., sut_dir = "sut/otoolep.hraftd"
        workdir = f"./{sut_dir.strip('/')}"
        print(f"[{node_id}] Executing local build in {workdir}")

        # Extract repository name from URL for clone directory
        repo_name = build_config["source"].split("/")[-1].replace(".git", "")

        # remove existing clone if it exists
        subprocess.run(["rm", "-rf", f"{workdir}/{repo_name}"], check=True)

        # Change to workdir and clone (creates repo subdirectory)
        subprocess.run(["git", "clone", build_config["source"]], check=True, cwd=workdir)

        # Git checkout commit (only if commit_hash is provided)
        # e.g., repo_path = "./sut/otoolep.hraftd/hraftd"
        repo_path = f"{workdir}/{repo_name}"
        commit_hash = build_config.get("commit_hash")
        if commit_hash and commit_hash.strip():
            subprocess.run(["git", "checkout", commit_hash], check=True, cwd=repo_path)
        
        subprocess.run(["cd", repo_path], shell=True, check=True)

        # Execute build commands in the repo directory
        for cmd in build_config["build_commands"]:
            subprocess.run(cmd, shell=True, check=True, cwd=repo_path)

        return BuildResult(node_id, True, "Local build completed successfully")

    except subprocess.CalledProcessError as e:
        return BuildResult(node_id, False, f"Local build failed: {e}")


def execute_remote_build(node_id: str, node: Dict[str, str], build_config: Dict[str, Any], ssh: Dict[str, Any]) -> BuildResult:
    """
    Execute build process on a remote node via SSH.
    """
    host = node["public"]
    user = ssh["username"]
    key_path = ssh["key"]

    dependencies = build_config.get("dependencies") or []
    dep_failure = ensure_dependencies(
        node_id,
        dependencies,
        lambda command: run_remote_probe(command, host, user, key_path),
    )
    if dep_failure:
        return dep_failure

    try:
        workdir = build_config.get("remote_workdir") or f"/home/{user}"

        print(f"[{node_id}] Starting remote build on {host} in {workdir!r}")
        print(f"[{node_id}] Workdir quoted: {shlex.quote(workdir)}")

        # Extract repository name from URL for clone directory
        repo_name = build_config["source"].split("/")[-1].replace(".git", "")

        # Build the complete command sequence
        build_sequence = [
            f"mkdir -p {shlex.quote(workdir)}",
            f"cd {shlex.quote(workdir)}",
            f"rm -rf {shlex.quote(repo_name)}",
            f"git clone {shlex.quote(build_config['source'])}",
            f"cd {shlex.quote(repo_name)}"
        ]

        # Add git checkout only if commit_hash is provided
        commit_hash = build_config.get("commit_hash")
        if commit_hash and commit_hash.strip():
            build_sequence.append(f"git checkout {shlex.quote(commit_hash)}")

        # Add build commands
        for cmd in build_config["build_commands"]:
            build_sequence.append(cmd)

        # Combine all commands with && for proper error handling
        combined_cmd = " && ".join(build_sequence)
        print(f"[{node_id}] Combined command: {combined_cmd}")

        # Execute via SSH. Wraped the command string.
        remote_command = f"bash -lc {shlex.quote(combined_cmd)}"

        print(f"[{node_id}] Remote command: {remote_command}")
        ssh_cmd = [
            "ssh", "-i", str(key_path), f"{user}@{host}",
            remote_command
        ]

        print(f"[{node_id}] Executing build sequence on {host}")
        result = subprocess.run(ssh_cmd, capture_output=True, text=True)

        if result.returncode == 0:
            return BuildResult(node_id, True, f"Remote build completed successfully", result.stdout)
        else:
            return BuildResult(node_id, False, f"Remote build failed (exit {result.returncode})", result.stderr)

    except Exception as e:
        return BuildResult(node_id, False, f"Remote build error: {str(e)}")


def print_build_results(results: List[BuildResult]) -> None:
    """
    Print build results in a formatted way.
    """
    print("\n=== Build Results ===")
    success_count = sum(1 for r in results if r.success)
    total_count = len(results)

    for result in results:
        status = "success" if result.success else "error"
        print(f"{status} {result.node_id}: {result.message}")

        if not result.success and result.output:
            print(f"    Error output: {result.output[:200]}...")

    print(f"\nSummary: {success_count}/{total_count} nodes built successfully")

    return success_count == total_count

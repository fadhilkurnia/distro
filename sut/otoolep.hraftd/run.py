from pathlib import Path
import subprocess
import threading
import time
import os
import signal

from src.utils import helper
from src.utils.builder import build_on_nodes, print_build_results

CURR_DIR = Path("./sut/otoolep.hraftd")
HRAFTD_BIN = CURR_DIR / "hraftd" / "hraftd"
HRAFTD_GIT = "https://github.com/otoolep/hraftd.git"

# Build configuration for hraftd
BUILD_CONFIG = {
    "source": "https://github.com/otoolep/hraftd.git",
    # No commit_hash specified - will use latest commit as a default
    "build_commands": [
        "go install",
        "go build"
    ],
    "dependencies": [
        {"name": "golang", "version": ">=1.20"}
    ]
    # remote_workdir not specified - will use default "/home/ubuntu"
}

OPTIONS = [{"num": 0, "text": "Build hraftd on all nodes"},
           {"num": 1, "text": "Start hraftd cluster"},
           {"num": 2, "text": "Stop hraftd cluster"},
           {"num": 3, "text": "Run Benchmark"}]


def main(run_ycsb, nodes, ssh):
    node_data = map_ip_port(nodes)
    print("hraftd IP-Port Data:")
    for item in node_data:
        print(item)

    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                build_hraftd_cluster(nodes, ssh)
            case 1:
                start_hraftd_cluster(node_data, ssh)
            case 2:
                stop_hraftd_cluster(node_data, ssh)
            case 3:
                endpoints = [f"http://{node['client_ip']}:{node['client_port']}" for node in node_data]
                print("endpoint list:", endpoints)
                run_ycsb({
                    "name": "raft",
                    "language": "Go",
                    "consistency": "Linearizability",
                    "persistency": "In-Memory"
                }, "hraftd", endpoints, "hraftd.hosts", ssh)


def build_hraftd_cluster(nodes, ssh):
    """
    Build hraftd on all protocol nodes using the build framework.
    """
    print("Building hraftd on all nodes...")

    script_path = Path(__file__)
    sut_dir = script_path.parent
    project_root = Path.cwd()
    sut_relative_path = sut_dir.relative_to(project_root)

    print(f"Using SUT directory: {sut_relative_path}")

    results = build_on_nodes(BUILD_CONFIG, nodes, ssh, str(sut_relative_path))
    success = print_build_results(results)
    
    if not success:
        print("Build failed on one or more nodes. Check the error output above.")
        return False

    print("hraftd built successfully on all nodes.")
    return True


def start_hraftd_cluster(nodes, ssh):
    """
    Start hraftd cluster processes on all nodes.
    Note: Build must be completed before calling this function.
    """
    user = ssh["username"]
    join = None

    for i, node in enumerate(nodes):
        if node["client_ip"] == "127.0.0.1" and node["peer_ip"] == "127.0.0.1":
            # Local execution
            haddr = f"{node['peer_ip']}:{node['client_port']}"
            raddr = f"{node['peer_ip']}:{node['peer_port']}"
            local_dir = CURR_DIR / f"node{i+1}"

            join_part = "" if join is None else join
            run_cmd = (
                f"nohup {HRAFTD_BIN.resolve()} -id node{i+1} -haddr {haddr} "
                f"-raddr {raddr} {join_part} {local_dir} > /dev/null 2>&1 &"
            )
        else:
            # Remote execution
            host = node["client_ip"]
            haddr = f"{node['peer_ip']}:{node['client_port']}"
            raddr = f"{node['peer_ip']}:{node['peer_port']}"
            remote_workdir = BUILD_CONFIG.get("remote_workdir", f"/home/{user}")
            repo_name = BUILD_CONFIG["source"].split("/")[-1].replace(".git", "")
            remote_repo_dir = f"{remote_workdir.rstrip('/')}/{repo_name}"
            remote_hraftd = f"{remote_repo_dir}/hraftd"
            remote_dir = f"{remote_repo_dir}/node{i+1}"

            join_part = "" if join is None else join
            run_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'nohup {remote_hraftd} -id node{i+1} -haddr {haddr} "
                f"-raddr {raddr} {join_part} {remote_dir} > /dev/null 2>&1 &'"
            )

        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)

        if join is None:
            join = f"-join {node['peer_ip']}:{node['client_port']}"

    print("hraftd cluster successfully started")


def stop_hraftd_cluster(nodes, ssh):
    user = ssh["username"]

    for i, node in enumerate(nodes):
        if node["client_ip"] == "127.0.0.1" and node["peer_ip"] == "127.0.0.1":
            local_dir = CURR_DIR / f"node{i+1}"
            cmd = (
                f"pids=$(ps aux | grep '{HRAFTD_BIN}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
            )

            print("Running command:", cmd)
            subprocess.run(cmd, shell=True)
            os.system(f"rm -rf {local_dir.resolve()}")
        else:
            host = node["client_ip"]
            remote_workdir = BUILD_CONFIG.get("remote_workdir", f"/home/{user}")
            repo_name = BUILD_CONFIG["source"].split("/")[-1].replace(".git", "")
            remote_repo_dir = f"{remote_workdir.rstrip('/')}/{repo_name}"
            remote_hraftd = f"{remote_repo_dir}/hraftd"
            remote_dir = f"{remote_repo_dir}/node{i+1}"

            remote_command = (
                f"pids=$(ps aux | grep '{remote_hraftd}' | grep -v grep | awk '{{print $2}}'); "
                f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                f"rm -rf {remote_dir};"
            )

            cmd = ["ssh", "-i", str(ssh["key"]), f"{user}@{host}",
                   remote_command]
            print("Running command:", " ".join(cmd))
            subprocess.run(cmd)


def map_ip_port(nodes):
    data = []
    ip_map = {}
    for node in nodes:
        public_ip = node["public"]
        private_ip = node["private"]

        # Check duplicate machine using only public IP address)
        if (public_ip not in ip_map
                or ip_map[public_ip] is None):
            ip_map[public_ip] = {}
            ip_map[public_ip]["client"] = 2001
            ip_map[public_ip]["peer"] = 3001
        else:
            ip_map[public_ip]["client"] += 1
            ip_map[public_ip]["peer"] += 1

        data.append({"client_ip": public_ip,
                     "client_port": ip_map[public_ip]["client"],
                     "peer_ip": private_ip,
                     "peer_port": ip_map[public_ip]["peer"]})

    return data


if __name__ == "__main__":
    raise RuntimeError("This script is meant to be imported, not run directly")

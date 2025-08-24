from pathlib import Path
import subprocess
import threading
import time
import os
import signal

from src.utils import helper

CURR_DIR = Path("./sut/otoolep.hraftd")
HRAFTD_BIN = CURR_DIR / "hraftd"
HRAFTD_GIT = "https://github.com/otoolep/hraftd.git"

OPTIONS = [{"num": 0, "text": "Start hraftd cluster"},
           {"num": 1, "text": "Stop hraftd cluster"},
           {"num": 2, "text": "Run Benchmark"}]


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
                start_hraftd_cluster(node_data, ssh)
            case 1:
                stop_hraftd_cluster(node_data, ssh)
            case 2:
                endpoints = [f"http://{node["client_ip"]}:{node["client_port"]}" for node in node_data]
                print("endpoint list:", endpoints)
                run_ycsb({"name": "raft", "language": "Go"}, "hraftd", endpoints, "hraftd.hosts")


def start_hraftd_cluster(nodes, ssh):
    user = ssh["username"]
    join = None
    for i, node in enumerate(nodes):
        if node["client_ip"] == "127.0.0.1" and node["peer_ip"] == "127.0.0.1":
            haddr = f"{node["peer_ip"]}:{node["client_port"]}"
            raddr = f"{node["peer_ip"]}:{node["peer_port"]}"
            remote_dir = f"/home/{user}/hraftd/node{i+1}"
            local_dir = CURR_DIR / f"node{i+1}"

            run_cmd = (
                f"nohup {HRAFTD_BIN.resolve()} -id node{i+1} -haddr {haddr} "
                f"-raddr {raddr} {"" if join is None else join} {local_dir} > /dev/null 2>&1 &"
            )
        else:
            host = node["client_ip"]

            build_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'if [ -f ~/go/bin/hraftd ]; then "
                f"echo \"hraftd already exists in {host}\"; "
                f"else "
                f"mkdir -p ~/hraftd && cd ~/hraftd && git clone {HRAFTD_GIT} "
                f"&& cd hraftd && go install && go build; "
                f"fi'"
            )

            print("Running command:", build_cmd)
            subprocess.run(build_cmd, check=True, shell=True)

            remote_hraftd = f"/home/{user}/hraftd/hraftd/hraftd"
            haddr = f"{node["peer_ip"]}:{node["client_port"]}"
            raddr = f"{node["peer_ip"]}:{node["peer_port"]}"
            remote_dir = f"/home/{user}/hraftd/node{i+1}"

            run_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'nohup {remote_hraftd} -id node{i+1} -haddr {haddr} "
                f"-raddr {raddr} {"" if join is None else join} {remote_dir} > /dev/null 2>&1 &'"
            )

        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)

        if join is None:
            join = f"-join {node["peer_ip"]}:{node["client_port"]}"
    print("hraftd cluster successfully started")


def stop_hraftd_cluster(nodes, ssh):
    user = ssh["username"]
    remote_hraftd = f"/home/{user}/hraftd/hraftd/hraftd"

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
            remote_dir = f"/home/{user}/hraftd/node{i+1}"

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

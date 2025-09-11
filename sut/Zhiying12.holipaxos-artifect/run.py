from pathlib import Path
import json
import subprocess
import threading
import os
import time
import shutil

from src.utils import helper

CURR_DIR = Path("./sut/Zhiying12.holipaxos-artifect/")
BIN_DIR = CURR_DIR / "bin"
LOG_DIR = CURR_DIR / "logs"
CONFIG_DIR = CURR_DIR / "config"

OPTIONS = [{"num": 0, "text": "Start HoliPaxos cluster"},
           {"num": 1, "text": "Stop HoliPaxos cluster"},
           {"num": 2, "text": "Run Benchmark"}]

PROTOCOLS = [{"num": 1, "text": "holipaxos", "language": "Go", "binary": "holipaxos_replicant"},
             {"num": 2, "text": "multipaxos", "language": "Go", "binary": "multipaxos_replicant"},
             {"num": 3, "text": "omnipaxos", "language": "Rust", "binary": "omni_replicant"}]

PERSISTENCY = [{"num": 1, "text": "In-Memory"},
               {"num": 2, "text": "On-Disk"}]


def main(run_ycsb, nodes, ssh) -> None:
    selected_protocol = None
    protocol_num = helper.get_option(1, len(PROTOCOLS), PROTOCOLS)
    persistency_num = helper.get_option(1, len(PERSISTENCY), PERSISTENCY)

    selected_protocol = {
        "name": PROTOCOLS[protocol_num-1]["text"],
        "language": PROTOCOLS[protocol_num-1]["language"],
        "consistency": "Linearizability",
        "persistency": PERSISTENCY[persistency_num-1]["text"],
        "binary": PROTOCOLS[protocol_num-1]["binary"],
    }

    port_map = map_ip_port(nodes)

    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start(selected_protocol, ssh, port_map)
            case 1:
                stop(selected_protocol, ssh, port_map)
            case 2:
                endpoints = [f"{node["private_ip"]}:{node["client_port"]}"
                             for node in port_map]
                print("endpoint list:", endpoints)
                print("selected protocol:", selected_protocol)

                run_ycsb(selected_protocol, "holipaxos", [",".join(endpoints)],
                         "holipaxos.hosts", ssh)


def start(protocol, ssh, ip_port_map):
    config_path = generate_config(ip_port_map, protocol["persistency"])

    binary_path = BIN_DIR / protocol["binary"]
    for i, node in enumerate(ip_port_map):
        if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
            if protocol["name"] == "omnipaxos":
                run_cmd = f"nohup {binary_path.resolve()} --id {i} --config-path {config_path.resolve()} > /dev/null 2>&1 &"
            else:
                run_cmd = f"nohup {binary_path.resolve()} -id {i} -c {config_path.resolve()} -d > /dev/null 2>&1 &"
        else:
            user = ssh["username"]
            host = node["public_ip"]
            remote_dir = f"/home/{user}/holipaxos"
            remote_binary = f"{remote_dir}/{protocol["binary"]}"
            remote_config = f"{remote_dir}/run_config.json"

            copy_cmd = ["rsync", "-avz", "-e", f"ssh -i {ssh['key']}",
                        str(config_path.resolve()), str(binary_path.resolve()),
                        f'{user}@{host}:{remote_dir}/']

            if protocol["name"] == "omnipaxos":
                run_cmd = (
                    f"ssh -i {ssh['key']} {user}@{host} "
                    f"'nohup {remote_binary} --id {i} "
                    f"--config-path {remote_config} > /dev/null 2>&1 &'"
                )
            else:
                run_cmd = (
                    f"ssh -i {ssh['key']} {user}@{host} "
                    f"'nohup {remote_binary} -id {i} "
                    f"-c {remote_config} > /dev/null 2>&1 &'"
                )

            print("Running command:", " ".join(copy_cmd))
            subprocess.run(copy_cmd, check=True)

        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)


def stop(protocol, ssh, ip_port_map):
    user = ssh["username"]
    for i, node in enumerate(ip_port_map):
        if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
            binary_path = BIN_DIR / protocol["binary"]
            cmd = (
                f"pids=$(ps aux | grep '{binary_path.resolve()}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
            )
            print("Running command:", cmd)
            subprocess.run(cmd, shell=True)
        else:
            host = node["public_ip"]
            remote_dir = f"/home/{user}/holipaxos"
            remote_binary = f"{remote_dir}/{protocol["binary"]}"
            remote_config = f"{remote_dir}/run_config.json"

            remote_command = (
                f"pids=$(ps aux | grep '{remote_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm {remote_config};"
            )

            cmd = ["ssh", "-i", str(ssh["key"]), f"{user}@{host}",
                   remote_command]
            print("Running command:", " ".join(cmd))
            subprocess.run(cmd)

    config = CURR_DIR / "run_config.json"
    os.system(f"rm {config.resolve()}")


def generate_config(port_map, persistency):
    # Create custom config.json file
    with open(CURR_DIR / "template.json", 'r') as file:
        data = json.load(file)

        for node in port_map:
            data["peers"].append(f"{node["private_ip"]}:{node["peer_port"]}")

            if persistency == "In-Memory":
                data["store"] = "mem"
            elif persistency == "On-Disk":
                data["store"] = "rocksdb"
            else:
                raise RuntimeError(f"{persistency} persistency is not supported.")

    config = CURR_DIR / "run_config.json"
    with open(config, "w") as f:
        json.dump(data, f, indent=2)

    print("run_config.json has been generated.")
    return config


def map_ip_port(nodes):
    data = []
    ip_map = {}
    for node in nodes:
        public_ip = node["public"]
        private_ip = node["private"]

        # Check duplicate machine using only public IP address)
        if (public_ip not in ip_map
                or ip_map[public_ip] is None):
            ip_map[public_ip] = 2000
        else:
            ip_map[public_ip] += 1

        data.append({"public_ip": public_ip,
                     "private_ip": private_ip,
                     "peer_port": ip_map[public_ip],
                     "client_port": ip_map[public_ip] + 1})

    return data


if __name__ == "__main__":
    raise RuntimeError("This script is meant to be imported, not run directly")


def hello():
    print("Hello from holipaxos")

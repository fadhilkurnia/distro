from pathlib import Path
import subprocess
import threading
import os
import json
import signal

from src.utils import helper

CURR_DIR = Path("./sut/ailidani.paxi")
PAXI_BIN = CURR_DIR / "paxi" / "bin"

OPTIONS = [{"num": 0, "text": "Start Paxi"},
           {"num": 1, "text": "Stop Paxi"},
           {"num": 2, "text": "Run Benchmark"}]

PROTOCOLS = [{"num": 1, "text": "paxos", "consistency": "Linearizability"},
             {"num": 2, "text": "epaxos", "consistency": "Linearizability"},
             {"num": 3, "text": "sdpaxos", "consistency": "Linearizability"},
             {"num": 4, "text": "wpaxos", "consistency": "Linearizability"},
             {"num": 5, "text": "abd", "consistency": "Linearizability"},
             {"num": 6, "text": "chain", "consistency": "Linearizability"},
             {"num": 7, "text": "vpaxos", "consistency": "Linearizability"},
             {"num": 8, "text": "wankeeper", "consistency": "Linearizability"},
             {"num": 9, "text": "kpaxos", "consistency": "Linearizability"},
             {"num": 10, "text": "paxos_groups", "consistency": "Linearizability"},
             {"num": 11, "text": "dynamo", "consistency": "Eventual"},
             {"num": 12, "text": "blockchain", "consistency": "Linearizability"},
             {"num": 13, "text": "m2paxos", "consistency": "Linearizability"},
             {"num": 14, "text": "hpaxos", "consistency": "Linearizability"}]


def main(run_ycsb, nodes, ssh) -> None:
    """
    Main function called by the root main.py script.
    Gives user a choice to start/stop paxi instances
    and to run the YCSB benchmark on the instance.

    :param run_ycsb: Function to run YCSB benchmark. Takes in protocol
                     data {name, language} and YCSB interface name as argument.
    :type run_ycsb: Callable[dict[str, str], str]
    """
    selected_protocol = None
    port_map = map_ip_port(nodes)
    prot_num = helper.get_option(1, len(PROTOCOLS), PROTOCOLS)
    selected_protocol = {
        "name": PROTOCOLS[prot_num-1]["text"],
        "language": "Go",
        "consistency": PROTOCOLS[prot_num-1]["consistency"],
        "persistency": "In-Memory"
    }

    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start_paxi(PAXI_BIN, selected_protocol, nodes, ssh, port_map)
            case 1:
                stop_paxi(PAXI_BIN, nodes, ssh)
            case 2:
                endpoints = [f"http://{ip}:{port}"
                             for ip, port in port_map["public"].items()]
                print("endpoint list:", endpoints)
                print("selected protocol:", selected_protocol)
                run_ycsb(selected_protocol, "paxi", endpoints,
                         "rest.endpoint", ssh)


def start_paxi(path, protocol, nodes, ssh, port_map):
    """
    Runs the paxi instances with the specified protocol in different
    threads concurrently. Currently only supports local startup.

    :param path: Path to bin/ directory inside the paxi repository.
    :type path: Path
    :param protocol: Protocol data that is being started.
    :type protocol: dict[str, str]
    """
    print("Paxi nodes:", nodes)
    print("Paxi ssh:", ssh)

    # Create custom config.json file
    with open(CURR_DIR / "template.json", 'r') as file:
        data = json.load(file)

    for i, node in enumerate(nodes):
        id = f"1.{i+1}"
        public_ip = node["public"]
        private_ip = node["private"]
        public_port = port_map['public'][public_ip]
        private_port = port_map['private'][private_ip]

        data["address"][id] = f"tcp://{private_ip}:{private_port}"
        data["http_address"][id] = f"http://{public_ip}:{public_port}"

    config = CURR_DIR / "run_config.json"
    with open(config, "w") as f:
        json.dump(data, f, indent=2)

    server = path / "server"
    for i, node in enumerate(nodes):
        if node["private"] == "127.0.0.1" and node["public"] == "127.0.0.1":
            run_cmd = f"nohup {server.resolve()} -id 1.{i+1} -algorithm={protocol['name']} -config {config} > /dev/null 2>&1 &"
        else:
            user = ssh["username"]
            host = node["public"]
            remote_dir = f"/home/{user}/paxi"
            remote_server = f"{remote_dir}/server"
            remote_config = f"{remote_dir}/run_config.json"

            copy_cmd = ["rsync", "-avz", "-e", f"ssh -i {ssh['key']}",
                        str(config.resolve()), str(server.resolve()),
                        f'{user}@{host}:{remote_dir}/']
            run_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'nohup {remote_server} -id 1.{i+1} -algorithm={protocol['name']} "
                f"-config {remote_config} > /dev/null 2>&1 &'"
            )

            print("Running command:", " ".join(copy_cmd))
            subprocess.run(copy_cmd, check=True)

        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)

    print(f"Paxi {protocol['name']} instances successfully started")


def stop_paxi(path, nodes, ssh) -> None:
    """
    Terminates all running instances of paxi that are still recorded inside
    the jobs list, then removes all the logfiles created by the instances.
    """
    server = path / "server"
    user = ssh["username"]
    for i, node in enumerate(nodes):

        if node["private"] == "127.0.0.1" and node["public"] == "127.0.0.1":
            cmd = (
                f"pids=$(ps aux | grep '{server}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
            )
            print("Running command:", cmd)
            subprocess.run(cmd, shell=True)
            os.system("rm server.*.log")
        else:
            host = node["public"]
            remote_dir = f"/home/{user}/paxi"
            remote_server = f"{remote_dir}/server"

            remote_command = (
                f"pids=$(ps aux | grep '{remote_server}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm /home/{user}/server.*.log;"
                    f"rm {remote_dir}/run_config.json;"
            )

            cmd = ["ssh", "-i", str(ssh["key"]), f"{user}@{host}",
                   remote_command]
            print("Running command:", " ".join(cmd))
            subprocess.run(cmd)

    config = CURR_DIR / "run_config.json"
    os.system(f"rm {config.resolve()}")


def map_ip_port(nodes):
    port = {"private": {}, "public": {}}
    for node in nodes:
        public_ip = node["public"]
        private_ip = node["private"]

        if (public_ip not in port["public"]
                or port["public"][public_ip] is None):
            port["public"][public_ip] = 3000
        else:
            port["public"][public_ip] += 1

        if (private_ip not in port["private"]
                or port["private"][private_ip] is None):
            port["private"][private_ip] = 2000
        else:
            port["private"][private_ip] += 1

    return port


if __name__ == "__main__":
    raise RuntimeError("This script is meant to be imported, not run directly")


def hello():
    print("Hello from paxi")

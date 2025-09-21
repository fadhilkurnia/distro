import json
import logging
from pathlib import Path

from sut.abstract import Launcher
from src.utils import helper

PAXI_DIR = Path("./sut/ailidani.paxi")
PAXI_BIN = PAXI_DIR / "paxi" / "bin"
REPO = "git@github.com:ailidani/paxi.git"
COMMIT_HASH = "6823d0b0fb1690a906391bcd5b4e0b01486ea2bd"

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


class PaxiLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of PaxiLauncher")
        super().__init__(nodes, ssh, client_ip, num_of_nodes, output_file)

    def map_ip_port(self):
        port = {"private": {}, "public": {}}
        for node in self.nodes:
            public_ip = node["public_ip"]
            private_ip = node["private_ip"]

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

    def launch(self):
        logging.info("Launching PaxiLauncher")
        prot_num = helper.get_option(1, len(PROTOCOLS), PROTOCOLS)

        self.project_name = "ailidani.paxi"
        self.project_repository = REPO
        self.project_commit = COMMIT_HASH
        self.ycsb_interface = "paxi"
        self.ycsb_endpoint = "rest.endpoint"
        self.selected_protocol = {
            "name": PROTOCOLS[prot_num-1]["text"],
            "language": "Go",
            "consistency": PROTOCOLS[prot_num-1]["consistency"],
            "persistency": "In-Memory",
        }

        # Build here...
        self.build()

        port_map = self.map_ip_port()

        while True:
            val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
            print()

            match val:
                case 0:
                    self.start(port_map)
                case 1:
                    self.stop(port_map)
                case 2:
                    endpoints = [f"http://{ip}:{port}" for ip,
                                 port in port_map["public"].items()]
                    self.ycsb(endpoints)

    def generate_config(self, port_map):
        logging.info("Generating run_config.json file")
        with open(PAXI_DIR / "template.json", 'r') as file:
            data = json.load(file)

        for i, node in enumerate(self.nodes):
            id = f"1.{i+1}"
            public_ip = node["public_ip"]
            private_ip = node["private_ip"]
            public_port = port_map['public'][public_ip]
            private_port = port_map['private'][private_ip]

            data["address"][id] = f"tcp://{private_ip}:{private_port}"
            data["http_address"][id] = f"http://{public_ip}:{public_port}"

        config = PAXI_DIR / "run_config.json"
        with open(config, "w") as f:
            json.dump(data, f, indent=2)

        return config

    def start(self, port_map):
        config_path = self.generate_config(port_map)
        self.build(config_path)

        binary = PAXI_BIN / "server"
        for i, node in enumerate(self.nodes):
            logging.info(f"Starting Paxi instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                run_cmd = (
                    f"nohup {binary.resolve()} -id 1.{i+1} "
                    f"-algorithm={self.selected_protocol['name']} "
                    f"-config {config_path} > /dev/null 2>&1 &"
                )
                self.local_run_cmd(run_cmd)
            else:
                remote_dir = f"/home/{self.user}/paxi"
                remote_binary = f"{remote_dir}/server"
                remote_config = f"{remote_dir}/run_config.json"
                run_cmd = (
                    f"nohup {remote_binary} -id 1.{i+1} "
                    f"-algorithm={self.selected_protocol['name']} "
                    f"-config {remote_config} > /dev/null 2>&1 &"
                )
                self.remote_run_cmd(node["public_ip"], run_cmd, True)

        logging.info(f"All paxi {self.selected_protocol['name']} instances successfully started")

    def stop(self, port_map):
        binary = PAXI_BIN / "server"
        local_config = PAXI_DIR / "run_config.json"

        for i, node in enumerate(self.nodes):
            logging.info(f"Stopping Paxi instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                stop_cmd = (
                    f"pids=$(ps aux | grep '{binary.resolve()}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm server.*.log; "
                    f"rm {local_config.resolve()}"
                )
                self.local_run_cmd(stop_cmd)
            else:
                remote_dir = f"/home/{self.user}/paxi"
                remote_binary = f"{remote_dir}/server"
                remote_config = f"{remote_dir}/run_config.json"
                stop_cmd = (
                    f"pids=$(ps aux | grep '{remote_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm /home/{self.user}/server.*.log; "
                    f"rm {remote_config}"
                )

                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

        logging.info(f"All paxi {self.selected_protocol['name']} instances successfully stopped")

    def build(self, config_path=None):
        logging.info("Checking if protocol executables already exists...")

        path = self.ensure_repo_exists(self.local_dir,
                                       self.project_repository,
                                       COMMIT_HASH)

        logging.info("Building protocol executables...")
        '''
        self.check_dependency(DEPENDECIES.golang, ">=13123.123")

        self._local_run_cmd(....)
        self._remote_run_cmd(....)
        '''



        return

        # rsync data to remote nodes
        binary = PAXI_BIN / "server"
        source_files = f"{str(config_path.resolve())} {str(binary.resolve())}"
        remote_dir = f"/home/{self.user}/paxi"

        for node in self.nodes:
            if node["public_ip"] == "127.0.0.1":
                continue

            self.remote_rsync(node["public_ip"], source_files, remote_dir)


'''
def start(path, protocol, nodes, ssh, port_map):
    config_path = generate_config(nodes, port_map)

    server = path / "server"
    for i, node in enumerate(nodes):
        if node["private"] == "127.0.0.1" and node["public"] == "127.0.0.1":
            local_start(server, protocol, config_path, i+1)
        else:
            remote_start(server, protocol, config_path, i+1, node, ssh)

    print(f"Paxi {protocol['name']} instances successfully started")




def local_start(binary, protocol, config_path, id):
    run_cmd = (
        f"nohup {binary.resolve()} -id 1.{id} -algorithm={protocol['name']} "
        f"-config {config_path} > /dev/null 2>&1 &"
    )
    print("Starting Paxi on localhost")
    subprocess.run(run_cmd, check=True, shell=True)


def remote_start(binary, protocol, config_path, id, node):
    host = node["public"]
    user = ssh["username"]
    remote_dir = f"/home/{user}/paxi"
    remote_server = f"{remote_dir}/server"
    remote_config = f"{remote_dir}/run_config.json"

    copy_cmd = ["rsync", "-avz", "-e", f"ssh -i {ssh['key']}",
                str(config_path.resolve()), str(binary.resolve()),
                f'{user}@{host}:{remote_dir}/']

    run_cmd = (
        f"ssh -i {ssh['key']} {user}@{host} "
        f"'nohup {remote_server} -id 1.{id} -algorithm={protocol['name']} "
        f"-config {remote_config} > /dev/null 2>&1 &'"
    )

    copy_cmd = ["rsync", "-avz", "-e", f"ssh -i {ssh['key']}",
                str(config_path.resolve()), str(binary.resolve()),
                f'{user}@{host}:{remote_dir}/']
    subprocess.run(copy_cmd, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"Starting Paxi remotely on {host}")
    subprocess.run(run_cmd, check=True, shell=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
'''

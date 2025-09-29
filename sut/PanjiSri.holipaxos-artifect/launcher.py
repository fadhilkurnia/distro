import json
import logging
import os

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/PanjiSri/holipaxos-artifect.git"
COMMIT_HASH = "834cbcdce27391916485f7dffa88f217c122053b"

ROCKSDB_REPO = "https://github.com/facebook/rocksdb.git"
ROCKSDB_COMMIT = "e859c3b7af8892064b1538a58565f7cc3ec354d5"

OPTIONS = [{"num": 0, "text": "Start Protocol"},
           {"num": 1, "text": "Stop Protocol"},
           {"num": 2, "text": "Run Benchmark"}]

PROTOCOLS = [{"num": 1, "text": "holipaxos", "language": "Go"},
             {"num": 2, "text": "multipaxos", "language": "Go"},
             {"num": 3, "text": "omnipaxos", "language": "Rust"}]

PERSISTENCY = [{"num": 1, "text": "In-Memory"},
               {"num": 2, "text": "On-Disk"}]


class HolipaxosLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of HolipaxosLauncher")
        super().__init__(nodes, ssh, client_ip, num_of_nodes, output_file)

    def map_ip_port(self):
        data = []
        ip_map = {}
        for node in self.nodes:
            public_ip = node["public_ip"]
            private_ip = node["private_ip"]

            # Check duplicate machine using only public IP address
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

    def launch(self):
        logging.info("Launching HolipaxosLauncher")
        prot_num = helper.get_option(1, len(PROTOCOLS), PROTOCOLS, "\nSelect a protocol:")
        pers_num = helper.get_option(1, len(PERSISTENCY), PERSISTENCY, "\nSelect a Persistency Model:")

        self.project_name = "PanjiSri.holipaxos-artifect"
        self.remote_dir = f"~/distro/{self.project_name}"
        self.project_repository = REPO
        self.project_commit = COMMIT_HASH
        self.ycsb_interface = "holipaxos"
        self.ycsb_endpoint = "holipaxos.hosts"
        self.selected_protocol = {
            "name": PROTOCOLS[prot_num-1]["text"],
            "language": PROTOCOLS[prot_num-1]["language"],
            "consistency": "Linearizability",
            "persistency": PERSISTENCY[pers_num-1]["text"],
        }

        node_maps = self.map_ip_port()
        self.build()

        while True:
            val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
            print()

            match val:
                case 0:
                    self.start(node_maps)
                case 1:
                    self.stop(node_maps)
                case 2:
                    endpoints = [f"{node["private_ip"]}:{node["client_port"]}" for node in node_maps]
                    self.ycsb([",".join(endpoints)])

    def generate_config(self, node_maps):
        logging.info("Generating run_config.json file")

        persistency = self.selected_protocol["persistency"]
        with open(f"{self.local_dir}/template.json", 'r') as file:
            data = json.load(file)

            for node in node_maps:
                data["peers"].append(f"{node["private_ip"]}:{node["peer_port"]}")

                if persistency == "In-Memory":
                    data["store"] = "mem"
                elif persistency == "On-Disk":
                    data["store"] = "rocksdb"
                else:
                    raise RuntimeError(f"{persistency} persistency is not supported.")

        config = f"{self.local_dir}/run_config.json"
        with open(config, "w") as f:
            json.dump(data, f, indent=2)

        return config

    def start(self, node_maps):
        config_path = self.generate_config(node_maps)

        local_binary = self.get_local_binary_path()
        source_files = f"{config_path} {local_binary}"

        # Send binary to remote machine
        for node in self.nodes:
            if node["public_ip"] == "127.0.0.1":
                continue

            logging.info(f"Sending protocol executables to {node["public_ip"]}")
            mkdir_cmd = f"mkdir -p {self.remote_dir}"
            self.remote_run_cmd(node["public_ip"], mkdir_cmd)
            self.remote_rsync(node["public_ip"], source_files, self.remote_dir)

        # Start holipaxos instances
        for i, node in enumerate(self.nodes):
            logging.info(f"Starting {self.selected_protocol["name"]} instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                run_cmd = self.get_run_cmd(local_binary, i, config_path)
                self.local_run_cmd(run_cmd)
            else:
                remote_binary = f"{self.remote_dir}/replicant"
                remote_config = f"{self.remote_dir}/run_config.json"
                run_cmd = self.get_run_cmd(remote_binary, i, remote_config)
                self.remote_run_cmd(node["public_ip"], run_cmd, True)

        logging.info(f"All {self.selected_protocol['name']} instances successfully started")

    def stop(self, port_map):
        local_binary = self.get_local_binary_path()
        remote_binary = f"{self.project_name}/replicant"

        for i, node in enumerate(self.nodes):
            logging.info(f"Stopping {self.selected_protocol["name"]} instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                stop_cmd = (
                    f"pids=$(ps aux | grep '{local_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm server.{i}.log; "
                )
                self.local_run_cmd(stop_cmd)
            else:
                remote_config = f"{self.remote_dir}/run_config.json"
                stop_cmd = (
                    f"pids=$(ps aux | grep '{remote_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm {self.remote_dir}/server.{i}.log; "
                    f"rm {remote_config}"
                )
                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

        local_config = f"{self.local_dir}/run_config.json"
        rm_config_cmd = f"rm {local_config}"
        self.local_run_cmd(rm_config_cmd)

        logging.info(f"All {self.selected_protocol['name']} instances successfully stopped")

    def build(self):
        logging.info("Checking if protocol binaries already exists...")
        self.check_dependency(DEPENDENCIES["golang"], ">=1.22")
        self.check_dependency(DEPENDENCIES["rust"], ">=1.87")

        rocksdb_path, matching_commit = self.ensure_repo_exists(self.local_dir,
                                                                ROCKSDB_REPO,
                                                                ROCKSDB_COMMIT)

        rocksdb_bin = f"{rocksdb_path}/librocksdb.so"
        if not os.path.isfile(rocksdb_bin) or not matching_commit:
            logging.info("Building rocksdb from source...")
            build_cmd = (
                f"cd {rocksdb_path} && "
                "sudo make install-shared"
            )
            self.local_run_cmd(build_cmd)

        path, matching_commit = self.ensure_repo_exists(self.local_dir,
                                                        self.project_repository,
                                                        COMMIT_HASH)
        self.repo_dir_path = path
        binary = self.get_local_binary_path()
        build_cmd = self.get_build_cmd()

        # Rebuild if binary doesn't exist
        # or if repo commit doesn't match the default commit hash
        if not os.path.isfile(binary) or not matching_commit:
            logging.info("Building protocol executables...")
            self.local_run_cmd(build_cmd)

        logging.info(f"{self.selected_protocol["name"]} build complete")

    def get_local_binary_path(self):
        prot_name = self.selected_protocol["name"]
        match prot_name:
            case "holipaxos" | "multipaxos":
                binary = f"{self.repo_dir_path}/{prot_name}/bin/replicant"
            case "omnipaxos":
                binary = f"{self.repo_dir_path}/{prot_name}-kv-store/replicant/target/release/replicant"
            case _:
                raise ValueError(f"Protocol {prot_name} not found.")

        return binary

    def get_build_cmd(self):
        prot_name = self.selected_protocol["name"]
        match prot_name:
            case "holipaxos" | "multipaxos":
                build_cmd = (
                    f"cd {self.repo_dir_path}/{prot_name} && "
                    f"go build -o bin/replicant main/main.go"
                )
            case "omnipaxos":
                build_cmd = (
                    f"cd {self.repo_dir_path}/{prot_name}-kv-store/replicant && "
                    f"cargo build --release"
                )
            case _:
                raise ValueError(f"Protocol {prot_name} not found.")

        return build_cmd

    def get_run_cmd(self, binary_path, id, config_path):
        prot_name = self.selected_protocol["name"]
        match prot_name:
            case "holipaxos" | "multipaxos":
                run_cmd = f"nohup {binary_path} -id {id} -c {config_path} -d > /dev/null 2>&1 &"
            case "omnipaxos":
                run_cmd = f"nohup {binary_path} --id {id} --config-path {config_path} > /dev/null 2>&1 &"
            case _:
                raise ValueError(f"Protocol {prot_name} not found.")

        return run_cmd

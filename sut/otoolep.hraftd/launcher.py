import logging
import os

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/otoolep/hraftd.git"
COMMIT_HASH = "b931e1f8956e13f0cf51ec02f6eaea5c28439b1c"

OPTIONS = [{"num": 0, "text": "Start hraftd"},
           {"num": 1, "text": "Stop hraftd"},
           {"num": 2, "text": "Run Benchmark"}]


class HraftdLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of HraftdLauncher")
        super().__init__(nodes, ssh, client_ip, num_of_nodes, output_file)

    def map_ip_port(self):
        data = []
        ip_map = {}
        for node in self.nodes:
            public_ip = node["public_ip"]
            private_ip = node["private_ip"]

            # Check duplicate machine using only public IP address)
            if (public_ip not in ip_map
                    or ip_map[public_ip] is None):
                ip_map[public_ip] = {}
                ip_map[public_ip]["client"] = 2001
                ip_map[public_ip]["peer"] = 3001
            else:
                ip_map[public_ip]["client"] += 1
                ip_map[public_ip]["peer"] += 1

            data.append({"public_ip": public_ip,
                         "client_port": ip_map[public_ip]["client"],
                         "private_ip": private_ip,
                         "peer_port": ip_map[public_ip]["peer"]})

        return data

    def launch(self):
        logging.info("Launching HraftdLauncher")

        self.project_name = "otoolep.hraftd"
        self.remote_dir = f"~/distro/{self.project_name}"
        self.project_repository = REPO
        self.project_commit = COMMIT_HASH
        self.ycsb_interface = "hraftd"
        self.ycsb_endpoint = "hraftd.hosts"
        self.selected_protocol = {
            "name": "raft",
            "language": "Go",
            "consistency": "Linearizability",
            "persistency": "In-Memory",
        }

        nodes_map = self.map_ip_port()
        self.build()

        while True:
            val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
            print()

            match val:
                case 0:
                    self.start(nodes_map)
                case 1:
                    self.stop(nodes_map)
                case 2:
                    endpoints = [
                        f"http://{node['public_ip']}:{node['client_port']}"
                        for node in nodes_map
                    ]
                    self.ycsb(endpoints)

    def generate_config(self):
        logging.warning("HraftdLauncher does not implement generate_config()")

    def start(self, nodes_map):
        binary = f"{self.repo_dir_path}/hraftd"
        source_files = f"{binary}"

        # Send binary to remote machine
        for node in self.nodes:
            if node["public_ip"] == "127.0.0.1":
                continue

            logging.info(f"Sending hraftd binary to {node["public_ip"]}")
            mkdir_cmd = f"mkdir -p {self.remote_dir}"
            self.remote_run_cmd(node["public_ip"], mkdir_cmd)
            self.remote_rsync(node["public_ip"], source_files, self.remote_dir)

        # Start hraftd instances
        join = None

        for i, node in enumerate(nodes_map):
            logging.info(f"Starting hraftd instance on {node["public_ip"]}")
            haddr = f"{node['private_ip']}:{node['client_port']}"
            raddr = f"{node['private_ip']}:{node['peer_port']}"
            join_part = "" if join is None else join

            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                data_dir = f"{self.local_dir}/node{i+1}"
                run_cmd = (
                    f"nohup {binary} -id node{i+1} -haddr {haddr} "
                    f"-raddr {raddr} {join_part} {data_dir} > /dev/null 2>&1 &"
                )
                self.local_run_cmd(run_cmd)
            else:
                remote_binary = f"{self.remote_dir}/hraftd"
                data_dir = f"{self.remote_dir}/node{i+1}"
                run_cmd = (
                    f"nohup {remote_binary} -id node{i+1} -haddr {haddr} "
                    f"-raddr {raddr} {join_part} {data_dir} > /dev/null 2>&1 &"
                )
                self.remote_run_cmd(node["public_ip"], run_cmd, True)

            if join is None:
                join = f"-join {node['private_ip']}:{node['client_port']}"

        logging.info("All hraftd instances successfully started")

    def stop(self, nodes_map):
        binary = f"{self.repo_dir_path}/hraftd"

        for i, node in enumerate(nodes_map):
            logging.info(f"Stopping hraftd instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                data_dir = f"{self.local_dir}/node{i+1}"
                stop_cmd = (
                    f"pids=$(ps aux | grep '{binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm -rf {self.local_dir}; "
                )
                self.local_run_cmd(stop_cmd)
            else:
                remote_binary = f"{self.project_name}/hraftd"
                data_dir = f"{self.remote_dir}/node{i+1}"
                stop_cmd = (
                    f"pids=$(ps aux | grep '{remote_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm -rf {data_dir}"
                )

                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

        logging.info("All hraftd instances successfully stopped")

    def build(self):
        logging.info("Checking if protocol executables already exists...")
        path, matching_commit = self.ensure_repo_exists(self.local_dir,
                                                        self.project_repository,
                                                        COMMIT_HASH)
        self.repo_dir_path = path
        self.check_dependency(DEPENDENCIES["golang"], ">=1.20")
        binary = f"{path}/hraftd"

        # Rebuild if binary doesn't exist
        # or if repo commit doesn't match the default commit hash
        if not os.path.isfile(binary) or not matching_commit:
            logging.info("Building hraftd binary...")
            build_cmd = (
                f"cd {path} && "
                "go install && "
                "go build"
            )
            self.local_run_cmd(build_cmd)

        logging.info("Hraftd build complete")

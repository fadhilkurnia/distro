import logging
import os

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/etcd-io/etcd.git"

SOURCE_VER = "v3.4"
SOURCE_HASH = "f9d68b5f4c71969c1593881ee8792cd42e35499b"

RELEASE_VER = "v3.6.5"
GOOGLE_URL = "https://storage.googleapis.com/etcd"
RELEASE_URL = f"{GOOGLE_URL}/{RELEASE_VER}/etcd-{RELEASE_VER}-linux-amd64.tar.gz"
RELEASE_HASH = "a0614505aff9b8ff469e9c59c2d979a5936d13f4"


OPTIONS = [{"num": 0, "text": "Start etcd"},
           {"num": 1, "text": "Stop etcd"},
           {"num": 2, "text": "Run Benchmark"}]

BUILD_OPTIONS = [{"num": 1, "text": f"Official Release Build ({RELEASE_VER})"},
                 {"num": 2, "text": f"Build from Source ({SOURCE_VER})"}]


class EtcdLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of EtcdLauncher")
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
        logging.info("Launching EtcdLauncher")

        self.project_name = "etcd-io.etcd"
        self.remote_dir = f"~/distro/{self.project_name}"
        self.project_repository = REPO
        self.ycsb_interface = "etcd"
        self.ycsb_endpoint = "etcd.endpoints"
        self.selected_protocol = {
            "name": "raft",
            "language": "Go",
            "consistency": "Linearizability",
            "persistency": "On-Disk",
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
                    endpoints = [
                        f"http://{node["private_ip"]}:{node["client_port"]}"
                        for node in node_maps
                    ]
                    self.ycsb(endpoints)

    def generate_config(self):
        logging.warning("EtcdLauncher does not implement generate_config()")

    def start(self, node_maps):
        # Copy binary over to remote machine
        for i, node in enumerate(self.nodes):
            if node["public_ip"] == "127.0.0.1":
                continue

            source_files = f"{self.repo_dir_path}"

            logging.info(f"Sending etcd binary to {node["public_ip"]}")
            mkdir_cmd = f"mkdir -p {self.remote_dir}"
            self.remote_run_cmd(node["public_ip"], mkdir_cmd)
            self.remote_rsync(node["public_ip"], source_files, self.remote_dir)

        # Start instances
        initial_cluster = ",".join(f"node{i+1}=http://{n["private_ip"]}:{n["peer_port"]}" for i, n in enumerate(node_maps))

        for i, node in enumerate(node_maps):
            logging.info(f"Starting etcd instance on {node["public_ip"]}")

            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                local_binary = f"{self.repo_dir_path}/etcd"
                data_dir = f"{self.local_dir}/node{i+1}"

                run_cmd = (
                    f"nohup {local_binary} --name node{i+1} --data-dir {data_dir} "
                    f"--listen-peer-urls http://0.0.0.0:{node["peer_port"]} "
                    f"--initial-advertise-peer-urls http://{node["private_ip"]}:{node["peer_port"]} "
                    f"--listen-client-urls http://0.0.0.0:{node["client_port"]} "
                    f"--advertise-client-urls http://{node["private_ip"]}:{node["client_port"]},http://{node["public_ip"]}:{node["client_port"]} "
                    f"--initial-cluster {initial_cluster} "
                    f"--initial-cluster-state new "
                    f"--initial-cluster-token etcd-distrobench-cluster > /dev/null 2>&1 &"
                )
                self.local_run_cmd(run_cmd)
            else:
                remote_binary = f"{self.remote_dir}/{self.repo_dir_name}/etcd"
                data_dir = f"{self.remote_dir}/node{i+1}"

                run_cmd = (
                    f"nohup {remote_binary} --name node{i+1} --data-dir {data_dir} "
                    f"--listen-peer-urls http://0.0.0.0:{node["peer_port"]} "
                    f"--initial-advertise-peer-urls http://{node["private_ip"]}:{node["peer_port"]} "
                    f"--listen-client-urls http://0.0.0.0:{node["client_port"]} "
                    f"--advertise-client-urls http://{node["private_ip"]}:{node["client_port"]},http://{node["public_ip"]}:{node["client_port"]} "
                    f"--initial-cluster {initial_cluster} "
                    f"--initial-cluster-state new "
                    f"--initial-cluster-token etcd-distrobench-cluster > /dev/null 2>&1 &"
                )
                self.remote_run_cmd(node["public_ip"], run_cmd, True)
        logging.info("etcd cluster successfully started")

    def stop(self, node_maps):
        for i, node in enumerate(node_maps):
            logging.info(f"Stopping etcd instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                local_binary = f"{self.repo_dir_path}/etcd"
                local_dir = f"{self.local_dir}/node{i+1}"
                stop_cmd = (
                    f"pids=$(ps aux | grep '{local_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm -rf {local_dir}; "
                )
                self.local_run_cmd(stop_cmd)
            else:
                remote_binary = f"{self.project_name}/{self.repo_dir_name}/etcd"
                data_dir = f"{self.remote_dir}/node{i+1}"

                stop_cmd = (
                    f"pids=$(ps aux | grep '{remote_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm -rf {data_dir}; "
                )
                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

        logging.info("All etcd instances successfully stopped")

    def build(self):
        val = helper.get_option(1, len(BUILD_OPTIONS), BUILD_OPTIONS,
                                "\nPick Your Preferred Build Option:")

        logging.info("Checking if protocol executables already exists...")

        self.check_dependency(DEPENDENCIES["golang"], ">=1.12")

        if val == 1:
            # Download Release Build
            logging.info(f"Downloading official release build {RELEASE_VER}")
            self.project_commit = RELEASE_HASH

            extracted_binary = f"etcd-release-{RELEASE_VER}"

            if (not helper.check_subdir_exists(self.local_dir, extracted_binary)
                    and not os.path.isfile(f"{extracted_binary}/etcd")):

                os.makedirs(f"{self.local_dir}/{extracted_binary}", exist_ok=True)
                curl_cmd = (
                    f"curl -L {RELEASE_URL} | tar -xz -C "
                    f"{self.local_dir}/{extracted_binary} "
                    "--strip-components=1 --no-same-owner"
                )
                self.local_run_cmd(curl_cmd)
            self.repo_dir_path = f"{self.local_dir}/{extracted_binary}"
            self.repo_dir_name = extracted_binary
        else:
            # Build from Source
            logging.info(f"Building from source version {SOURCE_VER}")
            self.project_commit = SOURCE_HASH
            path, matching_commit = self.ensure_repo_exists(self.local_dir,
                                                            self.project_repository,
                                                            SOURCE_HASH)
            # Build
            binary = f"{path}/bin/etcd"

            if not os.path.isfile(binary) or not matching_commit:
                build_cmd = (
                    f"cd {path} && "
                    "./build "
                )
                self.local_run_cmd(build_cmd)

            self.repo_dir_path = f"{path}/bin"
            self.repo_dir_name = "bin"
        logging.info("etcd build & setup complete")

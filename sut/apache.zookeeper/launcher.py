import logging
import os
import subprocess
import time

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/apache/zookeeper.git"

SOURCE_VER = "3.10.0-SNAPSHOT"
SOURCE_HASH = "d8e5217729bfc7303b15bc36b1a6b7f1ecdd07d4"

RELEASE_VER = "3.8.5"
RELEASE_URL = "https://dlcdn.apache.org/zookeeper/zookeeper-3.8.5/apache-zookeeper-3.8.5-bin.tar.gz"
RELEASE_HASH = "19f1842d4b6ee1f82cc8b05284959b26c6ae507d"

OPTIONS = [{"num": 0, "text": "Start Zookeeper"},
           {"num": 1, "text": "Stop Zookeeper"},
           {"num": 2, "text": "Run Benchmark"}]

BUILD_OPTIONS = [{"num": 1, "text": "Official Release Artifact (Recommended)"},
                 {"num": 2, "text": "Build from Source"}]


class ZookeeperLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of ZookeeperLauncher")
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
                ip_map[public_ip]["peer"] = 2001
                ip_map[public_ip]["election"] = 3001
                ip_map[public_ip]["client"] = 2101
            else:
                ip_map[public_ip]["peer"] += 1
                ip_map[public_ip]["election"] += 1
                ip_map[public_ip]["client"] += 1

            data.append({"public_ip": public_ip,
                         "private_ip": private_ip,
                         "peer": ip_map[public_ip]["peer"],
                         "election": ip_map[public_ip]["election"],
                         "client": ip_map[public_ip]["client"]})

        return data

    def launch(self):
        logging.info("Launching ZookeeperLauncher")

        self.project_name = "apache.zookeeper"
        self.remote_dir = f"/home/{self.user}/{self.project_name}"
        self.project_repository = REPO
        self.ycsb_interface = "zookeeper"
        self.ycsb_endpoint = "zookeeper.connectString"
        self.selected_protocol = {
            "name": "zab",
            "language": "Java",
            "consistency": "Linearizability + Primary Integrity",
            "persistency": "In-Memory",
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
                    endpoints = [f"{node["public_ip"]}:{node["client"]}" for node in node_maps]
                    self.ycsb(endpoints)

    def generate_config(self, node_maps):
        template_config = []
        with open(f"{self.local_dir}/template.cfg", 'r') as file:
            for line in file:
                template_config.append(line.strip())

            for i, node in enumerate(node_maps):
                template_config.append(f"server.{i+1}={node["private_ip"]}:{node['peer']}:{node['election']}")

        for i, node in enumerate(node_maps):
            local_config_path = f"{self.local_dir}/cluster/node{i+1}/config.cfg"
            local_myid_path = f"{self.local_dir}/cluster/node{i+1}/data/myid"
            local_data_path = f"{self.local_dir}/cluster/node{i+1}/data"
            remote_data_path = f"{self.remote_dir}/node{i+1}/data"

            logging.info(f"Generating {local_config_path}")

            os.makedirs(local_data_path, exist_ok=True)
            with open(local_myid_path, "w") as f:
                f.writelines(f"{i+1}\n")

            config = template_config.copy()

            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                config.append(f"dataDir={local_data_path}")
            else:
                config.append(f"dataDir={remote_data_path}")

            config.append(f"clientPort={node["client"]}")

            with open(local_config_path, "w") as f:
                for line in config:
                    f.write(f"{line}\n")

    def start(self, node_maps):
        self.generate_config(node_maps)

        # Copy binary over to remote machine
        config_path = f"{self.local_dir}/cluster"

        for i, node in enumerate(self.nodes):
            if node["public_ip"] == "127.0.0.1":
                continue

            self.check_dependency(DEPENDENCIES["java"], ">=17", node["public_ip"])
            self.check_dependency(DEPENDENCIES["maven"], ">=3.8", node["public_ip"])
            copied_config = f"{config_path}/node{i+1}"
            source_files = f"{self.repo_dir_path} {copied_config}"

            logging.info(f"Sending protocol executables and configs to {node["public_ip"]}")
            self.remote_rsync(node["public_ip"], source_files, self.remote_dir)

        # Start instances
        for i, node in enumerate(node_maps):
            logging.info(f"Starting Zookeeper instance on {node["public_ip"]}")

            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                local_binary = f"{self.repo_dir_path}/bin/zkServer.sh"
                local_config = f"{self.local_dir}/cluster/node{i+1}/config.cfg"
                run_cmd = f"{local_binary} start {local_config}"
                self.local_run_cmd(run_cmd)
            else:
                remote_binary = f"{self.remote_dir}/{self.extracted_bin_name}/bin/zkServer.sh"
                remote_config = f"{self.remote_dir}/node{i+1}/config.cfg"
                run_cmd = f"{remote_binary} start {remote_config}"
                self.remote_run_cmd(node["public_ip"], run_cmd, True)

        time.sleep(5)

        # Insert /benchmark for YCSB from local machine
        client = f"{self.repo_dir_path}/bin/zkCli.sh"
        process = subprocess.Popen(
            [client, "-server", f"{node_maps["public_ip"]}:2101"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        process.stdin.write("create /benchmark\n")
        process.stdin.write("quit\n")
        process.stdin.flush()

        stdout, stderr = process.communicate()
        logging.debug("Local client created /benchmark")
        logging.info("Zookeeper cluster successfully started")

    def stop(self, node_maps):
        for i, node in enumerate(node_maps):
            logging.info(f"Stopping Zookeeper instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                local_binary = f"{self.repo_dir_path}/bin/zkServer.sh"
                local_config = f"{self.local_dir}/cluster/node{i+1}/config.cfg"
                stop_cmd = f"{local_binary} stop {local_config}"
                self.local_run_cmd(stop_cmd)
            else:
                remote_binary = f"{self.remote_dir}/{self.extracted_bin_name}/bin/zkServer.sh"
                remote_config = f"{self.remote_dir}/node{i+1}/config.cfg"
                stop_cmd = (
                    f"{remote_binary} stop {remote_config} && "
                    f"rm -rf {self.remote_dir}/node{i+1}"
                )
                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

        rm_config_cmd = f"rm -rf {self.local_dir}/cluster"
        self.local_run_cmd(rm_config_cmd)
        logging.info("All Zookeeper instances successfully stopped")

    def build(self):
        val = helper.get_option(1, len(BUILD_OPTIONS), BUILD_OPTIONS,
                                "\nPick Your Preferred Build Option:")

        logging.info("Checking if protocol executables already exists...")

        self.check_dependency(DEPENDENCIES["java"], ">=17")
        self.check_dependency(DEPENDENCIES["maven"], ">=3.8")

        if val == 1:
            # Download Release Artefact
            logging.info(f"Downloading release artefact version {RELEASE_VER}")
            self.project_commit = RELEASE_HASH

            extracted_binary = f"apache-zookeeper-{RELEASE_VER}-bin"

            if not helper.check_subdir_exists(self.local_dir, extracted_binary):
                curl_cmd = f"curl -L {RELEASE_URL} | tar -xz -C {self.local_dir}"
                self.local_run_cmd(curl_cmd)
        else:
            # Build from Source
            logging.info(f"Building from source version {SOURCE_VER}")
            self.project_commit = SOURCE_HASH
            path, matching_commit = self.ensure_repo_exists(self.local_dir,
                                                            self.project_repository,
                                                            SOURCE_HASH)
            # Build
            binary = f"{path}/zookeeper-assembly/target/apache-zookeeper-{SOURCE_VER}-bin.tar.gz"
            extracted_binary = f"apache-zookeeper-{SOURCE_VER}-bin"

            if not os.path.isfile(binary) or not matching_commit:
                logging.warning("Skipping build tests when building Zookeeper from source...")
                build_cmd = (
                    f"cd {path} && "
                    "mvn clean install -DskipTests"
                )
                self.local_run_cmd(build_cmd)

            # Extract Binary
            if not helper.check_subdir_exists(self.local_dir, extracted_binary):
                extract_cmd = (
                    f"tar -zxvf {binary} -C {self.local_dir}"
                )
                self.local_run_cmd(extract_cmd)

        self.extracted_bin_name = extracted_binary
        self.repo_dir_path = f"{self.local_dir}/{extracted_binary}"
        logging.info("Zookeeper build & setup complete")

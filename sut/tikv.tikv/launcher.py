import logging
import os

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/tikv/tikv.git"

SOURCE_VER = "v8.5"
SOURCE_HASH = "a092f0444428d2d106993d3a039de7bdca57642c"

RELEASE_VER = "v7.5.7"
RELEASE_OS = "linux"
RELEASE_ARCH = "amd64"
RELEASE_URL_PD = f"https://tiup-mirrors.pingcap.com/pd-{RELEASE_VER}-{RELEASE_OS}-{RELEASE_ARCH}.tar.gz"
RELEASE_URL_TIKV = f"https://tiup-mirrors.pingcap.com/tikv-{RELEASE_VER}-{RELEASE_OS}-{RELEASE_ARCH}.tar.gz"
RELEASE_HASH = "7f1d4fd723efd08203884203ae15cf28e5d421be"


OPTIONS = [{"num": 0, "text": "Start TiKV"},
           {"num": 1, "text": "Stop TiKV"},
           {"num": 2, "text": "Run Benchmark"}]

BUILD_OPTIONS = [{"num": 1, "text": f"Official Release Build ({RELEASE_VER})"},
                 {"num": 2, "text": f"Build from Source ({SOURCE_VER})"}]


class TikvLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of TikvLauncher")
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
                ip_map[public_ip]["service"] = 2101
            else:
                ip_map[public_ip]["client"] += 1
                ip_map[public_ip]["peer"] += 1
                ip_map[public_ip]["service"] += 1

            data.append({"public_ip": public_ip,
                         "private_ip": private_ip,
                         "client_port": ip_map[public_ip]["client"],
                         "peer_port": ip_map[public_ip]["peer"],
                         "service_port": ip_map[public_ip]["service"]})

        return data

    def launch(self):
        logging.info("Launching TikvLauncher")

        self.project_name = "tikv.tikv"
        self.remote_dir = f"/home/{self.user}/{self.project_name}"
        self.project_repository = REPO
        self.ycsb_interface = "tikv"
        self.ycsb_endpoint = "tikv.clientConnect"
        self.selected_protocol = {
            "name": "raft",
            "language": "Rust",
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
                        f"{node["private_ip"]}:{node["client_port"]}"
                        for node in node_maps
                    ]
                    self.ycsb(endpoints)

    def generate_config(self):
        logging.warning("TikvLauncher does not implement generate_config()")

    def start(self, node_maps):
        # Copy binary over to remote machine
        for i, node in enumerate(self.nodes):
            if node["public_ip"] == "127.0.0.1":
                continue

            source_files = f"{self.repo_dir_path}"
            logging.info(f"Sending TiKV binary to {node["public_ip"]}")
            self.remote_rsync(node["public_ip"], source_files, self.remote_dir)

        return
        # Start instances
        initial_cluster = ",".join(f"pd{i+1}=http://{n["private_ip"]}:{n["peer_port"]}" for i, n in enumerate(node_maps))
        pd_endpoints = ",".join(f"{n["private_ip"]}:{n["client_port"]}" for n in node_maps)

        for i, node in enumerate(node_maps):
            logging.info(f"Starting TiKV on {node["public_ip"]}")

            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                local_pd = f"{self.repo_dir_path}/pd-server"
                local_tikv = f"{self.repo_dir_path}/tikv-server"
                local_pd_dir = f"{self.local_dir}/pd{i+1}"
                local_tikv_dir = f"{self.local_dir}/tikv{i+1}"

                run_pd_cmd = (
                    f"nohup {local_pd} --name=pd{i+1} "
                    f"--data-dir={local_pd_dir} "
                    f"--client-urls=\"http://0.0.0.0:{node["client_port"]}\" "
                    f"--advertise-client-urls=\"http://{node["public_ip"]}:{node["client_port"]}\" "
                    f"--peer-urls=\"http://0.0.0.0:{node["peer_port"]}\" "
                    f"--advertise-peer-urls=\"http://{node["private_ip"]}:{node["peer_port"]}\" "
                    f"--initial-cluster=\"{initial_cluster}\" > /dev/null 2>&1 &"
                )
                self.local_run_cmd(run_pd_cmd)

                run_tikv_cmd = (
                    f"{local_tikv} --addr=\"0.0.0.0:{node["service_port"]}\" "
                    f"--advertise-addr=\"{node["public_ip"]}:{node["service_port"]}\" "
                    f"--data-dir={local_tikv_dir} "
                    f"--pd-endpoints=\"{pd_endpoints}\" > /dev/null 2>&1 &"
                )
                self.local_run_cmd(run_tikv_cmd)
            else:
                remote_pd = f"{self.remote_dir}/{self.repo_dir_name}/pd-server"
                remote_tikv = f"{self.remote_dir}/{self.repo_dir_name}/tikv-server"
                remote_pd_dir = f"{self.remote_dir}/pd{i+1}"
                remote_tikv_dir = f"{self.remote_dir}/tikv{i+1}"

                run_pd_cmd = (
                    f"nohup {remote_pd} --name=pd{i+1} "
                    f"--data-dir={remote_pd_dir} "
                    f"--client-urls=\"http://0.0.0.0:{node["client_port"]}\" "
                    f"--advertise-client-urls=\"http://{node["public_ip"]}:{node["client_port"]}\" "
                    f"--peer-urls=\"http://0.0.0.0:{node["peer_port"]}\" "
                    f"--advertise-peer-urls=\"http://{node["private_ip"]}:{node["peer_port"]}\" "
                    f"--initial-cluster=\"{initial_cluster}\" > /dev/null 2>&1 &"
                )
                self.remote_run_cmd(node["public_ip"], run_pd_cmd, True)

                run_tikv_cmd = (
                    f"nohup {remote_tikv} --addr=\"0.0.0.0:{node["service_port"]}\" "
                    f"--advertise-addr=\"{node["public_ip"]}:{node["service_port"]}\" "
                    f"--data-dir={remote_tikv_dir} "
                    f"--pd-endpoints=\"{pd_endpoints}\" > /dev/null 2>&1 &"
                )
                self.remote_run_cmd(node["public_ip"], run_tikv_cmd, True)
        logging.info("TiKV cluster successfully started")

    def stop(self, node_maps):
        for i, node in enumerate(node_maps):
            logging.info(f"Stopping TiKV instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                local_binary = f"{self.repo_dir_path}/.*-server"
                local_pd_dir = f"{self.local_dir}/pd{i+1}"
                local_tikv_dir = f"{self.local_dir}/tikv{i+1}"

                stop_cmd = (
                    f"pids=$(ps aux | grep '{local_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm -rf {local_pd_dir} && "
                    f"rm -rf {local_tikv_dir}"
                )
                self.local_run_cmd(stop_cmd)
            else:
                remote_binary = f"{self.remote_dir}/{self.repo_dir_name}/.*-server"
                remote_pd_dir = f"{self.remote_dir}/pd{i+1}"
                remote_tikv_dir = f"{self.remote_dir}/tikv{i+1}"

                stop_cmd = (
                    f"pids=$(ps aux | grep '{remote_binary}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                    f"rm -rf {remote_pd_dir} && "
                    f"rm -rf {remote_tikv_dir}"
                )
                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

        logging.info("All TiKV instances successfully stopped")

    def build(self):
        val = helper.get_option(1, len(BUILD_OPTIONS), BUILD_OPTIONS,
                                "\nPick Your Preferred Build Option:")

        logging.info("Checking if protocol executables already exists...")

        self.check_dependency(DEPENDENCIES["golang"], ">=1.16")
        self.check_dependency(DEPENDENCIES["cmake"], ">=3.28")
        self.check_dependency(DEPENDENCIES["gcc"], ">=13.3")
        self.check_dependency(DEPENDENCIES["rust"], ">=1.28")

        if val == 1:
            # Download Release Build
            logging.info(f"Downloading official release build {RELEASE_VER}")
            self.project_commit = RELEASE_HASH

            self.repo_dir_name = f"tikv-release-{RELEASE_VER}"
            self.repo_dir_path = f"{self.local_dir}/{self.repo_dir_name}"

            if not helper.check_subdir_exists(self.local_dir, self.repo_dir_name):
                os.makedirs(self.repo_dir_path, exist_ok=True)

            if not os.path.isfile(f"{self.repo_dir_path}/pd-server"):
                curl_cmd = (
                    f"cd {self.repo_dir_path} && "
                    f"curl -L {RELEASE_URL_PD} | tar -xz"
                )
                self.local_run_cmd(curl_cmd)

            if not os.path.isfile(f"{self.repo_dir_path}/tikv-server"):
                curl_cmd = (
                    f"cd {self.repo_dir_path} && "
                    f"curl -L {RELEASE_URL_TIKV} | tar -xz"
                )
                self.local_run_cmd(curl_cmd)
        else:
            # Build from Source
            logging.info("Building from source")
            logging.error("Building from source is not implemented yet...")
            raise RuntimeError("Build from source for TiKV is not implemented yet")

            self.project_commit = SOURCE_HASH
            path, matching_commit = self.ensure_repo_exists(self.local_dir,
                                                            self.project_repository,
                                                            SOURCE_HASH)
            # Build
            self.repo_dir_path = f"{path}/release/build"
            self.repo_dir_name = "build"

            binaries = ["tikv-server", "pd_server"]
            binary_exists = True
            for bin in binaries:
                if not os.path.isfile(f"{self.repo_dir_path}/{bin}"):
                    binary_exists = False

            if not binary_exists or not matching_commit:
                build_cmd = (
                    f"cd {path} && "
                    "cargo clean && "
                    "cargo build --release "
                )
                self.local_run_cmd(build_cmd)

        logging.info("TiKV build & setup complete")

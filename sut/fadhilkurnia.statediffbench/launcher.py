import logging

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/fadhilkurnia/statediffbench"
COMMIT_HASH = "d18a5f54360323e181ab7fcc417f356d899c3901"

OPTIONS = [{"num": 0, "text": "Start restkv"},
           {"num": 1, "text": "Stop restkv"},
           {"num": 2, "text": "Run Benchmark"}]


class RestKVLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of RestKVLauncher")
        super().__init__(nodes, ssh, client_ip, num_of_nodes, output_file)

    def map_ip_port(self):
        data = []
        node = self.nodes[0]

        data.append({
            "public_ip": node["public_ip"],
            "port": 8080})
        return data

    def launch(self):
        logging.info("Launching RestKVLauncher")

        self.project_name = "fadhilkurnia.statediffbench"
        self.remote_dir = f"~/distro/{self.project_name}"
        self.project_repository = REPO
        self.project_commit = COMMIT_HASH
        self.ycsb_interface = "xdn"
        self.ycsb_endpoint = "url.prefix"
        self.selected_protocol = {
            "name": "local docker",
            "language": "Rust",
            "consistency": "None",
            "persistency": "On-Disk"
        }

        nodes_map = self.map_ip_port()

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
                        f"http://{node["public_ip"]}:{node["port"]}/api/kv/"
                        for node in nodes_map
                    ]
                    self.ycsb(endpoints)

    def generate_config(self):
        logging.warning("RestKVLauncher does not implement generate_config()")

    def start(self, nodes_map):
        # Send docker compose file to remote machine
        source_files = f"{self.local_dir}/docker-compose.yml"

        node = self.nodes[0]
        if node["public_ip"] != "127.0.0.1":
            self.check_dependency(DEPENDENCIES["docker"], ">=26", node["public_ip"])
            logging.info(f"Sending docker-compose file to {node["public_ip"]}")

            mkdir_cmd = f"mkdir -p {self.remote_dir}"
            self.remote_run_cmd(node["public_ip"], mkdir_cmd)
            self.remote_rsync(node["public_ip"], source_files, self.remote_dir)

        # Start restkv
        if node == "127.0.0.1":
            run_cmd = (
                f"cd {self.local_dir}; "
                "docker compose up -d"
            )
            self.local_run_cmd(run_cmd)
        else:
            run_cmd = (
                f"cd {self.remote_dir}; "
                "docker compose up -d"
            )
            self.remote_run_cmd(node["public_ip"], run_cmd, True)

        logging.info(f"All restkv instance successfully started on {node["public_ip"]}")

    def stop(self, nodes_map):
        node = self.nodes[0]
        logging.info(f"Stopping restkv instance on {node["public_ip"]}")

        if node["public_ip"] == "127.0.0.1":
            stop_cmd = (
                f"cd {self.local_dir}; "
                "docker compose down"
            )
            self.local_run_cmd(stop_cmd)
        else:
            stop_cmd = (
                f"cd {self.remote_dir}; "
                "docker compose down"
            )
            self.remote_run_cmd(node["public_ip"], stop_cmd, False)

    def build(self):
        logging.warning("RestKVLauncher does not implement build()")

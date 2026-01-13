import logging
import re

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/fadhilkurnia/statediffbench"
COMMIT_HASH = "d18a5f54360323e181ab7fcc417f356d899c3901"

OPTIONS = [{"num": 0, "text": "Start restkv"},
           {"num": 1, "text": "Stop restkv"},
           {"num": 2, "text": "Run Benchmark"}]

SERVICE = [{"num": 1, "text": "No Proxy"},
           {"num": 2, "text": "Nginx Proxy"},
           {"num": 3, "text": "Nginx Proxy V2"}]


class RestKVLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of RestKVLauncher")
        super().__init__(nodes, ssh, client_ip, num_of_nodes, output_file)

    def map_ip_port(self):
        data = []
        for node in self.nodes:
            data.append({
                "public_ip": node["public_ip"],
                "port": 3000})

        return data

    def launch(self):
        logging.info("Launching RestKVLauncher")
        ser_num = helper.get_option(1, len(SERVICE), SERVICE)

        self.project_name = "fadhilkurnia.statediffbench"
        self.remote_dir = f"~/distro/{self.project_name}"
        self.service = SERVICE[ser_num-1]["text"]

        if self.service == "No Proxy":
            self.docker_compose = ["docker-compose.yml"]
        elif self.service == "Nginx Proxy":
            self.docker_compose = ["docker-compose-nginx.yml"]
        elif self.service == "Nginx Proxy V2":
            self.docker_compose = [
                "docker-compose-v2-nginx.yml",
                "docker-compose-v2-restkv.yml"
            ]

        self.project_repository = REPO
        self.project_commit = COMMIT_HASH
        self.ycsb_interface = "xdn"
        self.ycsb_endpoint = "url.prefix"
        self.selected_protocol = {
            "name": f"restkv-{self.service}",
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
                    node = nodes_map[0]
                    endpoints = [f"http://{node["public_ip"]}:{node["port"]}/api/kv/"]
                    self.ycsb(endpoints)

    def generate_config(self, nodes_map):
        template_path = f"{self.local_dir}/nginx-template.conf"
        config_path = f"{self.local_dir}/nginx-v2.conf"
        restkv_addr = f"{nodes_map[1]["public_ip"]}:8080"

        try:
            with open(template_path, 'r') as f:
                content = f.read()

            pattern = r"(proxy_pass http:\/\/)(.*?)(;)"
            replacement = rf"\g<1>{restkv_addr}\g<3>"
            new_content = re.sub(pattern, replacement, content, count=1)

            if new_content == content:
                logging.error("'proxy_pass' directive not found or IP address is already correct.")
                return

            with open(config_path, 'w') as f:
                f.write(new_content)

            logging.debug(f"Successfully updated proxy_pass to: {restkv_addr}")
        except FileNotFoundError:
            logging.error(f"Config file not found at {config_path}")

    def start(self, nodes_map):
        # Send docker compose file to remote machine
        if self.service == "Nginx Proxy V2":
            self.generate_config(nodes_map)
            source_files = f"{self.local_dir}/nginx-v2.conf"
        else:
            source_files = f"{self.local_dir}/nginx.conf"

        for file in self.docker_compose:
            source_files += f" {self.local_dir}/{file}"

        # Check dependency, send files to remote
        for node in self.nodes:
            if node["public_ip"] != "127.0.0.1":
                self.check_dependency(DEPENDENCIES["docker"], ">=26", node["public_ip"])
                logging.info(f"Sending docker-compose file to {node["public_ip"]}")

                mkdir_cmd = f"mkdir -p {self.remote_dir}"
                self.remote_run_cmd(node["public_ip"], mkdir_cmd)
                self.remote_rsync(node["public_ip"], source_files, self.remote_dir)

        # Start restkv
        if self.service == "Nginx Proxy V2":
            restkv_node = nodes_map[1]
            nginx_node = nodes_map[0]
            print("NGINX PROXY V2")

            if restkv_node == "127.0.0.1":
                run_cmd = (
                    f"cd {self.local_dir}; "
                    f"docker compose -f {self.docker_compose[1]} up -d "
                )
                self.local_run_cmd(run_cmd)
            else:
                run_cmd = (
                    f"cd {self.remote_dir}; "
                    f"docker compose -f {self.docker_compose[1]} up -d "
                )
                self.remote_run_cmd(restkv_node["public_ip"], run_cmd, True)

            if nginx_node == "127.0.0.1":
                run_cmd = (
                    f"cd {self.local_dir}; "
                    f"docker compose -f {self.docker_compose[0]} up -d "
                )
                self.local_run_cmd(run_cmd)
            else:
                run_cmd = (
                    f"cd {self.remote_dir}; "
                    f"docker compose -f {self.docker_compose[0]} up -d "
                )
                self.remote_run_cmd(nginx_node["public_ip"], run_cmd, True)
        else:
            if node == "127.0.0.1":
                run_cmd = (
                    f"cd {self.local_dir}; "
                    f"docker compose -f {self.docker_compose[0]} up -d"
                )
                self.local_run_cmd(run_cmd)
            else:
                run_cmd = (
                    f"cd {self.remote_dir}; "
                    f"docker compose -f {self.docker_compose[0]} up -d "
                )
                self.remote_run_cmd(node["public_ip"], run_cmd, True)

        logging.info(f"All restkv instance successfully started on {node["public_ip"]}")

    def stop(self, nodes_map):
        node = self.nodes[0]
        logging.info(f"Stopping restkv instance on {node["public_ip"]}")

        if self.service == "Nginx Proxy V2":
            restkv_node = nodes_map[1]["public_ip"]
            nginx_node = nodes_map[0]["public_ip"]

            if restkv_node == "127.0.0.1":
                run_cmd = (
                    f"cd {self.local_dir}; "
                    f"docker compose -f {self.docker_compose[1]} down "
                )
                self.local_run_cmd(run_cmd)
            else:
                run_cmd = (
                    f"cd {self.remote_dir}; "
                    f"docker compose -f {self.docker_compose[1]} down "
                )
                self.remote_run_cmd(restkv_node, run_cmd, True)

            if nginx_node == "127.0.0.1":
                run_cmd = (
                    f"cd {self.local_dir}; "
                    f"docker compose -f {self.docker_compose[0]} down "
                )
                self.local_run_cmd(run_cmd)
            else:
                run_cmd = (
                    f"cd {self.remote_dir}; "
                    f"docker compose -f {self.docker_compose[0]} down "
                )
                self.remote_run_cmd(nginx_node, run_cmd, True)

            self.local_run_cmd(f"cd {self.local_dir}; rm nginx-v2.conf")
        else:
            if node["public_ip"] == "127.0.0.1":
                stop_cmd = (
                    f"cd {self.local_dir}; "
                    f"docker compose -f {self.docker_compose} down"
                )
                self.local_run_cmd(stop_cmd)
            else:
                stop_cmd = (
                    f"cd {self.remote_dir}; "
                    f"docker compose -f {self.docker_compose} down"
                )
                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

    def build(self):
        logging.warning("RestKVLauncher does not implement build()")

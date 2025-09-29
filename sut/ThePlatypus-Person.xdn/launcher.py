import logging
import os
import subprocess
import time

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/ThePlatypus-Person/xdn.git"
COMMIT_HASH = "fbd416c60403fbaeddddf6909ff28d0d0abe9542"

FUSE_REPO = "https://github.com/ThePlatypus-Person/fuse_rust.git"
FUSE_COMMIT_HASH = "6c4007998d7da0cf7eaa6bee3c0d0c01d606f17d"

OPTIONS = [{"num": 0, "text": "Start XDN"},
           {"num": 1, "text": "Stop XDN"},
           {"num": 2, "text": "Run Benchmark"}]

'''
SERVICE_TYPE = [{"num": 1, "text": "deterministic"},
                {"num": 2, "text": "non-deterministic"}]
'''


class XdnLauncher(Launcher):
    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        logging.info("Creating instance of XdnLauncher")
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
                ip_map[public_ip] = 2000
                ip_map["client_port"] = 2300
            else:
                ip_map[public_ip] += 1
                ip_map["client_port"] += 1

            data.append({"public_ip": public_ip,
                         "private_ip": private_ip,
                         "port": ip_map[public_ip],
                         "client_port": ip_map["client_port"]})
        return data

    def launch(self):
        logging.info("Launching XdnLauncher")

        self.project_name = "ThePlatypus-Person.xdn"
        self.remote_dir = f"~/distro/{self.project_name}"
        self.project_repository = REPO
        self.project_commit = COMMIT_HASH
        self.ycsb_interface = "xdn"
        self.ycsb_endpoint = "xdn.restkv.endpoint"
        self.selected_protocol = {
            "name": "xdn",
            "language": "Java",
            "consistency": "Linearizability + Primary Integrity",
            "persistency": "On-Disk"
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
                        f"http://{node["public_ip"]}:{node["client_port"]}"
                        for node in nodes_map
                    ]
                    self.ycsb(endpoints)

    def generate_config(self, nodes_map):
        logging.info("Generating config.properties file")
        config = []
        with open(f"{self.local_dir}/template.properties", 'r') as file:
            for line in file:
                config.append(line.strip())

            config.append(f"DEFAULT_NUM_REPLICAS={self.num_of_nodes}")

            for i, node in enumerate(nodes_map):
                config.append(f"active.AR{i}={node["private_ip"]}:{node["port"]}")

            reconf = nodes_map[0]
            config.append(f"reconfigurator.RC0={reconf["private_ip"]}:{reconf["port"] + 1000}")

            if nodes_map[0]["private_ip"] == "127.0.0.1" and nodes_map[0]["public_ip"] == "127.0.0.1":
                config.append(f"SSH_KEY_PATH={self.ssh_key}")
            else:
                config.append(f"SSH_KEY_PATH=~/distro/ThePlatypus-Person.xdn/{self.ssh_filename}")

        config_path = f"{self.local_dir}/config.properties"
        with open(config_path, "w") as f:
            for line in config:
                f.write(f"{line}\n")

        return config_path

    def start(self, nodes_map):
        config_path = self.generate_config(nodes_map)

        # Send binary to remote machine
        build_dir = f"{self.repo_dir_path}/build"
        jar_dir = f"{self.repo_dir_path}/jars"
        conf_dir = f"{self.repo_dir_path}/conf"
        fuselog = f"{self.local_dir}/fuselog"
        fuselog_apply = f"{self.local_dir}/fuselog-apply"

        source_files = f"{build_dir} {jar_dir} {conf_dir} {config_path} {fuselog} {fuselog_apply}"

        for node in self.nodes:
            if node["public_ip"] == "127.0.0.1":
                continue

            self.check_dependency(DEPENDENCIES["java"], ">=21", node["public_ip"])
            self.check_dependency(DEPENDENCIES["fuse"], ">=3.10", node["public_ip"])
            self.check_dependency(DEPENDENCIES["docker"], ">=26", node["public_ip"])
            logging.info(f"Sending XDN binaries to {node["public_ip"]}")

            mkdir_cmd = f"mkdir -p {self.remote_dir}"
            self.remote_run_cmd(node["public_ip"], mkdir_cmd)
            self.remote_rsync(node["public_ip"], source_files, self.remote_dir)
            cp_cmd = (
                f"sudo cp {self.remote_dir}/fuselog /usr/local/bin/fuselog && "
                f"sudo cp {self.remote_dir}/fuselog-apply /usr/local/bin/fuselog-apply"
            )
            self.remote_run_cmd(node["public_ip"], cp_cmd, True)

        jar_files = []
        for item in os.listdir(f"{self.repo_dir_path}/jars"):
            jar_files.append(item)

        # Start instances
        for i, node in enumerate(nodes_map):
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                local_jars = [f"{self.repo_dir_path}/jars/{item}" for item in jar_files]
                jars = ":".join(local_jars)
                run_cmd = (
                    f"cd {self.repo_dir_path}; "
                    f"nohup java -DgigapaxosConfig={config_path} -ea "
                    "-Djavax.net.ssl.keyStorePassword=qwerty "
                    "-Djavax.net.ssl.trustStorePassword=qwerty "
                    "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
                    "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
                    "-Djava.util.logging.config.file=conf/logging.properties "
                    "-Dlog4j.configuration=conf/log4j.properties "
                    "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
                    f"-cp build/classes:{jars} "
                    f"edu.umass.cs.reconfiguration.ReconfigurableNode AR{i} "
                    f"> node_{i}.log 2>&1 &"
                )
                self.local_run_cmd(run_cmd)
            else:
                remote_jars = [f"~/distro/{self.project_name}/jars/{item}" for item in jar_files]
                jars = ":".join(remote_jars)
                run_cmd = (
                    f"cd {remote_dir}; "
                    "nohup java -DgigapaxosConfig=config.properties -ea "
                    "-Djavax.net.ssl.keyStorePassword=qwerty "
                    "-Djavax.net.ssl.trustStorePassword=qwerty "
                    "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
                    "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
                    "-Djava.util.logging.config.file=conf/logging.properties "
                    "-Dlog4j.configuration=conf/log4j.properties "
                    "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
                    f"-cp build/classes:{jars} "
                    f"edu.umass.cs.reconfiguration.ReconfigurableNode AR{i} "
                    f"> node_{i}.log 2>&1 &"
                )
                self.remote_run_cmd(node["public_ip"], run_cmd, True)

        # Start Reconfigurator
        if nodes_map[0]["private_ip"] == "127.0.0.1" and nodes_map[0]["public_ip"] == "127.0.0.1":
            local_jars = [f"{self.repo_dir_path}/jars/{item}" for item in jar_files]
            jars = ":".join(local_jars)
            run_cmd = (
                f"cd {self.repo_dir_path}; "
                f"nohup java -DgigapaxosConfig={config_path} -ea "
                "-Djavax.net.ssl.keyStorePassword=qwerty "
                "-Djavax.net.ssl.trustStorePassword=qwerty "
                "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
                "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
                "-Djava.util.logging.config.file=conf/logging.properties "
                "-Dlog4j.configuration=conf/log4j.properties "
                "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
                f"-cp build/classes:{jars} "
                f"edu.umass.cs.reconfiguration.ReconfigurableNode RC0 "
                f"> reconf_{i}.log 2>&1 &"
            )
            self.local_run_cmd(run_cmd)
        else:
            remote_jars = [f"~/distro/{self.project_name}/jars/{item}" for item in jar_files]
            jars = ":".join(remote_jars)
            run_cmd = (
                f"cd {remote_dir}; "
                "nohup java -DgigapaxosConfig=config.properties -ea "
                "-Djavax.net.ssl.keyStorePassword=qwerty "
                "-Djavax.net.ssl.trustStorePassword=qwerty "
                "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
                "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
                "-Djava.util.logging.config.file=conf/logging.properties "
                "-Dlog4j.configuration=conf/log4j.properties "
                "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
                f"-cp build/classes:{jars} "
                f"edu.umass.cs.reconfiguration.ReconfigurableNode RC0 "
                f"> reconf_{i}.log 2>&1 &"
            )
            self.remote_run_cmd(nodes_map[0]["public_ip"], run_cmd, True)

        time.sleep(15)
        env = os.environ.copy()
        env["XDN_CONTROL_PLANE"] = nodes_map[0]["public_ip"]
        yaml_path = f"{self.local_dir}/restkv-nd.yaml"
        cmd_service = [f"{self.repo_dir_path}/bin/xdn", "launch", "restkv", f"--file={yaml_path}"]
        subprocess.run(cmd_service, text=True, env=env)
        logging.info("All XDN instances successfully started")

    def stop(self, nodes_map):
        core_cleanup_cmd = (
            "pids=$(ps aux | grep 'edu.umass.cs.reconfiguration.ReconfigurableNode' | grep -v grep | awk '{{print $2}}'); "
            "for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "

            "container_ids=$(docker ps -a -q --filter 'name=c0.e0.restkv.ar*.xdn.io'); "
            "if [ -n \"$container_ids\" ]; then "
            "  echo \"Stopping and removing containers: $container_ids\"; "
            "  docker stop $container_ids; "
            "  docker rm -f $container_ids; "
            "fi; "

            "docker network prune --force;"

            "for mountpoint in $(find /tmp/xdn/state/fuselog/ -type d -name 'ar*' | xargs -I{{}} echo {{}}/mnt/restkv/e0); do "
            "  echo \"Unmounting $mountpoint\"; fusermount -u $mountpoint || true; done; "
            "rm -rf /tmp/xdn /tmp/gigapaxos"
        )

        for i, node in enumerate(nodes_map):
            logging.info(f"Stopping XDN instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                stop_cmd = (
                    f"{core_cleanup_cmd}; "
                    f"rm -rf {self.repo_dir_path}/node_{i}.log {self.repo_dir_path}/reconf_{i}.log"
                )
                self.local_run_cmd(stop_cmd)
            else:
                remote_dir = f"~/distro/{self.project_name}"
                remote_config = f"{remote_dir}/config.properties"
                stop_cmd = (
                    f"{core_cleanup_cmd}; "
                    f"rm -rf {remote_dir}/node_{i}.log {remote_dir}/reconf_{i}.log {remote_config}"
                )
                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

        local_config = f"{self.local_dir}/config.properties"
        rm_config_cmd = f"rm {local_config}"
        self.local_run_cmd(rm_config_cmd)
        logging.info("All XDN instances successfully stopped")

    def build(self):
        logging.info("Checking if protocol executables already exists...")
        self.check_dependency(DEPENDENCIES["java"], ">=21")
        self.check_dependency(DEPENDENCIES["fuse"], ">=3.10")
        self.check_dependency(DEPENDENCIES["docker"], ">=26")

        path, matching_commit = self.ensure_repo_exists(self.local_dir,
                                                        self.project_repository,
                                                        COMMIT_HASH)
        self.repo_dir_path = path

        jars = ["gigapaxos-1.0.10.jar",
                "gigapaxos-nio-src.jar",
                "nio-1.2.1.jar"]

        binary_exists = True
        for jar in jars:
            jar_path = f"{path}/jars/{jar}"
            if not os.path.exists(jar_path) or not os.path.isfile(jar_path):
                binary_exists = False

        build_path = f"{path}/build"
        if not os.path.exists(build_path) or not os.path.isdir(build_path):
            binary_exists = False

        # Rebuild if binary doesn't exist
        # or if repo commit doesn't match the default commit hash
        if not binary_exists or not matching_commit:
            logging.info("Building XDN binary...")
            build_cmd = (
                f"cd {path} && "
                "./bin/build_xdn_jar.sh",
                "./bin/build_xdn_cli.sh"
            )
            self.local_run_cmd(build_cmd)

        # Check Fuselog
        self.check_dependency(DEPENDENCIES["rust"], ">=1.87")
        self.check_dependency(DEPENDENCIES["cargo"], ">=1.87")
        path, matching_commit = self.ensure_repo_exists(self.local_dir,
                                                        FUSE_REPO,
                                                        FUSE_COMMIT_HASH)
        fuse_binaries = ["fuselog", "fuselog-apply"]
        binary_exists = True
        for bin in fuse_binaries:
            bin_path = f"{self.local_dir}/{bin}"
            if not os.path.exists(bin_path) or not os.path.isfile(bin_path):
                binary_exists = False

        if not binary_exists or not matching_commit:
            logging.info("Building Fuselog binary...")
            build_cmd = (
                f"cd {path}/fuselog_core && cargo build --release && "
                f"cd {path}/fuselog_apply && cargo build --release && "
                f"mv {path}/target/release/fuselog_core {self.local_dir}/fuselog && "
                f"mv {path}/target/release/fuselog_apply {self.local_dir}/fuselog-apply"
            )
            self.local_run_cmd(build_cmd)

        binary_exists_path = True
        for bin in fuse_binaries:
            bin_path = f"/usr/local/bin/{bin}"
            if not os.path.exists(bin_path) or not os.path.isfile(bin_path):
                binary_exists_path = False

        if not binary_exists_path:
            copy_cmd = (
                f"sudo cp {self.local_dir}/fuselog /usr/local/bin/fuselog && "
                f"sudo cp {self.local_dir}/fuselog-apply /usr/local/bin/fuselog-apply"
            )
            self.local_run_cmd(copy_cmd)

        logging.info("XDN & Fuselog build complete")

import logging
import os
import subprocess
import time
import sys

from sut.abstract import Launcher
from src.utils import helper
from src.utils.dependency_probes import DEPENDENCIES

REPO = "https://github.com/fadhilkurnia/xdn.git"
COMMIT_HASH = "dd39bccdf150e3bea94d95131994df6a81a5fbe0"

OPTIONS = [{"num": 0, "text": "Start XDN"},
           {"num": 1, "text": "Stop XDN"},
           {"num": 2, "text": "Run Benchmark"}]

SERVICE_TYPE = [{"num": 1, "text": "deterministic"},
                {"num": 2, "text": "non-deterministic"}]

CONSISTENCY = [{"num": 1, "text": "Linearizability",    "deterministic": "restkv-d-linearizability.yaml",   "non-deterministic": "bookcatalog-nd.my.yaml"},
               {"num": 2, "text": "Sequential",         "deterministic": "restkv-d-sequential.yaml",        "non-deterministic": None},
               {"num": 3, "text": "Causal",             "deterministic": "restkv-d-causal.yaml",            "non-deterministic": None},
               {"num": 4, "text": "PRAM",               "deterministic": "restkv-d-pram.yaml",              "non-deterministic": None},
               {"num": 5, "text": "Eventual",           "deterministic": "restkv-d-eventual.yaml",          "non-deterministic": None}]


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
        prot_num = helper.get_option(1, len(SERVICE_TYPE), SERVICE_TYPE, "\nSelect Service Type:")
        cons_num = helper.get_option(1, len(CONSISTENCY), CONSISTENCY, "\nSelect a Consistency Model:")

        service_name = SERVICE_TYPE[prot_num-1]["text"]
        consistency = CONSISTENCY[cons_num-1]

        self.project_name = "fadhilkurnia.xdn"
        self.remote_dir = f"~/distro/{self.project_name}"
        self.project_repository = REPO
        self.project_commit = COMMIT_HASH
        self.ycsb_interface = "xdn"
        self.ycsb_endpoint = "url.prefix"
        self.launch_filename = consistency[service_name]
        self.selected_protocol = {
            "name": f"xdn-{service_name}",
            "language": "Java",
            "consistency": f"{consistency["text"]}{" + Primary Integrity" if service_name == "non-deterministic" else ""}",
            "persistency": "On-Disk"
        }

        if self.launch_filename is None:
            logging.error(f"{self.project_name} currently does not support {service_name} {consistency["text"]} application")
            sys.exit()

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
                        f"http://{node["public_ip"]}:{node["client_port"]}/api/kv/"
                        for node in nodes_map
                    ]
                    self.ycsb(endpoints, "headers='XDN restkv'")

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
                f"sudo rm -rf /usr/local/bin/fuselog && "
                f"sudo rm -rf /usr/local/bin/fuselog-apply && "
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
                remote_jars = [f"$HOME/distro/{self.project_name}/jars/{item}" for item in jar_files]
                jars = ":".join(remote_jars)
                run_cmd = (
                    f"cd {self.remote_dir}; "
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
                f"> reconf_0.log 2>&1 &"
            )
            self.local_run_cmd(run_cmd)
        else:
            remote_jars = [f"$HOME/distro/{self.project_name}/jars/{item}" for item in jar_files]
            jars = ":".join(remote_jars)
            run_cmd = (
                f"cd {self.remote_dir}; "
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
                f"> reconf_0.log 2>&1 &"
            )
            self.remote_run_cmd(nodes_map[0]["public_ip"], run_cmd, True)

        time.sleep(15)
        env = os.environ.copy()
        env["XDN_CONTROL_PLANE"] = nodes_map[0]["public_ip"]
        yaml_path = f"{self.local_dir}/{self.launch_filename}"
        cmd_service = [f"{self.repo_dir_path}/bin/xdn", "launch", "bookcatalog-nd-app", f"--file={yaml_path}"]
        subprocess.run(cmd_service, text=True, env=env)
        logging.info("All XDN instances successfully started")

    def stop(self, nodes_map):
        for i, node in enumerate(nodes_map):
            core_cleanup_cmd = (
                "pids=$(ps aux | grep 'edu.umass.cs.reconfiguration.ReconfigurableNode' | grep -v grep | awk '{{print $2}}'); "
                "for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "

                f"container_ids=$(docker ps -a -q --filter 'name=c0.e0.restkv.ar{i}.xdn.io'); "
                "if [ -n \"$container_ids\" ]; then "
                "  echo \"Stopping and removing containers: $container_ids\"; "
                "  docker stop $container_ids; "
                "  docker rm -f $container_ids; "
                "fi; "

                "docker network prune --force;"

                f"for mountpoint in $(find /tmp/xdn/state/fuselog/ -type d -name 'ar{i}' | xargs -I{{}} echo {{}}/mnt/restkv/e0); do "
                "  echo \"Unmounting $mountpoint\"; fusermount -u $mountpoint || true; done; "
                "rm -rf /tmp/xdn /tmp/gigapaxos || true"
            )

            logging.info(f"Stopping XDN instance on {node["public_ip"]}")
            if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
                stop_cmd = (
                    f"{core_cleanup_cmd}; "
                    f"rm -rf {self.repo_dir_path}/node_{i}.log {self.repo_dir_path}/reconf_{i}.log || true"
                )
                self.local_run_cmd(stop_cmd)
            else:
                remote_config = f"{self.remote_dir}/config.properties"
                stop_cmd = (
                    f"{core_cleanup_cmd}; "
                    f"rm -rf {self.remote_dir}/node_{i}.log {self.remote_dir}/reconf_{i}.log {remote_config} || true"
                )
                self.remote_run_cmd(node["public_ip"], stop_cmd, False)

        local_config = f"{self.local_dir}/config.properties"
        rm_config_cmd = f"rm {local_config} || true "
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

        cli_binary = f"{path}/bin/xdn"
        if not os.path.exists(cli_binary):
            binary_exists = False

        # Rebuild if binary doesn't exist
        # or if repo commit doesn't match the default commit hash
        if not binary_exists or not matching_commit:
            logging.info("Building XDN binary...")
            build_cmd = (
                f"cd {path} && "
                "./bin/build_xdn_jar.sh"
            )
            self.local_run_cmd(build_cmd)

        # Build CLI
        build_cmd = (
            f"cd {path} && "
            "./bin/build_xdn_cli.sh"
        )
        self.local_run_cmd(build_cmd)

        # Check Fuselog
        fuse_binaries = ["fuselog", "fuselog-apply"]
        copy_cmd = (
            f"sudo cp {self.local_dir}/fuselog /usr/local/bin/fuselog && "
            f"sudo cp {self.local_dir}/fuselog-apply /usr/local/bin/fuselog-apply"
        )
        self.local_run_cmd(copy_cmd)
        logging.info("XDN build complete")

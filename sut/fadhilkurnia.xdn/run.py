from pathlib import Path
import subprocess
import threading
import os
import re
import time

from src.utils import helper

CURR_DIR = Path("./sut/fadhilkurnia.xdn")
ROOT_PATH = Path(".")
XDN_BIN = CURR_DIR / "xdn" / "bin"
#START_CONFIG = CURR_DIR / "xdn" / "eval" / "static" / "gigapaxos.xdn.3way.local.properties"
TEMPLATE_CONFIG = CURR_DIR / "xdn" / "conf" / "template.properties"

OPTIONS = [{"num": 0, "text": "Start XDN"},
           {"num": 1, "text": "Stop XDN"},
           {"num": 2, "text": "Run Benchmark"}]

'''
SERVICE_TYPE = [{"num": 1, "text": "deterministic"},
                {"num": 2, "text": "non-deterministic"},
'''


class TriggerWatcher:
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.thread = None
        self._lock = threading.Lock()
        self._trigger_text = None
        self._trigger_event = threading.Event()

    def start(self):
        self.process = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        self.thread = threading.Thread(target=self._watch_output, daemon=True)
        self.thread.start()

    def _watch_output(self):
        for line in self.process.stdout:
            print(f"[OUTPUT] {line.strip()}")
            with self._lock:
                if self._trigger_text and self._trigger_text in line:
                    self._trigger_event.set()

    def wait_for(self, trigger_text, timeout=None):
        with self._lock:
            self._trigger_text = trigger_text
            self._trigger_event.clear()
        print(f"Waiting for: {trigger_text}")
        found = self._trigger_event.wait(timeout=timeout)
        if not found:
            raise TimeoutError(f"Timeout waiting for: {trigger_text}")
        print(f"Trigger '{trigger_text}' detected")

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.wait()


def run_command(cmd) -> None:
    """
    Runs a command in a new subprocess which will be tracked inside jobs list.

    :param cmd: Command line arguments
    :type cmd: str[]
    """
    proc = subprocess.Popen(cmd)
    proc.wait()


def main(run_ycsb, nodes, ssh) -> None:
    """
    Main function called by the root main.py script.
    Gives user a choice to start/stop instances
    and to run the YCSB benchmark on the instance.

    :param run_ycsb: Function to run YCSB benchmark. Takes in protocol
                     data {name, language} and YCSB interface name as argument.
    :type run_ycsb: Callable[dict[str, str], str]
    :param nodes: List of node IP
    :type nodes: dict[str, str, str, str, str]
    """
    node_data = map_ip_port(nodes)
    print("XDN IP-Port Map:")
    for item in node_data:
        print(item)

    config = None
    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start(XDN_BIN, node_data, ssh)
            case 1:
                stop(XDN_BIN, node_data, ssh)
            case 2:
                endpoints = [f"http://{node["public_ip"]}:{node["client_port"]}" for node in node_data]
                print("endpoint list:", endpoints)
                run_ycsb({
                    "name": "xdn",
                    "language": "Java",
                    "consistency": "Linearizability + Primary Integrity",
                    "persistency": "On-Disk"
                }, "xdn", endpoints, "xdn.restkv.endpoint", ssh)


def start(path, nodes, ssh) -> None:
    """
    Runs the XDN instances with the specified protocol in different
    threads concurrently. Currently only supports local startup.

    :param path: Path to bin/ directory inside the xdn repository.
    :type path: Path
    :param config: Path to config file to run XDN startup script
    :type config: Path
    """
    config = []
    user = ssh["username"]
    with open(CURR_DIR / "template.properties", 'r') as file:
        for line in file:
            config.append(line.strip())

        config.append(f"DEFAULT_NUM_REPLICAS={len(nodes)}")

        for i, node in enumerate(nodes):
            config.append(f"active.AR{i}={node["private_ip"]}:{node["port"]}")

        reconf = nodes[0]
        config.append(f"reconfigurator.RC0={reconf["private_ip"]}:{reconf["port"] + 1000}")

        if nodes[0]["private_ip"] == "127.0.0.1" and nodes[0]["public_ip"] == "127.0.0.1":
            config.append(f"SSH_KEY_PATH={ROOT_PATH.resolve()}/{ssh["filename"]}")
        else:
            config.append(f"SSH_KEY_PATH=/home/{user}/fadhilkurnia.xdn/{ssh["filename"]}")

    print("config:")
    for line in config:
        print(line)

    config_path = CURR_DIR / "config.properties"
    with open(config_path, "w") as f:
        for line in config:
            f.write(f"{line}\n")

    for i, node in enumerate(nodes):
        if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
            jar_paths_str = subprocess.check_output(
                ['find', 'jars', '-name', '*.jar'],
                cwd=f"{CURR_DIR.resolve()}/xdn",
                text=True,
                stderr=subprocess.PIPE
            ).strip()
            classpath_jars = jar_paths_str.replace('\n', ':')

            run_cmd = (
                f"cd {CURR_DIR}/xdn; "
                "nohup java -DgigapaxosConfig=../config.properties -ea "
                "-Djavax.net.ssl.keyStorePassword=qwerty "
                "-Djavax.net.ssl.trustStorePassword=qwerty "
                "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
                "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
                "-Djava.util.logging.config.file=conf/logging.properties "
                "-Dlog4j.configuration=conf/log4j.properties "
                "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
                f"-cp 'build/classes:{classpath_jars}' "
                f"edu.umass.cs.reconfiguration.ReconfigurableNode AR{i} "
                f"> node_{i}.log 2>&1 &"
            )

            print("Running command:", run_cmd)
            subprocess.run(run_cmd, check=True, shell=True)
        else:
            host = node["public_ip"]
            build_dir = CURR_DIR / "xdn" / "build"
            jar_dir = CURR_DIR / "xdn" / "jars"
            conf_dir = CURR_DIR / "xdn" / "conf"
            remote_base = f"/home/{user}/fadhilkurnia.xdn"

            rsync_cmd1 = (
                f"rsync --force -zaLP "
                f"--rsync-path=\"mkdir -p {remote_base}/conf && rsync\" "
                f"-e \"ssh -i {ssh['key']}\" "
                f"{build_dir.resolve()} {jar_dir.resolve()} {conf_dir.resolve()} "
                f"{config_path.resolve()} {ssh["key"].resolve()} {user}@{host}:{remote_base}"
            )

            print("Running command:", rsync_cmd1)
            subprocess.run(rsync_cmd1, check=True, shell=True)

            fuselog_apply = CURR_DIR / "fuse_rust" / "target" / "release" / "fuselog_apply"
            fuselog_core = CURR_DIR / "fuse_rust" / "target" / "release" / "fuselog_core"

            rsync_cmd2 = (
                f"rsync --force -zaLP "
                f"-e \"ssh -i {ssh['key']}\" "
                f"{fuselog_core.resolve()} "
                "--rsync-path=\"sudo rsync\" "
                f"{user}@{host}:/usr/local/bin/fuselog"
            )
            print("Running command:", rsync_cmd2)
            subprocess.run(rsync_cmd2, check=True, shell=True)

            rsync_cmd3 = (
                f"rsync --force -zaLP "
                f"-e \"ssh -i {ssh['key']}\" "
                f"{fuselog_apply.resolve()} "
                "--rsync-path=\"sudo rsync\" "
                f"{user}@{host}:/usr/local/bin/fuselog-apply"
            )
            print("Running command:", rsync_cmd3)
            subprocess.run(rsync_cmd3, check=True, shell=True)

            run_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"\"cd /home/{user}/fadhilkurnia.xdn; "
                "nohup java -DgigapaxosConfig=config.properties -ea "
                "-Djavax.net.ssl.keyStorePassword=qwerty "
                "-Djavax.net.ssl.trustStorePassword=qwerty "
                "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
                "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
                "-Djava.util.logging.config.file=conf/logging.properties "
                "-Dlog4j.configuration=conf/log4j.properties "
                "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
                "-cp \\\"build/classes:\\$(echo jars/*.jar | tr ' ' ':')\\\" "
                f"edu.umass.cs.reconfiguration.ReconfigurableNode AR{i} "
                f"> node_{i}.log 2>&1 &\""
            )

            print("Running command:", run_cmd)
            subprocess.run(run_cmd, check=True, shell=True)

    # Reconfigurator
    if nodes[0]["private_ip"] == "127.0.0.1" and nodes[0]["public_ip"] == "127.0.0.1":
        # Starts reconfigurator instance (local)
        jar_paths_str = subprocess.check_output(
            ['find', 'jars', '-name', '*.jar'],
            cwd=f"{CURR_DIR.resolve()}/xdn",
            text=True,
            stderr=subprocess.PIPE
        ).strip()
        classpath_jars = jar_paths_str.replace('\n', ':')

        run_cmd = (
            f"cd {CURR_DIR}/xdn; "
            "nohup java -DgigapaxosConfig=../config.properties -ea "
            "-Djavax.net.ssl.keyStorePassword=qwerty "
            "-Djavax.net.ssl.trustStorePassword=qwerty "
            "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
            "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
            "-Djava.util.logging.config.file=conf/logging.properties "
            "-Dlog4j.configuration=conf/log4j.properties "
            "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
            f"-cp 'build/classes:{classpath_jars}' "
            f"edu.umass.cs.reconfiguration.ReconfigurableNode RC0 "
            f"> reconf_{i}.log 2>&1 &"
        )
        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)
    else:
        # Starts reconfigurator instance (remote use ssh)
        run_cmd = (
            f"ssh -i {ssh['key']} {user}@{nodes[0]["public_ip"]} "
            f"\"cd /home/{user}/fadhilkurnia.xdn; "
            "nohup java -DgigapaxosConfig=config.properties -ea "
            "-Djavax.net.ssl.keyStorePassword=qwerty "
            "-Djavax.net.ssl.trustStorePassword=qwerty "
            "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
            "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
            "-Djava.util.logging.config.file=conf/logging.properties "
            "-Dlog4j.configuration=conf/log4j.properties "
            "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
            "-cp \\\"build/classes:\\$(echo jars/*.jar | tr ' ' ':')\\\" "
            f"edu.umass.cs.reconfiguration.ReconfigurableNode RC0 "
            f"> reconf_{i}.log 2>&1 &\""
        )
        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)

    time.sleep(15)

    # Launch restkv in XDN
    env = os.environ.copy()
    env["XDN_CONTROL_PLANE"] = nodes[0]["public_ip"]
    yaml_path = CURR_DIR / "restkv-nd.yaml"
    cmd_service = ["xdn", "launch", "restkv", f"--file={yaml_path}"]
    subprocess.run(cmd_service, text=True, env=env)

    print("restkv service has started in XDN")


def stop(bin_path, nodes, ssh) -> None:
    """
    Terminates all running instances of paxi that are still recorded inside
    the jobs list, then removes all the logfiles created by the instances.
    """
    user = ssh["username"]

    if nodes[0]["private_ip"] == "127.0.0.1" and nodes[0]["public_ip"] == "127.0.0.1":
        cmd = (
            f"pids=$(ps aux | grep 'edu.umass.cs.reconfiguration.ReconfigurableNode' | grep -v grep | awk '{{print $2}}'); "
            f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "

            f"container_ids=$(docker ps -a -q --filter 'name=c0.e0.restkv.ar*.xdn.io'); "
            f"if [ -n \"$container_ids\" ]; then "
            f"  echo \"Stopping and removing containers: $container_ids\"; "
            f"  docker stop $container_ids; "
            f"  docker rm -f $container_ids; "
            f"fi; "

            f"docker network prune --force; "

            f"for mountpoint in $(find /tmp/xdn/state/fuselog/ -type d -name 'ar*' | xargs -I{{}} echo {{}}/mnt/restkv/e0); do "
            f"  echo \"Unmounting $mountpoint\"; fusermount -u $mountpoint || true; done; "

            f"rm -rf /tmp/xdn /tmp/gigapaxos;"
        )

        print("Running command:", cmd)
        subprocess.run(cmd, check=True, shell=True)
        config = CURR_DIR / "xdn"
        os.system(f"rm -rf {config.resolve()}/node_*.log {config.resolve()}/reconf_*.log")

    else:
        for i, node in enumerate(nodes):
            host = node["public_ip"]
            
            remote_command = (
                f"pids=$(ps aux | grep 'edu.umass.cs.reconfiguration.ReconfigurableNode' | grep -v grep | awk '{{print $2}}'); "
                f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                f"docker remove -f c0.e0.restkv.ar{i}.xdn.io; "
                f"docker network prune --force; "
                f"fusermount -u /tmp/xdn/state/fuselog/ar{i}/mnt/restkv/e0; "
                f"rm -rf /tmp/xdn /tmp/gigapaxos;"
            )

            cmd = ["ssh", "-i", str(ssh["key"]), f"{user}@{host}",
                   remote_command]
            print("Running command:", " ".join(cmd))
            subprocess.run(cmd)

    print("xdn removal & cleanup completed")

    return
    ########
    start_script = bin_path / "gpServer.sh"

    cmd_xdn = [start_script, "-DgigapaxosConfig=123123", "forceclear", "all"]
    subprocess.run(cmd_xdn, text=True)

    subprocess.run(["docker", "network", "prune", "--force"], text=True)

    os.system("fusermount -u /tmp/xdn/state/fuselog/ar0/mnt/restkv/e0")
    os.system("rm -rf /tmp/gigapaxos")
    os.system("rm -rf /tmp/xdn")
    os.system("rm -rf ./output ./derby.log")
    print("XDN has stopped")


if __name__ == "__main__":
    main()


def generate_config(ori_config, new_ips):
    print("Generating modified config")
    new_actives = {key: val for key, val in new_ips.items() if "active" in key}
    new_reconfigurator = {key: val for key, val in new_ips.items()
                          if "reconfigurator" in key}

    lines = None
    with open(ori_config, "r") as f:
        lines = f.readlines()

    modified_lines = []
    for line in lines:
        match_active = re.match(r"(active\.AR\d+)=(.*)", line)
        if match_active:
            key = match_active.group(1)
            if key in new_actives:
                modified_lines.append(f"{key}={new_actives[key]}\n")
                continue

        match_reconf = re.match(r"(reconfigurator\.RC0)=(.*)", line)
        if match_reconf:
            key = match_reconf.group(1)
            modified_lines.append(f"{key}={new_reconfigurator[key]}\n")
            continue

        modified_lines.append(line)

    custom_property = CURR_DIR / "xdn" / "conf" / "custom.properties"
    with open(custom_property, "w") as f:
        f.writelines(modified_lines)

    return custom_property


def map_ip_port(nodes):
    data = []
    ip_map = {}
    for node in nodes:
        public_ip = node["public"]
        private_ip = node["private"]

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


def hello():
    print("Hello from XDN")

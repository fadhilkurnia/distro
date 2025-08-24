from pathlib import Path
import subprocess
import time
import os

from src.utils import helper

CURR_DIR = Path("./sut/apache.zookeeper")
ZK_BIN = CURR_DIR / "apache-zookeeper" / "bin"

OPTIONS = [{"num": 0, "text": "Start Zookeeper"},
           {"num": 1, "text": "Stop Zookeeper"},
           {"num": 2, "text": "Run Benchmark"}]


def main(run_ycsb, nodes, ssh) -> None:
    """
    Main function called by the root main.py script.
    Gives user a choice to start/stop instances
    and to run the YCSB benchmark on the instance.

    :param run_ycsb: Function to run YCSB benchmark. Takes in protocol
                     data {name, language} and YCSB interface name as argument.
    :type run_ycsb: Callable[dict[str, str], str]
    """
    node_data = map_ip_port(nodes)
    print("Zookeeper IP-Port Map:")
    for item in node_data:
        print(item)

    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start_zk(ZK_BIN, node_data, ssh)
            case 1:
                stop_zk(ZK_BIN, node_data, ssh)
            case 2:
                endpoints = [f"{node["public_ip"]}:{node["client"]}" for node in node_data]
                print("endpoint list:", endpoints)
                run_ycsb({"name": "zab", "language": "Java"}, "zookeeper", endpoints, "zookeeper.connectString")


def start_zk(path, node_data, ssh) -> None:
    """
    Runs the instances with the specified protocol in different
    threads concurrently. Currently only supports local startup.

    :param path: Path to bin/ directory inside the zookeeper repository.
    :type path: Path
    """
    # Generate custom config
    template_config = []
    with open(CURR_DIR / "template.cfg", 'r') as file:
        for line in file:
            template_config.append(line.strip())

        for i, node in enumerate(node_data):
            template_config.append(f"server.{i+1}={node["private_ip"]}:{node['peer']}:{node['election']}")

    print("template_config:", template_config)

    user = ssh["username"]
    zk_url = "https://dlcdn.apache.org/zookeeper/zookeeper-3.9.3/apache-zookeeper-3.9.3-bin.tar.gz"

    for i, node in enumerate(node_data):
        host = node["public_ip"]
        data_path = f"cluster/node{i+1}/data"
        os.makedirs(CURR_DIR / data_path, exist_ok=True)
        myid = CURR_DIR / f"cluster/node{i+1}/data/myid"
        with open(myid, "w") as f:
            f.writelines(f"{i+1}\n")

        config = template_config.copy()

        config.append(f"dataDir=/home/{user}/zookeeper/{data_path}")
        config.append(f"clientPort={node["client"]}")

        config_path = CURR_DIR / f"cluster/node{i+1}/config.cfg"
        with open(config_path, "w") as f:
            for line in config:
                f.write(f"{line}\n")

        if node["private_ip"] == "127.0.0.1" and host == "127.0.0.1":
            # Local Execution
            local_server = path / "zkServer.sh"
            run_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'{local_server} start {config_path}'"
            )
        else:
            # Remote Execution
            local_dir = CURR_DIR / "cluster" / f"node{i+1}"
            remote_dir = f"/home/{user}/zookeeper/cluster"
            copy_cmd = [
                "rsync", "-avz",
                "-e", f"ssh -i {ssh['key']}",
                "--rsync-path", f"mkdir -p {remote_dir} && rsync",
                str(local_dir.resolve()),
                f"{user}@{host}:{remote_dir}/"
            ]

            curl_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'if [ ! -d /home/{user}/zookeeper/apache-zookeeper-3.9.3-bin ]; then "
                f"curl -L {zk_url} | tar -xz -C /home/{user}/zookeeper/; "
                f"else echo \"Zookeeper already installed. Skipping download.\"; fi'"
            )

            print("Running command:", " ".join(copy_cmd))
            subprocess.run(copy_cmd, check=True)

            print("Running command:", curl_cmd)
            subprocess.run(curl_cmd, check=True, shell=True)

            remote_bin = f"/home/{user}/zookeeper/apache-zookeeper-3.9.3-bin/bin"
            remote_server = f"{remote_bin}/zkServer.sh"
            remote_config = f"{remote_dir}/node{i+1}/config.cfg"

            run_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'{remote_server} start {remote_config}'"
            )

        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)

    print("Zookeeper instances successfully started")
    time.sleep(5)

    # Insert /benchmark for YCSB
    client = path / "zkCli.sh"
    process = subprocess.Popen(
        [client, "-server", f"{node_data[0]["public_ip"]}:2101"],
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
    print("Client created /benchmark:\n", stdout)


def stop_zk(path, node_data, ssh) -> None:
    """
    Terminates all running instances of zookeeper.

    :param path: Path to bin/ directory inside the zookeeper repository.
    :type path: Path
    """
    user = ssh["username"]
    for i, node in enumerate(node_data):
        host = node["public_ip"]

        if node["private_ip"] == "127.0.0.1" and host == "127.0.0.1":
            # Local Execution
            local_server = path / "zkServer.sh"
            config_path = CURR_DIR / f"cluster/node{i+1}/config.cfg"
            stop_cmd = f"{local_server} stop {config_path}"
            print("Running command:", stop_cmd)
            subprocess.run(stop_cmd, check=True, shell=True)
        else:
            # Remote Execution
            remote_dir = f"/home/{user}/zookeeper/cluster"
            remote_bin = f"/home/{user}/zookeeper/apache-zookeeper-3.9.3-bin/bin"
            remote_server = f"{remote_bin}/zkServer.sh"
            remote_config = f"{remote_dir}/node{i+1}/config.cfg"

            stop_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'{remote_server} stop {remote_config} && rm -rf {remote_dir}'"
            )
            print("Running command:", stop_cmd)
            subprocess.run(stop_cmd, check=True, shell=True)

    # Remove local config
    local_cluster_dir = CURR_DIR / "cluster"
    rm_cmd = f"rm -rf {local_cluster_dir}"
    print("Running command:", rm_cmd)
    subprocess.run(rm_cmd, check=True, shell=True)

    print("Zookeeper instances stopped")


def map_ip_port(nodes):
    data = []
    ip_map = {}
    for node in nodes:
        public_ip = node["public"]
        private_ip = node["private"]

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


if __name__ == "__main__":
    raise RuntimeError("This script is meant to be imported, not run directly")


def hello():
    print("Hello from zookeeper")

from pathlib import Path
import subprocess
import threading
import os

from src.utils import helper

CURR_DIR = Path("./sut/tikv.tikv")
BIN = CURR_DIR / "bin"

OPTIONS = [{"num": 0, "text": "Start tikv"},
           {"num": 1, "text": "Stop tikv"},
           {"num": 2, "text": "Run Benchmark"}]


def main(run_ycsb, nodes, ssh) -> None:
    """
    Main function called by the root main.py script.
    Gives user a choice to start/stop tikv instances
    and to run the YCSB benchmark on the instance.

    :param run_ycsb: Function to run YCSB benchmark. Takes in protocol
                     data {name, language} and YCSB interface name as argument.
    :type run_ycsb: Callable[dict[str, str], str]
    """
    node_data = map_ip_port(nodes)
    print("TiKV IP-Port Data:")
    for item in node_data:
        print(item)

    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start(BIN, node_data, ssh)
            case 1:
                stop(node_data, ssh)
            case 2:
                endpoints = [f"{node["public_ip"]}:{node["client_port"]}" for node in node_data]
                print("endpoint list:", endpoints)

                run_ycsb({
                    "name": "raft",
                    "language": "Rust",
                    "consistency": "Linearizability",
                    "persistency": "On-Disk"
                }, "tikv", endpoints, "tikv.clientConnect")


def start(path, nodes, ssh) -> None:
    """
    Runs the protocol instances in different threads concurrently.
    Currently only supports local startup.

    :param path: Path to bin/ directory inside the protocol repository.
    :type path: Path
    """
    user = ssh["username"]
    TIKV_VERSION = "v7.5.0"
    GOOS = "linux"
    GOARCH = "amd64"

    for i, node in enumerate(nodes):
        if node["public_ip"] == "127.0.0.1" and node["private_ip"] == "127.0.0.1":
            local_pd = path / "pd-server"
            local_tikv = path / "tikv-server"
            initial_cluster = ",".join(f"pd{i+1}=http://{n["private_ip"]}:{n["peer_port"]}" for i, n in enumerate(nodes))
            pd_endpoints = ",".join(f"{n["private_ip"]}:{n["client_port"]}" for n in nodes)
            local_pd_dir = CURR_DIR / f"pd{i+1}"
            local_tikv_dir = CURR_DIR / f"tikv{i+1}"

            run_pd = (
                f"nohup {local_pd} --name=pd{i+1} "
                f"--data-dir={local_pd_dir.resolve()} "
                f"--client-urls=\"http://0.0.0.0:{node["client_port"]}\" "
                f"--advertise-client-urls=\"http://{node["public_ip"]}:{node["client_port"]}\" "
                f"--peer-urls=\"http://0.0.0.0:{node["peer_port"]}\" "
                f"--advertise-peer-urls=\"http://{node["private_ip"]}:{node["peer_port"]}\" "
                f"--initial-cluster=\"{initial_cluster}\" > /dev/null 2>&1 &"
            )

            run_tikv = (
                f"{local_tikv} --addr=\"0.0.0.0:{node["service_port"]}\" "
                f"--advertise-addr=\"{node["public_ip"]}:{node["service_port"]}\" "
                f"--data-dir={local_tikv_dir} "
                f"--pd-endpoints=\"{pd_endpoints}\" > /dev/null 2>&1 &"
            )
        else:
            host = node["public_ip"]
            pd_url = f"https://tiup-mirrors.pingcap.com/pd-{TIKV_VERSION}-{GOOS}-{GOARCH}.tar.gz"
            tikv_url = f"https://tiup-mirrors.pingcap.com/tikv-{TIKV_VERSION}-{GOOS}-{GOARCH}.tar.gz"

            curl_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'mkdir -p /home/{user}/tikv && cd /home/{user}/tikv && "
                f"if [ ! -f /home/{user}/tikv/pd-server ] || "
                f"[ ! -f /home/{user}/tikv/tikv-server ]; then "
                f"curl -L {pd_url} | tar -xz && "
                f"curl -L {tikv_url} | tar -xz; "
                f"else "
                f"echo \"PD and TiKV binaries already exist on {host}, skipping download.\"; "
                f"fi'"
            )

            print("Running command:", curl_cmd)
            subprocess.run(curl_cmd, check=True, shell=True)

            remote_pd = f"/home/{user}/tikv/pd-server"
            initial_cluster = ",".join(f"pd{i+1}=http://{n["private_ip"]}:{n["peer_port"]}" for i, n in enumerate(nodes))

            remote_tikv = f"/home/{user}/tikv/tikv-server"
            pd_endpoints = ",".join(f"{n["private_ip"]}:{n["client_port"]}" for n in nodes)

            run_pd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'nohup {remote_pd} --name=pd{i+1} "
                f"--data-dir=/home/{user}/tikv/pd{i+1} "
                f"--client-urls=\"http://0.0.0.0:{node["client_port"]}\" "
                f"--advertise-client-urls=\"http://{node["public_ip"]}:{node["client_port"]}\" "
                f"--peer-urls=\"http://0.0.0.0:{node["peer_port"]}\" "
                f"--advertise-peer-urls=\"http://{node["private_ip"]}:{node["peer_port"]}\" "
                f"--initial-cluster=\"{initial_cluster}\" > /dev/null 2>&1 &'"
            )

            run_tikv = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'nohup {remote_tikv} --addr=\"0.0.0.0:{node["service_port"]}\" "
                f"--advertise-addr=\"{node["public_ip"]}:{node["service_port"]}\" "
                f"--data-dir=/home/{user}/tikv/tikv{i+1} "
                f"--pd-endpoints=\"{pd_endpoints}\" > /dev/null 2>&1 &'"
            )

        print("Running command:", run_pd)
        subprocess.run(run_pd, check=True, shell=True)

        print("Running command:", run_tikv)
        subprocess.run(run_tikv, check=True, shell=True)

    print("tikv instances successfully started")


def stop(nodes, ssh) -> None:
    """
    Terminates all running instances that are still recorded inside
    the jobs list, then removes all the files created by the instances.
    """
    user = ssh["username"]
    for i, node in enumerate(nodes):
        if node["public_ip"] == "127.0.0.1" and node["private_ip"] == "127.0.0.1":
            proc = f"{BIN.resolve()}/.*-server"
            local_pd_dir = CURR_DIR / f"pd{i+1}"
            local_tikv_dir = CURR_DIR / f"tikv{i+1}"

            cmd = (
                f"pids=$(ps aux | grep '{proc}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
            )

            print("Running command:", cmd)
            subprocess.run(cmd, shell=True)
            os.system(f"rm -rf {local_pd_dir.resolve()}")
            os.system(f"rm -rf {local_tikv_dir.resolve()}")
        else:
            host = node["public_ip"]
            remote_proc = f"/home/{user}/tikv/.*-server"
            remote_pd_dir = f"/home/{user}/tikv/pd{i+1}"
            remote_tikv_dir = f"/home/{user}/tikv/tikv{i+1}"

            remote_command = (
                f"pids=$(ps aux | grep '{remote_proc}' | grep -v grep | awk '{{print $2}}'); "
                f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                f"rm -rf {remote_pd_dir}; "
                f"rm -rf {remote_tikv_dir};"
            )

            cmd = ["ssh", "-i", str(ssh["key"]), f"{user}@{host}",
                   remote_command]
            print("Running command:", " ".join(cmd))
            subprocess.run(cmd)


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


if __name__ == "__main__":
    main()


def hello():
    print("Hello from paxi")

from pathlib import Path
import subprocess
import time
import os

from src.utils import helper

CURR_DIR = Path("./sut/etcd-io.etcd")
ETCDCTL = CURR_DIR / "bin" / "etcdctl"

OPTIONS = [{"num": 0, "text": "Start etcd cluster"},
           {"num": 1, "text": "Stop etcd cluster"},
           {"num": 2, "text": "Run Benchmark"}]


def main(run_ycsb, nodes, ssh):
    node_data = map_ip_port(nodes)
    print("etcd IP-Port Data:")
    for item in node_data:
        print(item)

    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start_etcd_cluster(node_data, ssh)
            case 1:
                stop_etcd_cluster(node_data, ssh)
            case 2:
                endpoints = [f"http://{node["public_ip"]}:{node["client_port"]}" for node in node_data]
                print("endpoint list:", endpoints)
                run_ycsb({"name": "raft", "language": "Go"}, "etcd", endpoints, "etcd.endpoints")


def start_etcd_cluster(nodes, ssh):
    ETCD_VER = "v3.6.4"
    GOOGLE_URL = "https://storage.googleapis.com/etcd"
    DOWNLOAD_URL = f"{GOOGLE_URL}/{ETCD_VER}/etcd-{ETCD_VER}-linux-amd64.tar.gz"

    user = ssh["username"]

    initial_cluster = ",".join(f"node{i+1}=http://{n["private_ip"]}:{n["peer_port"]}" for i, n in enumerate(nodes))

    for i, node in enumerate(nodes):
        if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
            local_etcd = CURR_DIR / "bin" / "etcd"
            local_dir = CURR_DIR / f"node{i+1}"

            run_cmd = (
                f"nohup {local_etcd} --name node{i+1} --data-dir {local_dir} "
                f"--listen-peer-urls http://0.0.0.0:{node["peer_port"]} "
                f"--initial-advertise-peer-urls http://{node["private_ip"]}:{node["peer_port"]} "
                f"--listen-client-urls http://0.0.0.0:{node["client_port"]} "
                f"--advertise-client-urls http://{node["private_ip"]}:{node["client_port"]},http://{node["public_ip"]}:{node["client_port"]} "
                f"--initial-cluster {initial_cluster} "
                f"--initial-cluster-state new "
                f"--initial-cluster-token etcd-distrobench-cluster > /dev/null 2>&1 &"
            )
        else:
            host = node["public_ip"]
            curl_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                    f"'\nif [ ! -f /home/{user}/etcd/etcd ]; then\n"
                    f"  mkdir -p /home/{user}/etcd\n"
                    f"  curl -L {DOWNLOAD_URL} | tar -xz -C /home/{user}/etcd/ --strip-components=1 --no-same-owner\n"
                    f"else\n"
                    f"  echo \"Binary already exists, skipping download.\"\n"
                    f"fi'"
            )

            print("Running command:", curl_cmd)
            subprocess.run(curl_cmd, check=True, shell=True)

            remote_etcd = f"/home/{user}/etcd/etcd"
            remote_dir = f"/home/{user}/etcd/node{i+1}"

            run_cmd = (
                f"ssh -i {ssh['key']} {user}@{host} "
                f"'nohup {remote_etcd} --name node{i+1} --data-dir {remote_dir} "
                f"--listen-peer-urls http://0.0.0.0:{node["peer_port"]} "
                f"--initial-advertise-peer-urls http://{node["private_ip"]}:{node["peer_port"]} "
                f"--listen-client-urls http://0.0.0.0:{node["client_port"]} "
                f"--advertise-client-urls http://{node["private_ip"]}:{node["client_port"]},http://{node["public_ip"]}:{node["client_port"]} "
                f"--initial-cluster {initial_cluster} "
                f"--initial-cluster-state new "
                f"--initial-cluster-token etcd-distrobench-cluster > /dev/null 2>&1 &'"
            )

        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)


def stop_etcd_cluster(nodes, ssh):
    user = ssh["username"]

    for i, node in enumerate(nodes):
        if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
            local_etcd = CURR_DIR / "bin" / "etcd"
            local_dir = CURR_DIR / f"node{i+1}"

            cmd = (
                f"pids=$(ps aux | grep '{local_etcd}' | grep -v grep | awk '{{print $2}}'); "
                    f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
            )

            print("Running command:", cmd)
            subprocess.run(cmd, shell=True)
            os.system(f"rm -rf {local_dir.resolve()}")
        else:
            host = node["public_ip"]
            remote_etcd = f"/home/{user}/etcd/etcd"
            remote_dir = f"/home/{user}/etcd/node{i+1}"

            remote_command = (
                f"pids=$(ps aux | grep '{remote_etcd}' | grep -v grep | awk '{{print $2}}'); "
                f"for pid in $pids; do echo \"Killing $pid\"; kill -9 $pid; done; "
                f"rm -rf {remote_dir};"
            )

            cmd = ["ssh", "-i", str(ssh["key"]), f"{user}@{host}",
                   remote_command]
            print("Running command:", " ".join(cmd))
            subprocess.run(cmd)
    print("etcd removal & cleanup completed")


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
        else:
            ip_map[public_ip]["client"] += 1
            ip_map[public_ip]["peer"] += 1

        data.append({"public_ip": public_ip,
                     "client_port": ip_map[public_ip]["client"],
                     "private_ip": private_ip,
                     "peer_port": ip_map[public_ip]["peer"]})

    return data


if __name__ == "__main__":
    raise RuntimeError("This script is meant to be imported, not run directly")

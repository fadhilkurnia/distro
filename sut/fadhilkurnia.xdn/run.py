from pathlib import Path
import subprocess
import os
import re
import time

from src.utils import helper

CURR_DIR = Path("./sut/fadhilkurnia.xdn")
ROOT_PATH = Path(".")
XDN_BIN = CURR_DIR / "xdn" / "bin"
TEMPLATE_CONFIG = CURR_DIR / "xdn" / "conf" / "template.properties"

OPTIONS = [{"num": 0, "text": "Start XDN"},
           {"num": 1, "text": "Stop XDN"},
           {"num": 2, "text": "Run Benchmark"}]

'''
SERVICE_TYPE = [{"num": 1, "text": "deterministic"},
                {"num": 2, "text": "non-deterministic"},
'''


def main(run_ycsb, nodes, ssh) -> None:
    node_data = map_ip_port(nodes)

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
    user = ssh["username"]
    config_path = generate_config(nodes, user, ssh)

    for i, node in enumerate(nodes):
        if node["private_ip"] == "127.0.0.1" and node["public_ip"] == "127.0.0.1":
            local_start_active_replica(config_path, i)
        else:
            host = node["public_ip"]
            remote_start_active_replica(config_path, i, host, ssh)

    # Reconfigurator
    if nodes[0]["private_ip"] == "127.0.0.1" and nodes[0]["public_ip"] == "127.0.0.1":
        local_start_reconfigurator(config_path, 0)
    else:
        host = nodes[0]["public_ip"]
        remote_start_reconfigurator(config_path, 0, host, ssh)

    time.sleep(15)

    # Launch restkv in XDN
    env = os.environ.copy()
    env["XDN_CONTROL_PLANE"] = nodes[0]["public_ip"]
    yaml_path = CURR_DIR / "restkv-nd.yaml"
    cmd_service = ["xdn", "launch", "restkv", f"--file={yaml_path}"]
    subprocess.run(cmd_service, text=True, env=env)

    print("restkv service has started in XDN")


def stop(bin_path, nodes, ssh) -> None:
    if nodes[0]["private_ip"] == "127.0.0.1" and nodes[0]["public_ip"] == "127.0.0.1":
        local_stop()
    else:
        for node in nodes:
            host = node["public_ip"]
            remote_stop(host, ssh)

    print("xdn removal & cleanup completed")


if __name__ == "__main__":
    main()


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


def generate_config(nodes, user, ssh):
    config = []
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


def local_start_active_replica(config_path, id):
    jar_paths_str = subprocess.check_output(
        ['find', 'jars', '-name', '*.jar'],
        cwd=f"{CURR_DIR.resolve()}/xdn",
        text=True,
        stderr=subprocess.PIPE
    ).strip()
    classpath_jars = jar_paths_str.replace('\n', ':')

    run_cmd = (
        f"cd {CURR_DIR}/xdn; "
        f"nohup java -DgigapaxosConfig={config_path.resolve()} -ea "
        "-Djavax.net.ssl.keyStorePassword=qwerty "
        "-Djavax.net.ssl.trustStorePassword=qwerty "
        "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
        "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
        "-Djava.util.logging.config.file=conf/logging.properties "
        "-Dlog4j.configuration=conf/log4j.properties "
        "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
        f"-cp 'build/classes:{classpath_jars}' "
        f"edu.umass.cs.reconfiguration.ReconfigurableNode AR{id} "
        f"> node_{id}.log 2>&1 &"
    )

    print("Running command:", run_cmd)
    subprocess.run(run_cmd, check=True, shell=True)


def remote_start_active_replica(config_path, id, host, ssh):
    user = ssh["username"]
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
        f"edu.umass.cs.reconfiguration.ReconfigurableNode AR{id} "
        f"> node_{id}.log 2>&1 &\""
    )

    print("Running command:", run_cmd)
    subprocess.run(run_cmd, check=True, shell=True)


def local_start_reconfigurator(config_path, id):
    jar_paths_str = subprocess.check_output(
        ['find', 'jars', '-name', '*.jar'],
        cwd=f"{CURR_DIR.resolve()}/xdn",
        text=True,
        stderr=subprocess.PIPE
    ).strip()
    classpath_jars = jar_paths_str.replace('\n', ':')

    run_cmd = (
        f"cd {CURR_DIR}/xdn; "
        f"nohup java -DgigapaxosConfig={config_path.resolve()} -ea "
        "-Djavax.net.ssl.keyStorePassword=qwerty "
        "-Djavax.net.ssl.trustStorePassword=qwerty "
        "-Djavax.net.ssl.keyStore=conf/keyStore.jks "
        "-Djavax.net.ssl.trustStore=conf/trustStore.jks "
        "-Djava.util.logging.config.file=conf/logging.properties "
        "-Dlog4j.configuration=conf/log4j.properties "
        "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host "
        f"-cp 'build/classes:{classpath_jars}' "
        f"edu.umass.cs.reconfiguration.ReconfigurableNode RC0 "
        f"> reconf_{id}.log 2>&1 &"
    )
    print("Running command:", run_cmd)
    subprocess.run(run_cmd, check=True, shell=True)


def remote_start_reconfigurator(config_path, id, host, ssh):
    user = ssh["username"]
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
        f"edu.umass.cs.reconfiguration.ReconfigurableNode RC0 "
        f"> reconf_{id}.log 2>&1 &\""
    )
    print("Running command:", run_cmd)
    subprocess.run(run_cmd, check=True, shell=True)


def local_stop():
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


def remote_stop(host, ssh):
    user = ssh["username"]
    remote_command = (
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

    cmd = ["ssh", "-i", str(ssh["key"]), f"{user}@{host}",
           remote_command]
    print("Running command:", " ".join(cmd))
    subprocess.run(cmd)

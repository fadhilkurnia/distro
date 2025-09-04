from pathlib import Path
import subprocess
import threading
import os
import time
import shutil

from src.utils import helper

CURR_DIR = Path("./sut/holipaxos-artifect.holipaxos")
BIN_DIR = CURR_DIR / "bin"
LOG_DIR = CURR_DIR / "logs"
CONFIG_DIR = CURR_DIR / "config"

OPTIONS = [{"num": 0, "text": "Start HoliPaxos cluster"},
           {"num": 1, "text": "Stop HoliPaxos cluster"},
           {"num": 2, "text": "Run Benchmark"}]

PROTOCOLS = [{"num": 1, "text": "holipaxos"},
             {"num": 2, "text": "multipaxos"},
             {"num": 3, "text": "omnipaxos"}]

PROTOCOL_CONFIGS = {
    "holipaxos": {
        "binary": "holipaxos_replicant",
        "args_format": "posix",  # -id, -c, -d
        "env": None
    },
    "multipaxos": {
        "binary": "multipaxos_replicant",
        "args_format": "posix",  # -id, -c, -d
        "env": None
    },
    "omnipaxos": {
        "binary": "omni_replicant",
        "args_format": "gnu",    # --id, --config-path
        "env": None
    }
}

NODES = [0, 1, 2, 3, 4]

# Shared list to store job info
jobs = []


def run_command(cmd, env=None, log_file=None) -> None:
    """
    :param cmd: Command line arguments
    :type cmd: str[]
    :param env: Environment variables
    :type env: dict
    :param log_file: Path to log file for stdout/stderr redirection
    :type log_file: str
    """
    proc_env = os.environ.copy()
    if env:
        proc_env.update(env)
    
    # Redirect stdout and stderr to log file if provided
    log_handle = None
    if log_file:
        log_handle = open(log_file, 'w')
        proc = subprocess.Popen(cmd, env=proc_env, stdout=log_handle, stderr=subprocess.STDOUT)
    else:
        proc = subprocess.Popen(cmd, env=proc_env)
    
    jobs.append({
        'cmd': cmd,
        'process': proc,
        'log_file': log_handle,
        'thread': threading.current_thread()
    })


def build_command(protocol_name, node_id):
    """
    :param protocol_name: Name of the protocol (holipaxos, multipaxos, omnipaxos)
    :type protocol_name: str
    :param node_id: Node ID (0-4)
    :type node_id: int
    :return: Command array and environment variables
    :rtype: tuple[list[str], dict]
    """
    config = PROTOCOL_CONFIGS[protocol_name]
    binary_path = BIN_DIR / config["binary"]
    config_file = CONFIG_DIR / f"config_node{node_id}.json"
    
    if config["args_format"] == "posix":
        # holipaxos and multipaxos: -id X -c config -d
        cmd = [str(binary_path), "-id", str(node_id), "-c", str(config_file), "-d"]
    else: 
        # omnipaxos: --id X --config-path config
        cmd = [str(binary_path), "--id", str(node_id), "--config-path", str(config_file)]
    
    return cmd, config["env"]


def is_remote_mode(nodes):
    """Check if any node requires remote deployment"""
    if not nodes:
        return False
    return any(node.get("public") != "127.0.0.1" or node.get("private") != "127.0.0.1" 
               for node in nodes)


def setup_remote_files(protocol_name, nodes, ssh):
    """Check if files exist on remote nodes and copy if needed"""
    user = ssh["username"]
    config = PROTOCOL_CONFIGS[protocol_name]
    replicant_binary = BIN_DIR / config["binary"]
    config_file = CONFIG_DIR / "config.json"
    
    for i, node in enumerate(nodes):
        public_ip = node["public"]
        private_ip = node["private"]
        
        if private_ip == "127.0.0.1" and public_ip == "127.0.0.1":
            continue
            
        host = public_ip
        remote_dir = f"/home/{user}/holipaxos"
        remote_replicant = f"{remote_dir}/{config['binary']}"
        remote_config = f"{remote_dir}/config.json"
        
        check_cmd = (
            f"ssh -i {ssh['key']} {user}@{host} "
            f"'if [ ! -f {remote_replicant} ] || [ ! -f {remote_config} ]; then "
            f"mkdir -p {remote_dir}; echo \"missing\"; "
            f"else echo \"exists\"; fi'"
        )
        
        print(f"Checking files on node {i}: {host}")
        print("Running command:", check_cmd)
        result = subprocess.run(check_cmd, check=True, shell=True, capture_output=True, text=True)
        
        if "missing" in result.stdout:
            copy_cmd = ["rsync", "-avz", "-e", f"ssh -i {ssh['key']}",
                        str(replicant_binary.resolve()), str(config_file.resolve()),
                        f'{user}@{host}:{remote_dir}/']
            
            print(f"Copying files to node {i}: {host}")
            print("Running command:", " ".join(copy_cmd))
            subprocess.run(copy_cmd, check=True)
        else:
            print(f"Files already exist on node {i}: {host}, skipping copy")
    
    print("File setup completed for all remote nodes")


def main(run_ycsb, nodes=None, ssh=None) -> None:
    selected_protocol = None
    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                prot_num = helper.get_option(1, len(PROTOCOLS), PROTOCOLS)
                protocol_name = PROTOCOLS[prot_num-1]["text"]
                selected_protocol = {
                    "name": protocol_name,
                    "language": "Go" if protocol_name != "omnipaxos" else "Rust",
                }
                start_holipaxos_cluster(protocol_name, nodes, ssh)
            case 1:
                stop_holipaxos_cluster(nodes, ssh)
            case 2:
                if selected_protocol:
                    if is_remote_mode(nodes) and nodes:
                        endpoints = []
                        for i, node in enumerate(nodes):
                            if node["public"] != "127.0.0.1":
                                client_port = 2200 + (i * 10) + 1
                                endpoints.append(f"{node['public']}:{client_port}")
                        
                        if endpoints:
                            print("Remote benchmark endpoints:", endpoints)
                            combined = ",".join(endpoints)
                            print("Combined endpoints:", combined)
                            run_ycsb(selected_protocol, "holipaxos", [combined], "holipaxos.hosts")
                        else:
                            print("No remote nodes available for benchmarking")
                    else:
                        endpoints = [f"127.0.0.1:{10000 + node_id * 1000 + 1}" for node_id in NODES]
                        print("Local benchmark endpoints:", endpoints)
                        run_ycsb(selected_protocol, "holipaxos", endpoints, "holipaxos.hosts")
                else:
                    print("Please start a cluster first")


def start_remote_instances(protocol_name, nodes, ssh):
    """Start holipaxos instances on remote nodes"""
    user = ssh["username"]
    config = PROTOCOL_CONFIGS[protocol_name]
    
    for i, node in enumerate(nodes):
        public_ip = node["public"]
        private_ip = node["private"]
        
        # Skip local nodes
        if private_ip == "127.0.0.1" and public_ip == "127.0.0.1":
            continue
            
        host = public_ip
        remote_dir = f"/home/{user}/holipaxos"
        remote_replicant = f"{remote_dir}/{config['binary']}"
        remote_config = f"{remote_dir}/config.json"
        
        # Build command args based on protocol format
        if config["args_format"] == "posix":
            # holipaxos and multipaxos: -id X -c config -d
            args = f"-id {i} -c {remote_config} -d"
        else:
            # omnipaxos: --id X --config-path config
            args = f"--id {i} --config-path {remote_config}"
            
        run_cmd = (
            f"ssh -i {ssh['key']} {user}@{host} "
            f"'nohup {remote_replicant} {args} > /dev/null 2>&1 &'"
        )
        
        print(f"Starting holipaxos instance on node {i}: {host}")
        print("Running command:", run_cmd)
        subprocess.run(run_cmd, check=True, shell=True)
    
    print("All remote holipaxos instances started successfully")


def start_holipaxos_cluster(protocol_name, nodes=None, ssh=None) -> None:
    if is_remote_mode(nodes) and nodes and ssh:
        print(f"Remote mode detected. Setting up and starting holipaxos instances...")
        stop_holipaxos_cluster(nodes, ssh, protocol_name)
        setup_remote_files(protocol_name, nodes, ssh)
        start_remote_instances(protocol_name, nodes, ssh)
        print(f"Remote deployment completed for {protocol_name}")
        return
    
    LOG_DIR.mkdir(exist_ok=True)
    
    stop_holipaxos_cluster()
    
    print(f"Starting {protocol_name} cluster with {len(NODES)} nodes...")
    
    for node_id in NODES:
        cmd, env = build_command(protocol_name, node_id)
        consensus_port = 10000 + node_id * 1000
        client_port = consensus_port + 1
        log_file = LOG_DIR / f"node_{node_id}.log"
        
        print(f"Starting Node {node_id}: consensus=localhost:{consensus_port}, client=localhost:{client_port}")
        print(f"Command: {' '.join(cmd)}")
        
        t = threading.Thread(target=run_command, args=(cmd, env, str(log_file)))
        t.start()
        time.sleep(1) 
    
    print(f"{protocol_name} cluster started successfully")


def stop_remote_instances(protocol_name, nodes, ssh):
    """Stop holipaxos instances on remote nodes"""
    user = ssh["username"]
    
    if protocol_name is None:
        binaries_to_kill = [config["binary"] for config in PROTOCOL_CONFIGS.values()]
    else:
        binaries_to_kill = [PROTOCOL_CONFIGS[protocol_name]["binary"]]
    
    for i, node in enumerate(nodes):
        public_ip = node["public"]
        private_ip = node["private"]
        
        if private_ip == "127.0.0.1" and public_ip == "127.0.0.1":
            continue
            
        host = public_ip
        remote_dir = f"/home/{user}/holipaxos"
        
        kill_commands = []
        for binary in binaries_to_kill:
            remote_binary = f"{remote_dir}/{binary}"
            kill_commands.append(f"pids=$(ps aux | grep '{remote_binary}' | grep -v grep | awk '{{print $2}}'); for pid in $pids; do echo \"Killing {binary} $pid\"; kill -9 $pid; done;")
        
        remote_command = " ".join(kill_commands) + " rm -rf /tmp/presistent_node*;"
        
        cmd = ["ssh", "-i", str(ssh["key"]), f"{user}@{host}", remote_command]
        print(f"Stopping holipaxos on node {i}: {host}")
        print("Running command:", " ".join(cmd))
        subprocess.run(cmd)
    
    print("All remote holipaxos instances stopped")


def stop_holipaxos_cluster(nodes=None, ssh=None, protocol_name=None) -> None:
    print("Stopping cluster...")
    
    if is_remote_mode(nodes) and nodes and ssh:
        stop_remote_instances(protocol_name, nodes, ssh)
        return
    
    for job in jobs:
        proc = job['process']
        if proc.poll() is None: 
            print(f"Terminating: {' '.join(map(str, job['cmd']))}")
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                print(f"Force killing: {' '.join(map(str, job['cmd']))}")
                proc.kill()
        
        # Close log file handle if it exists
        if job.get('log_file'):
            job['log_file'].close()
    
    jobs.clear()
    
    for node_id in NODES:
        data_dir = Path(f"/tmp/presistent_node{node_id}")
        if data_dir.exists():
            shutil.rmtree(data_dir)
    
    print("Cluster stopped and data directories cleaned")


if __name__ == "__main__":
    def mock_run_ycsb(protocol, interface):
        print(f"Would run YCSB with protocol: {protocol}, interface: {interface}")
    
    main(mock_run_ycsb)


def hello():
    print("Hello from holipaxos")
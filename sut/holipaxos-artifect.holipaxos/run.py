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


def main(run_ycsb) -> None:
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
                start_holipaxos_cluster(protocol_name)
            case 1:
                stop_holipaxos_cluster()
            case 2:
                if selected_protocol:
                    run_ycsb(selected_protocol, "holipaxos")
                else:
                    print("Please start a cluster first")


def start_holipaxos_cluster(protocol_name) -> None:
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


def stop_holipaxos_cluster() -> None:
    print("Stopping cluster...")
    
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
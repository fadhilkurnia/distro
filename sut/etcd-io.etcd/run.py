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

goreman_process = None

def main(run_ycsb):
    selected_protocol = {
        "name": "raft",
        "language": "Go",
    }
    
    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start_etcd_cluster()
            case 1:
                stop_etcd_cluster()
            case 2:
                run_ycsb(selected_protocol, "etcd")

def start_etcd_cluster():
    global goreman_process
    
    # Clean up first
    os.system(f"cd {CURR_DIR} && ./clean.sh")
    
    print("Starting etcd cluster with goreman...")
    goreman_process = subprocess.Popen(
        ["goreman", "start"],
        cwd=CURR_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for cluster to form
    print("Waiting for cluster to initialize...")
    time.sleep(5)
    
    # Verify cluster is running
    try:
        result = subprocess.run(
            [ETCDCTL, "--endpoints=http://127.0.0.1:2379", "member", "list"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            print("etcd cluster successfully started")
            print("Cluster members:")
            print(result.stdout)
        else:
            print("Error: Cluster verification failed")
            print(result.stderr)
            stop_etcd_cluster()
    except subprocess.TimeoutExpired:
        print("Error: Cluster verification timed out")
        stop_etcd_cluster()
    except Exception as e:
        print(f"Error verifying cluster: {e}")
        stop_etcd_cluster()

def stop_etcd_cluster():
    global goreman_process
    
    if goreman_process and goreman_process.poll() is None:
        print("Stopping etcd cluster...")
        goreman_process.terminate()
        try:
            goreman_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print("Force killing etcd cluster...")
            goreman_process.kill()
            goreman_process.wait()
        
        goreman_process = None
        print("etcd cluster stopped")
    
    # Clean up
    os.system(f"cd {CURR_DIR} && ./clean.sh")
    print("Cleanup completed")

if __name__ == "__main__":
    def mock_run_ycsb(protocol, interface):
        print(f"Would run YCSB with protocol: {protocol}, interface: {interface}")
    
    main(mock_run_ycsb)
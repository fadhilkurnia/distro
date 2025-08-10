from pathlib import Path
import subprocess
import threading
import time
import os
import signal

from src.utils import helper

CURR_DIR = Path("./sut/otoolep.hraftd")
HRAFTD_BIN = CURR_DIR / "hraftd"

OPTIONS = [{"num": 0, "text": "Start hraftd cluster"},
           {"num": 1, "text": "Stop hraftd cluster"},
           {"num": 2, "text": "Run Benchmark"}]

jobs = []

def run_command(cmd, cwd=None):
    proc = subprocess.Popen(cmd, cwd=cwd)
    jobs.append({
        'cmd': cmd,
        'process': proc,
        'thread': threading.current_thread()
    })
    proc.wait()

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
                start_hraftd_cluster()
            case 1:
                stop_hraftd_cluster()
            case 2:
                run_ycsb(selected_protocol, "hraftd")

def start_hraftd_cluster():
    for i in range(1, 6):
        os.makedirs(f"/tmp/hraftd-node{i}", exist_ok=True)
    
    commands = [
        [HRAFTD_BIN, "-id", "node1", "-haddr", "localhost:11001", 
         "-raddr", "localhost:12001", "/tmp/hraftd-node1"],
        
        [HRAFTD_BIN, "-id", "node2", "-haddr", "localhost:11002", 
         "-raddr", "localhost:12002", "-join", "localhost:11001", "/tmp/hraftd-node2"],
        
        [HRAFTD_BIN, "-id", "node3", "-haddr", "localhost:11003", 
         "-raddr", "localhost:12003", "-join", "localhost:11001", "/tmp/hraftd-node3"],
        
        [HRAFTD_BIN, "-id", "node4", "-haddr", "localhost:11004", 
         "-raddr", "localhost:12004", "-join", "localhost:11001", "/tmp/hraftd-node4"],
        
        [HRAFTD_BIN, "-id", "node5", "-haddr", "localhost:11005", 
         "-raddr", "localhost:12005", "-join", "localhost:11001", "/tmp/hraftd-node5"],
    ]
    
    print(f"Starting: {' '.join(map(str, commands[0]))}")
    t = threading.Thread(target=run_command, args=(commands[0],))
    t.start()
    
    time.sleep(3)
    
    for cmd in commands[1:]:
        print(f"Starting: {' '.join(map(str, cmd))}")
        t = threading.Thread(target=run_command, args=(cmd,))
        t.start()
        time.sleep(1)
    
    print("hraftd cluster successfully started")

def stop_hraftd_cluster():
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
    
    os.system("rm -rf /tmp/hraftd-node*")
    jobs.clear()

if __name__ == "__main__":
    main()
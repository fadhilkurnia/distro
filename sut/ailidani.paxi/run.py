from pathlib import Path
import subprocess
import threading
import os

from src.utils import helper

CURR_DIR = Path("./sut/ailidani.paxi")
PAXI_BIN = CURR_DIR / "paxi" / "bin"

OPTIONS = [{"num": 0, "text": "Start Paxi"},
           {"num": 1, "text": "Stop Paxi"},
           {"num": 2, "text": "Run Benchmark"}]

PROTOCOLS = [{"num": 1, "text": "paxos"},
             {"num": 2, "text": "epaxos"},
             {"num": 3, "text": "sdpaxos"},
             {"num": 4, "text": "wpaxos"},
             {"num": 5, "text": "abd"},
             {"num": 6, "text": "chain"},
             {"num": 7, "text": "vpaxos"},
             {"num": 8, "text": "wankeeper"},
             {"num": 9, "text": "kpaxos"},
             {"num": 10, "text": "paxos_groups"},
             {"num": 11, "text": "dynamo"},
             {"num": 12, "text": "blockchain"},
             {"num": 13, "text": "m2paxos"},
             {"num": 14, "text": "hpaxos"}]


# Shared list to store job info
jobs = []


def run_command(cmd):
    proc = subprocess.Popen(cmd)
    jobs.append({
        'cmd': cmd,
        'process': proc,
        'thread': threading.current_thread()
    })
    proc.wait()


def main(run_ycsb):
    selected_protocol = None
    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                prot_num = helper.get_option(1, len(PROTOCOLS), PROTOCOLS)
                selected_protocol = PROTOCOLS[prot_num-1]["text"]
                start_paxi(PAXI_BIN, selected_protocol)
            case 1:
                stop_paxi()
            case 2:
                run_ycsb(selected_protocol, "paxi")


def start_paxi(path, protocol):
    # List of commands to run
    server = path / "server"
    config = CURR_DIR / "config.json"

    commands = [
        [server, "-id", "1.1", f"-algorithm={protocol}", "-config", config],
        [server, "-id", "1.2", f"-algorithm={protocol}", "-config", config],
        [server, "-id", "1.3", f"-algorithm={protocol}", "-config", config],
        [server, "-id", "2.1", f"-algorithm={protocol}", "-config", config],
        [server, "-id", "2.2", f"-algorithm={protocol}", "-config", config],
    ]

    # Start each command in its own thread
    for cmd in commands:
        print(f"Starting: {' '.join(map(str, cmd))}")
        t = threading.Thread(target=run_command, args=(cmd,))
        t.start()
    print("Paxi instances successfully started")


def stop_paxi():
    # Terminate all still-running processes
    for job in jobs:
        proc = job['process']
        if proc.poll() is None:  # Still running
            print(f"Terminating: {' '.join(map(str, job['cmd']))}")
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                print(f"Force killing: {' '.join(map(str, job['cmd']))}")
                proc.kill()

    os.system("rm server.*")


if __name__ == "__main__":
    main()


def hello():
    print("Hello from paxi")

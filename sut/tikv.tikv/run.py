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

# Shared list to store job info
jobs = []


def run_command(cmd) -> None:
    """
    Runs a command in a new subprocess which will be tracked inside jobs list.

    :param cmd: Command line arguments
    :type cmd: str[]
    """
    proc = subprocess.Popen(
        cmd,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL
    )
    jobs.append({
        'cmd': cmd,
        'process': proc,
        'thread': threading.current_thread()
    })
    proc.wait()


def main(run_ycsb) -> None:
    """
    Main function called by the root main.py script.
    Gives user a choice to start/stop tikv instances
    and to run the YCSB benchmark on the instance.

    :param run_ycsb: Function to run YCSB benchmark. Takes in protocol
                     data {name, language} and YCSB interface name as argument.
    :type run_ycsb: Callable[dict[str, str], str]
    """
    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start(BIN)
            case 1:
                stop()
            case 2:
                run_ycsb({
                    "name": "raft",
                    "language": "Rust",
                    "consistency": "Linearizability",
                    "persistency": "On-Disk"
                }, "tikv")


def start(path) -> None:
    """
    Runs the protocol instances in different threads concurrently.
    Currently only supports local startup.

    :param path: Path to bin/ directory inside the protocol repository.
    :type path: Path
    """
    host_ip = "127.0.0.1"

    pd_server = path / "pd-server"
    tikv_server = path / "tikv-server"
    commands = [
        [pd_server, "--name=pd1", "--data-dir=pd1",
         f"--client-urls=http://{host_ip}:2379",
         f"--peer-urls=http://{host_ip}:2380",
         f"--initial-cluster=pd1=http://{host_ip}:2380"],

        [tikv_server, f"--pd-endpoints={host_ip}:2379",
         f"--addr={host_ip}:20160", '--data-dir=tikv1'],

        [tikv_server, f"--pd-endpoints={host_ip}:2379",
         f"--addr={host_ip}:20161", '--data-dir=tikv2'],

        [tikv_server, f'--pd-endpoints="{host_ip}:2379"',
         f"--addr={host_ip}:20162", '--data-dir=tikv3'],

        [tikv_server, f'--pd-endpoints="{host_ip}:2379"',
         f"--addr={host_ip}:20163", '--data-dir=tikv4'],

        [tikv_server, f'--pd-endpoints="{host_ip}:2379"',
         f"--addr={host_ip}:20164", '--data-dir=tikv5'],
    ]

    # Start each command in its own thread
    for cmd in commands:
        print(f"Starting: {' '.join(map(str, cmd))}")
        t = threading.Thread(target=run_command, args=(cmd,))
        t.start()
    print("tikv instances successfully started")


def stop() -> None:
    """
    Terminates all running instances that are still recorded inside
    the jobs list, then removes all the files created by the instances.
    """
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

    directories = ["pd1", "tikv1", "tikv2", "tikv3", "tikv4", "tikv5"]
    for dir in directories:
        os.system(f"rm -r {dir}")


if __name__ == "__main__":
    main()


def hello():
    print("Hello from paxi")

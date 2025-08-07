import subprocess
import threading
import json
import sys

PAXI_BIN = "./sut/ailidani.paxi/bin"
YCSB_DIR = "./src/ycsb"
YCSB_BIN = "./src/ycsb/bin/ycsb"
WORKLOADS_DIR = "./src/ycsb/workloads"
OPTIONS = [{
    "num": 0,
    "text": "Start Paxi"
}, {
    "num": 1,
    "text": "Stop Paxi"
}, {
    "num": 2,
    "text": "Run Benchmark"
}]
WORKLOADS = [{
    "num": 1,
    "text": "workloada"
}, {
    "num": 2,
    "text": "workloadb"
}, {
    "num": 3,
    "text": "workloadc"
}]
PROTOCOLS = [{
    "num": 1, "text": "paxos"
}, {
    "num": 2, "text": "epaxos"
}, {
    "num": 3, "text": "sdpaxos"
}, {
    "num": 4, "text": "wpaxos"
}, {
    "num": 5, "text": "abd"
}, {
    "num": 6, "text": "chain"
}, {
    "num": 7, "text": "vpaxos"
}, {
    "num": 8, "text": "wankeeper"
}, {
    "num": 9, "text": "kpaxos"
}, {
    "num": 10, "text": "paxos_groups"
}, {
    "num": 11, "text": "dynamo"
}, {
    "num": 12, "text": "blockchain"
}, {
    "num": 13, "text": "m2paxos"
}, {
    "num": 14, "text": "hpaxos"
}]


# Shared list to store job info
jobs = []


def get_option(min, max, opts):
    while True:
        print("\nOptions:")
        for opt in opts:
            print(f"{opt['num']} - {opt['text']}")

        try:
            num = int(input("Select: ").strip())

            if min <= num <= max:
                return num

        except KeyboardInterrupt:
            print("\nExiting program...")
            sys.exit()
        except ValueError:
            pass


def run_command(cmd):
    proc = subprocess.Popen(cmd)
    jobs.append({
        'cmd': cmd,
        'process': proc,
        'thread': threading.current_thread()
    })
    proc.wait()


def main():
    while True:
        val = get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                prot_num = get_option(1, len(PROTOCOLS), PROTOCOLS)
                start_paxi(PAXI_BIN, PROTOCOLS[prot_num-1]["text"])
            case 1:
                stop_paxi()
            case 2:
                wrk_num = get_option(1, len(WORKLOADS), WORKLOADS)
                run_ycsb(WORKLOADS[wrk_num-1]["text"])


def run_ycsb(workload):
    subprocess.run(
        ["./bin/ycsb", "load", "paxi", "-P", f"./workloads/{workload}"],
        cwd=YCSB_DIR)

    result = subprocess.run(
        ["./bin/ycsb", "run", "paxi", "-P", f"./workloads/{workload}"],
        cwd=YCSB_DIR,
        stdout=subprocess.PIPE,
        stderr=None,
        text=True)

    #output_result = result.stdout.splitlines()[-37:]
    parsed = parse_ycsb_output(result.stdout.splitlines())
    print(f"{workload} output:")
    print(json.dumps(parsed, indent=2))


def parse_ycsb_output(lines):
    data = {}
    for line in lines:
        line = line.strip()
        if (not line or not line.startswith("[")
                or line.startswith("[INFO]")
                or line.startswith("[DEBUG]")
                or line.startswith("[WARNING]")):
            continue  # skip empty lines or non-data lines

        try:
            # Split into components
            parts = line.split("],")
            section = parts[0][1:].strip()  # Remove opening '['
            key_value = parts[1].split(",", 1)
            key = key_value[0].strip()
            value = key_value[1].strip()

            # Try to parse value as float or int
            if '.' in value:
                try:
                    value = float(value)
                except ValueError:
                    pass
            else:
                try:
                    value = int(value)
                except ValueError:
                    pass

            # Store in nested dict
            if section not in data:
                data[section] = {}
            data[section][key] = value
        except Exception as e:
            print(f"Failed to parse line: {line}\nError: {e}")
            continue

    return data


def start_paxi(path, protocol):
    # List of commands to run
    commands = [
        [f"{path}/server", "-id", "1.1", f"-algorithm={protocol}", "-config", f"{path}/config.json"],
        [f"{path}/server", "-id", "1.2", f"-algorithm={protocol}", "-config", f"{path}/config.json"],
        [f"{path}/server", "-id", "1.3", f"-algorithm={protocol}", "-config", f"{path}/config.json"],
        [f"{path}/server", "-id", "2.1", f"-algorithm={protocol}", "-config", f"{path}/config.json"],
        [f"{path}/server", "-id", "2.2", f"-algorithm={protocol}", "-config", f"{path}/config.json"],
    ]

    # Start each command in its own thread
    for cmd in commands:
        t = threading.Thread(target=run_command, args=(cmd,))
        t.start()


def stop_paxi():
    # Terminate all still-running processes
    for job in jobs:
        proc = job['process']
        if proc.poll() is None:  # Still running
            print(f"Terminating: {' '.join(job['cmd'])}")
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                print(f"Force killing: {' '.join(job['cmd'])}")
                proc.kill()


if __name__ == "__main__":
    main()


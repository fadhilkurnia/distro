from pathlib import Path
import subprocess
import threading
import os
import re

from src.utils import helper

CURR_DIR = Path("./sut/fadhilkurnia.xdn")
XDN_BIN = CURR_DIR / "xdn" / "bin"
#START_CONFIG = CURR_DIR / "xdn" / "eval" / "static" / "gigapaxos.xdn.3way.local.properties"
TEMPLATE_CONFIG = CURR_DIR / "xdn" / "conf" / "template.properties"

OPTIONS = [{"num": 0, "text": "Start XDN"},
           {"num": 1, "text": "Stop XDN"},
           {"num": 2, "text": "Run Benchmark"}]

'''
SERVICE_TYPE = [{"num": 1, "text": "deterministic"},
                {"num": 2, "text": "non-deterministic"},
'''


class TriggerWatcher:
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None
        self.thread = None
        self._lock = threading.Lock()
        self._trigger_text = None
        self._trigger_event = threading.Event()

    def start(self):
        self.process = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        self.thread = threading.Thread(target=self._watch_output, daemon=True)
        self.thread.start()

    def _watch_output(self):
        for line in self.process.stdout:
            print(f"[OUTPUT] {line.strip()}")
            with self._lock:
                if self._trigger_text and self._trigger_text in line:
                    self._trigger_event.set()

    def wait_for(self, trigger_text, timeout=None):
        with self._lock:
            self._trigger_text = trigger_text
            self._trigger_event.clear()
        print(f"Waiting for: {trigger_text}")
        found = self._trigger_event.wait(timeout=timeout)
        if not found:
            raise TimeoutError(f"Timeout waiting for: {trigger_text}")
        print(f"Trigger '{trigger_text}' detected")

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.wait()


def run_command(cmd) -> None:
    """
    Runs a command in a new subprocess which will be tracked inside jobs list.

    :param cmd: Command line arguments
    :type cmd: str[]
    """
    proc = subprocess.Popen(cmd)
    proc.wait()


def main(run_ycsb, nodes) -> None:
    """
    Main function called by the root main.py script.
    Gives user a choice to start/stop instances
    and to run the YCSB benchmark on the instance.

    :param run_ycsb: Function to run YCSB benchmark. Takes in protocol
                     data {name, language} and YCSB interface name as argument.
    :type run_ycsb: Callable[dict[str, str], str]
    :param nodes: List of node IP
    :type nodes: dict[str, str, str, str, str]
    """
    duplicate_count = {}
    custom_ips = {}
    for index, (k, v) in enumerate(nodes.items()):
        if v not in duplicate_count or duplicate_count[v] is None:
            duplicate_count[v] = 0
        else:
            duplicate_count[v] += 1

        custom_ips[f"active.AR{index}"] = f"{v}:200{duplicate_count[v]}"
        if index == 0:
            custom_ips["reconfigurator.RC0"] = f"{v}:3000"

    print("Node IP Adresses:")
    for k, v in custom_ips.items():
        print(f"{k} = {v}")

    config = generate_config(TEMPLATE_CONFIG, custom_ips)
    while True:
        val = helper.get_option(0, len(OPTIONS) - 1, OPTIONS)
        print()

        match val:
            case 0:
                start_xdn(XDN_BIN, config)
            case 1:
                stop_xdn(XDN_BIN, config)
            case 2:
                run_ycsb({
                    "name": "xdn",
                    "language": "Java",
                    "consistency": "Linearizability + Primary Integrity",
                    "persistency": "On-Disk"
                }, "xdn")


def start_xdn(path, config) -> None:
    """
    Runs the XDN instances with the specified protocol in different
    threads concurrently. Currently only supports local startup.

    :param path: Path to bin/ directory inside the xdn repository.
    :type path: Path
    :param config: Path to config file to run XDN startup script
    :type config: Path
    """
    start_script = path / "gpServer.sh"

    cmd_xdn = [start_script, f"-DgigapaxosConfig={config}", "start", "all"]
    watcher = TriggerWatcher(cmd_xdn)
    watcher.start()

    watcher.wait_for("HttpReconfigurator ready on")
    print("XDN has finished initialzing. Now starting restkv service...")

    yaml_path = CURR_DIR / "restkv.yaml"
    cmd_service = ["xdn", "launch", "restkv", f"--file={yaml_path}"]
    subprocess.run(cmd_service, text=True)

    watcher.wait_for("non-deterministic service initialization complete")
    print("restkv service has started in XDN")


def stop_xdn(bin_path, config) -> None:
    """
    Terminates all running instances of paxi that are still recorded inside
    the jobs list, then removes all the logfiles created by the instances.
    """
    start_script = bin_path / "gpServer.sh"

    cmd_xdn = [start_script, f"-DgigapaxosConfig={config}", "forceclear", "all"]
    subprocess.run(cmd_xdn, text=True)

    subprocess.run(["docker", "network", "prune", "--force"], text=True)

    os.system("fusermount -u /tmp/xdn/state/fuselog/ar0/mnt/restkv/e0")
    os.system("rm -rf /tmp/gigapaxos")
    os.system("rm -rf /tmp/xdn")
    os.system("rm -rf ./output ./derby.log")
    os.system(f"rm -rf {config}")
    print("XDN has stopped")


if __name__ == "__main__":
    main()


def generate_config(ori_config, new_ips):
    print("Generating modified config")
    new_actives = {key: val for key, val in new_ips.items() if "active" in key}
    new_reconfigurator = {key: val for key, val in new_ips.items()
                          if "reconfigurator" in key}

    lines = None
    with open(ori_config, "r") as f:
        lines = f.readlines()

    modified_lines = []
    for line in lines:
        match_active = re.match(r"(active\.AR\d+)=(.*)", line)
        if match_active:
            key = match_active.group(1)
            if key in new_actives:
                modified_lines.append(f"{key}={new_actives[key]}\n")
                continue

        match_reconf = re.match(r"(reconfigurator\.RC0)=(.*)", line)
        if match_reconf:
            key = match_reconf.group(1)
            modified_lines.append(f"{key}={new_reconfigurator[key]}\n")
            continue

        modified_lines.append(line)

    custom_property = CURR_DIR / "xdn" / "conf" / "custom.properties"
    with open(custom_property, "w") as f:
        f.writelines(modified_lines)

    return custom_property


def hello():
    print("Hello from XDN")

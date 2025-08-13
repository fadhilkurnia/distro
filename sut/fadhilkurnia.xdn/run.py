from pathlib import Path
import subprocess
import threading
import os

from src.utils import helper

CURR_DIR = Path("./sut/fadhilkurnia.xdn")
XDN_BIN = CURR_DIR / "xdn" / "bin"
START_CONFIG = CURR_DIR / "xdn" / "eval" / "static" / "gigapaxos.xdn.3way.local.properties"

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


def main(run_ycsb) -> None:
    """
    Main function called by the root main.py script.
    Gives user a choice to start/stop instances
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
                start_xdn(XDN_BIN, START_CONFIG)
            case 1:
                stop_xdn(XDN_BIN, START_CONFIG)
            case 2:
                run_ycsb({"name": "Primary-Backup", "language": "Java"}, "xdn")


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
    print("XDN has stopped")


if __name__ == "__main__":
    main()


def hello():
    print("Hello from XDN")

from pathlib import Path
import subprocess
import threading
import time

from src.utils import helper

CURR_DIR = Path("./sut/apache.zookeeper")
ZK_BIN = CURR_DIR / "apache-zookeeper" / "bin"

OPTIONS = [{"num": 0, "text": "Start Zookeeper"},
           {"num": 1, "text": "Stop Zookeeper"},
           {"num": 2, "text": "Run Benchmark"}]


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
                start_zk(ZK_BIN)
            case 1:
                stop_zk(ZK_BIN)
            case 2:
                run_ycsb({"name": "Zab", "language": "Java"}, "zookeeper")


def start_zk(path) -> None:
    """
    Runs the instances with the specified protocol in different
    threads concurrently. Currently only supports local startup.

    :param path: Path to bin/ directory inside the zookeeper repository.
    :type path: Path
    """
    server = path / "zkServer.sh"
    client = path / "zkCli.sh"
    cluster_dir = CURR_DIR / "zk-cluster"
    configs = [cluster_dir / f"node{i}" / "zoo.cfg" for i in range(1, 6)]

    commands = [
        [server, "start", configs[0]],
        [server, "start", configs[1]],
        [server, "start", configs[2]],
        [server, "start", configs[3]],
        [server, "start", configs[4]]
    ]

    for cmd in commands:
        subprocess.run(cmd, text=True)

    print("Zookeeper instances successfully started")
    time.sleep(5)

    process = subprocess.Popen(
        [client, "-server", "localhost:2181"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    process.stdin.write("create /benchmark\n")
    process.stdin.write("quit\n")
    process.stdin.flush()

    stdout, stderr = process.communicate()
    print("Client created /benchmark:\n", stdout)


def stop_zk(path) -> None:
    """
    Terminates all running instances of zookeeper.

    :param path: Path to bin/ directory inside the zookeeper repository.
    :type path: Path
    """
    server = path / "zkServer.sh"
    cluster_dir = CURR_DIR / "zk-cluster"
    configs = [cluster_dir / f"node{i}" / "zoo.cfg" for i in range(1, 6)]

    commands = [
        [server, "stop", configs[0]],
        [server, "stop", configs[1]],
        [server, "stop", configs[2]],
        [server, "stop", configs[3]],
        [server, "stop", configs[4]]
    ]

    for cmd in commands:
        subprocess.run(cmd, text=True)


if __name__ == "__main__":
    main()


def hello():
    print("Hello from zookeeper")

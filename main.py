from pathlib import Path
import importlib.util
import sys
import subprocess
import json
import os
from dotenv import load_dotenv

from src.utils import helper

load_dotenv()
YCSB_DIR = Path("./src/ycsb")
YCSB_BIN = Path("./bin/ycsb")
YCSB_WORKLOAD_DIR = Path("./workloads")
WORKLOADS = ["read-heavy", "update-heavy"]
DATA = os.getenv("OUTPUT_FILE", "data.local.json")
selected_project = None


def main(nodes, ssh_key) -> None:
    """
    List out all directories in sut/ for user to pick.
    The chosen directory will have its run.py script called
    to setup the protocol instance before running YCSB benchmark.

    :param nodes: List of node IP
    :type nodes: dict[str, str, str, str, str]
    """
    global selected_project
    systems_under_test = Path("./sut")
    systems = [p for p in systems_under_test.iterdir() if p.is_dir()]
    systems.sort()

    options = [{"num": i, "text": sys.name}
               for i, sys in enumerate(systems, start=1)]
    num = helper.get_option(1, len(options), options)

    selected_project = systems[num-1]
    path = "." / selected_project / "run.py"
    spec = importlib.util.spec_from_file_location(f"{selected_project.name}",
                                                  path)
    module = importlib.util.module_from_spec(spec)

    sys.modules[selected_project.name] = module
    spec.loader.exec_module(module)

    print(module)
    module.main(run_ycsb, nodes, ssh_key)


def run_ycsb(protocol, interface, addr_list, endpoint_name) -> None:
    """
    Give user options to pick a workload, then runs that workload
    onto the specified protocol. The YCSB output is then parsed and
    later stored in a .json file specified by DATA.

    :param protocol: Protocol data that will be benchmarked
                     (project, protocol, language, consistency, persistency)
    :type protocol: dict[str, str, str, str, str]
    :param interface: YCSB interface name for the protocol
    :type interface: str
    :param addr_list: List of addresses (ip & port) for all endpoints
    :type addr_list: dict[str, str, str, str, str]
    """
    global selected_project
    options = [{"num": i, "text": name}
               for i, name in enumerate(WORKLOADS, start=1)]
    num = helper.get_option(1, len(options), options)

    workload_path = YCSB_WORKLOAD_DIR / WORKLOADS[num-1]

    print("YCSB endpoint list:", addr_list)
    subprocess.run(
        [YCSB_BIN, "load", interface, "-P", workload_path, "-p",
         f"{endpoint_name}=http://{addr_list[0]}"],
        cwd=YCSB_DIR)

    process = subprocess.Popen(
        [YCSB_BIN, "run", interface, "-P", workload_path, "-p",
         f"{endpoint_name}=http://{addr_list[0]}"],
        cwd=YCSB_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # optional: merge stderr into stdout
        text=True,
    )

    result = []
    for line in process.stdout:
        print(line, end='')  # Print to terminal
        result.append(line)

    parsed = parse_ycsb_output(result)
    print(json.dumps(parsed, indent=2))

    keep_keys = {"READ", "UPDATE", "DELETE", "INSERT", "OVERALL"}
    result = {k: parsed[k] for k in keep_keys if k in parsed}

    with open(DATA, "r") as f:
        data = json.load(f)

    already_exists = False
    for item in data:
        if (item["project"] == selected_project.name
                and item["protocol"] == protocol["name"]
                and item["workload"] == workload_path.name):
            already_exists = True
            item["result"] = result

    if not already_exists:
        data.append({
            "project": selected_project.name,
            "protocol": protocol['name'],
            "language": protocol.get("language", ""),
            "workload": workload_path.name,
            "result": result,
            "consistency": protocol.get("consistency", ""),
            "persistency": protocol.get("persistency", ""),
        })

    with open(DATA, "w") as f:
        json.dump(data, f, indent=2)
    print(f"{workload_path.name} result has been inserted into {DATA}.")


def parse_ycsb_output(lines) -> dict[str]:
    """
    Parses the output of YCSB benchmark. Only takes the
    final results and parses it into a dictionary.

    :param lines: The YCSB benchmark output.
    :type lines: str[]
    :return: A dictionary of the benchmark result.
    :rtype: dict[str...]
    """
    data = {}
    for line in lines:
        line = line.strip()
        if (not line or not line.startswith("[")
                or line.startswith("[INFO]")
                or line.startswith("[DEBUG]")
                or line.startswith("[WARNING]")):
            continue  # skip empty lines or non-data lines

        try:
            parts = line.split("],")
            section = parts[0][1:].strip()
            key_value = parts[1].split(",", 1)
            key = key_value[0].strip()
            value = key_value[1].strip()

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


if __name__ == "__main__":
    num_of_nodes = int(os.getenv("NUM_OF_NODES"))
    '''
    nodes = {f"node{i}": os.getenv(f"NODE{i}_IP")
             for i in range(1, num_of_nodes+1)}
    '''
    ssh_key = Path.cwd() / os.getenv("SSH_KEY")
    username = os.getenv("REMOTE_USERNAME")
    ssh = {"key": ssh_key, "username": username}

    nodes = [{"private": os.getenv(f"PRIVATE_IP{i}"),
             "public": os.getenv(f"PUBLIC_IP{i}")}
             for i in range(1, num_of_nodes+1)]

    print("SSH:", ssh)
    print("Addresses:", nodes)
    main(nodes, ssh)

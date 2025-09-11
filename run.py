from pathlib import Path
import importlib.util
import sys
import subprocess
import json
import os
import shlex
from dotenv import load_dotenv

from src.utils import helper

load_dotenv()
YCSB_DIR = Path("./src/ycsb")
YCSB_BIN = YCSB_DIR / "bin" / "ycsb"
YCSB_WORKLOAD_DIR = Path("./workloads")
WORKLOADS = [
    {
        "num": 1,
        "text": "read-heavy",
        "type": "single-client",
    }, {
        "num": 2,
        "text": "update-heavy",
        "type": "single-client",
    }
]
DATA = os.getenv("OUTPUT_FILE", "data.local.json")
selected_project = None
client_ip = None


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


def run_ycsb(protocol, interface, addr_list, endpoint_name, ssh) -> None:
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
    num = helper.get_option(1, len(WORKLOADS), WORKLOADS)

    # rsync YCSB client files
    if client_ip == "127.0.0.1" and client_ip == "127.0.0.1":
        workload_path = YCSB_WORKLOAD_DIR / WORKLOADS[num-1]["text"]

        print("YCSB endpoint list:", addr_list)
        subprocess.run(
            [YCSB_BIN.resolve(), "load", interface, "-P", workload_path, "-p",
             f"{endpoint_name}={addr_list[0]}"],
            cwd=YCSB_DIR)

        process = subprocess.Popen(
            [YCSB_BIN.resolve(), "run", interface, "-P", workload_path, "-p",
             f"{endpoint_name}={addr_list[0]}"],
            cwd=YCSB_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    else:
        user = ssh["username"]
        local_dir = YCSB_DIR
        remote_dir = f"/home/{user}/ycsb"
        remote_bin = f"{remote_dir}/bin/ycsb"
        workload_path = f"{remote_dir}/workloads/{WORKLOADS[num-1]["text"]}"

        copy_cmd = (
            f"rsync -avz -e 'ssh -i {ssh['key']}' "
            f"{str(local_dir.resolve())}/ "
            f"{user}@{client_ip}:{remote_dir}/"
        )

        print("Running command:", copy_cmd)
        subprocess.run(copy_cmd, check=True, shell=True)

        remote_run = (
            f"cd {shlex.quote(remote_dir)}; "
            f"mvn clean package -pl {shlex.quote(interface)} -am; "
            f"{shlex.quote(remote_bin)} load {shlex.quote(interface)} "
            f"-P {shlex.quote(workload_path)} -p {shlex.quote(endpoint_name)}={shlex.quote(addr_list[0])} > /dev/null; "
            f"{shlex.quote(remote_bin)} run {shlex.quote(interface)} "
            f"-P {shlex.quote(workload_path)} -p {shlex.quote(endpoint_name)}={shlex.quote(addr_list[0])}; "
        )

        run_cmd = [
            "ssh",
            "-i", str(ssh['key']),
            f"{user}@{client_ip}",
            "bash -c",
            shlex.quote(remote_run)
        ]

        # Load & run YCSB
        print("Running command:", " ".join(run_cmd))
        process = subprocess.Popen(
            run_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Redirect stderr to stdout for a single output stream
            text=True,
        )

        live_output = []
        for line in process.stdout:
            print(line, end='')
            live_output.append(line)

        return_code = process.wait()

        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, process.args, output="".join(live_output))

    parsed = parse_ycsb_output(live_output)
    print(json.dumps(parsed, indent=2))

    keep_keys = {"READ", "UPDATE", "DELETE", "INSERT", "OVERALL"}
    result = {k: parsed[k] for k in keep_keys if k in parsed}
    insert_ycsb_output(selected_project.name, protocol, WORKLOADS[num-1], result)


def insert_ycsb_output(project_name, protocol, workload, result):
    with open(DATA, "r") as f:
        data = json.load(f)

    workload_data = {
        "name": workload["text"],
        "type": workload["type"],
        "num_of_nodes": int(os.getenv("NUM_OF_NODES")),
        "result": result
    }

    protocol_data = {
        "name": protocol["name"],
        "language": protocol.get("language", ""),
        "consistency": protocol.get("consistency", ""),
        "persistency": protocol.get("persistency", ""),
        "commit": protocol.get("commit", ""),
        "workloads": [workload_data]
    }

    project_data = {
        "project": project_name,
        "repo": protocol.get("repo", ""),
        "protocols": [protocol_data]
    }

    # Check if project already exists
    selected_project = next((p for p in data
                             if p["project"] == project_name
                             and p["repo"] == protocol.get("repo", "")
                             ), None)
    if selected_project is None:
        print(f"{project_name} doesn't exist. Adding new project")
        data.append(project_data)
        write_to_json(data, project_name, protocol, workload)
        return

    # Check if protocol already exists
    protocols = selected_project["protocols"]
    selected_protocol = next((p for p in protocols
                              if p["name"] == protocol["name"]
                              and p["language"] == protocol.get("language", "")
                              and p["consistency"] == protocol.get("consistency", "")
                              and p["persistency"] == protocol.get("persistency", "")
                              and p["commit"] == protocol.get("commit", "")
                              ), None)
    if selected_protocol is None:
        print(f"{protocol["name"]} doesn't exist. Adding new protocol")
        protocols.append(protocol_data)
        write_to_json(data, project_name, protocol, workload)
        return

    # Check if workload already exists
    workloads = selected_protocol["workloads"]
    selected_workload = next((w for w in workloads
                              if w["name"] == workload["text"]
                              and w["type"] == workload["type"]
                              ), None)
    if selected_workload is None:
        print(f"{workload["text"]} doesn't exist. Adding new workload")
        workloads.append(workload_data)
        write_to_json(data, project_name, protocol, workload)
        return

    # Workload already exists
    print(f"{workload["text"]} already exist. Adding overriding result")
    selected_workload["result"] = result
    write_to_json(data, project_name, protocol, workload)


def write_to_json(data, project_name, protocol, workload):
    with open(DATA, "w") as f:
        json.dump(data, f, indent=2)

    output = (
        f"{workload["type"]} {workload["text"]} benchmark for "
        f"{project_name}:{protocol["name"]} ({protocol.get("commit", "")}) "
        f"has been added to {DATA}."
    )
    print(output)

    '''
    already_exists = False
    for item in data:
        if (item["project"] == selected_project.name
            and item["protocol"] == protocol["name"]
                and item["workload"] == ):
            already_exists = True
            item["result"] = result

    if not already_exists:
        data.append({
            "project": selected_project.name,
            "protocol": protocol['name'],
            "language": protocol.get("language", ""),
            "workload": WORKLOADS[num-1],
            "result": result,
            "consistency": protocol.get("consistency", ""),
            "persistency": protocol.get("persistency", ""),
        })
        '''


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
    filename = os.getenv("SSH_KEY")
    ssh_key = Path.cwd() / filename
    username = os.getenv("REMOTE_USERNAME")
    client_ip = os.getenv("CLIENT_IP")

    ssh = {"key": ssh_key, "username": username, "filename": filename}

    nodes = [{"private": os.getenv(f"PRIVATE_IP{i}"),
             "public": os.getenv(f"PUBLIC_IP{i}")}
             for i in range(1, num_of_nodes+1)]

    print("SSH:", ssh)
    print("Addresses:", nodes)
    main(nodes, ssh)

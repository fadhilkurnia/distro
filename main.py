from pathlib import Path
import importlib
import sys
import subprocess
import json

from src.utils import helper

YCSB_DIR = Path("./src/ycsb")
YCSB_BIN = Path("./bin/ycsb")
YCSB_WORKLOAD_DIR = Path("./workloads")
WORKLOADS = ["workloada", "workloadb", "workloadc", "workloadd"]
DATA = "data.json"

selected_project = None


def main():
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
    module.main(run_ycsb)


def run_ycsb(protocol, interface):
    global selected_project
    options = [{"num": i, "text": name}
               for i, name in enumerate(WORKLOADS, start=1)]
    num = helper.get_option(1, len(options), options)

    workload_path = YCSB_WORKLOAD_DIR / WORKLOADS[num-1]

    subprocess.run(
        [YCSB_BIN, "load", interface, "-P", workload_path],
        cwd=YCSB_DIR)

    result = subprocess.run(
        [YCSB_BIN, "run", interface, "-P", workload_path],
        cwd=YCSB_DIR,
        stdout=subprocess.PIPE,
        stderr=None,
        text=True)

    parsed = parse_ycsb_output(result.stdout.splitlines())
    '''
    print(f"Project: {selected_project.name}")
    print(f"Protocol: {protocol}")
    print(f"{workload_path.name} output:")
    '''
    print(json.dumps(parsed, indent=2))

    keep_keys = {"READ", "UPDATE", "DELETE", "INSERT", "OVERALL"}
    result = {k: parsed[k] for k in keep_keys if k in parsed}

    with open(DATA, "r") as f:
        data = json.load(f)

    already_exists = False
    for item in data:
        if (item["project"] == selected_project.name
                and item["protocol"] == protocol
                and item["workload"] == workload_path.name):
            already_exists = True
            item["result"] = result

    if not already_exists:
        data.append({
            "project": selected_project.name,
            "protocol": protocol,
            "workload": workload_path.name,
            "result": result
        })

    print("data:", data)
    with open(DATA, "w") as f:
        json.dump(data, f, separators=(",", ":"))


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


if __name__ == "__main__":
    main()

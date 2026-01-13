from abc import ABC, abstractmethod
from dotenv import load_dotenv
import inspect
import json
import logging
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import warnings

from src.utils import helper
from src.utils.dependency import (
    ProbeEntry,
    run_local_probe,
    run_remote_probe,
    extract_version,
    version_satisfies,
)


load_dotenv()

YCSB_DIR = Path("./src/ycsb")
YCSB_BIN = YCSB_DIR / "bin" / "ycsb"

WORKLOADS = [
    {
        "num": 1,
        "type": "single-client",
        "name": "Workload A: Update heavy workload",
        "filename": "workloada",
        "request_distribution": "zipfian",
        "read_proportion": 0.5,
        "update_proportion": 0.5,
        "read_modify_write_proportion": 0,
        "insert_proportion": 0,
    }, {
        "num": 2,
        "type": "single-client",
        "name": "Workload B: Read mostly workload",
        "filename": "workloadb",
        "request_distribution": "zipfian",
        "read_proportion": 0.95,
        "update_proportion": 0.05,
        "read_modify_write_proportion": 0,
        "insert_proportion": 0,
    }, {
        "num": 3,
        "type": "single-client",
        "name": "Workload C: Read only",
        "filename": "workloadc",
        "request_distribution": "zipfian",
        "read_proportion": 1,
        "update_proportion": 0,
        "read_modify_write_proportion": 0,
        "insert_proportion": 0,
    }, {
        "num": 4,
        "type": "single-client",
        "name": "Workload D: Read latest workload",
        "filename": "workloadd",
        "request_distribution": "latest",
        "read_proportion": 0.95,
        "update_proportion": 0,
        "read_modify_write_proportion": 0,
        "insert_proportion": 0.05,
    }, {
        "num": 5,
        "type": "single-client",
        "name": "Workload F: Read-modify-write workload",
        "filename": "workloadf",
        "request_distribution": "zipfian",
        "read_proportion": 0.5,
        "update_proportion": 0,
        "read_modify_write_proportion": 0.05,
        "insert_proportion": 0,
    }, {
        "num": 6,
        "type": "single-client",
        "name": "Update only workload",
        "filename": "updateonly",
        "request_distribution": "zipfian",
        "read_proportion": 0,
        "update_proportion": 1,
        "read_modify_write_proportion": 0,
        "insert_proportion": 0,
    }, {
        "num": 7,
        "type": "single-client",
        "name": "Read mostly V2 workload",
        "filename": "readmostlyv2",
        "request_distribution": "zipfian",
        "read_proportion": 0.8,
        "update_proportion": 0.2,
        "read_modify_write_proportion": 0,
        "insert_proportion": 0,
    }
]


class Launcher(ABC):
    ROOT_DIR = Path(".").resolve()

    def __init__(self, nodes, ssh, client_ip, num_of_nodes, output_file):
        helper.validate_nodes(nodes)
        helper.validate_ssh(ssh)

        self.nodes = nodes

        self.ssh_key = ssh["key"].resolve()
        self.user = ssh["username"]
        self.ssh_filename = ssh["filename"]

        subclass_file = inspect.getfile(self.__class__)
        self.local_dir = Path(subclass_file).parent.resolve()

        self.client_ip = client_ip
        self.num_of_nodes = num_of_nodes
        self.output_file = output_file

    @property
    def project_name(self) -> str:
        return self._project_name

    @project_name.setter
    def project_name(self, name):
        if not isinstance(name, str):
            raise ValueError("Project name must be of type 'str'")

        self._project_name = name

    @property
    def project_commit(self) -> str:
        return self._project_commit

    @project_commit.setter
    def project_commit(self, commit):
        if not isinstance(commit, str):
            raise ValueError("Project commit must be of type 'str'")

        self._project_commit = commit

    @property
    def project_repository(self) -> str:
        return self._project_repository

    @project_repository.setter
    def project_repository(self, repository):
        if not isinstance(repository, str):
            raise ValueError("Project repository must be of type 'str'")

        self._project_repository = repository

    @property
    def repo_dir_path(self) -> str:
        return self._repo_dir_path

    @repo_dir_path.setter
    def repo_dir_path(self, path):
        if not isinstance(path, str):
            raise ValueError("Path to repo directory must be of type 'str'")

        self._repo_dir_path = path

    @property
    def selected_protocol(self):
        return self._selected_protocol

    @selected_protocol.setter
    def selected_protocol(self, protocol):
        if not isinstance(protocol, dict):
            raise ValueError("Selected protocol must be of type 'dict'")

        required_keys = ["name", "language", "consistency", "persistency"]
        for key in required_keys:
            if key not in protocol:
                raise ValueError(
                    f"Selected protocol missing required key: '{key}'")

            if not isinstance(protocol[key], str):
                raise ValueError(
                    f"{key} in selected protocol must be of type 'str'")

        self._selected_protocol = protocol

    @property
    def ycsb_interface(self) -> str:
        return self._ycsb_interface

    @ycsb_interface.setter
    def ycsb_interface(self, interface):
        if not isinstance(interface, str):
            raise ValueError("YCSB interface must be of type 'str'")

        self._ycsb_interface = interface

    @property
    def ycsb_endpoint(self) -> str:
        return self._ycsb_endpoint

    @ycsb_endpoint.setter
    def ycsb_endpoint(self, endpoint):
        if not isinstance(endpoint, str):
            raise ValueError("YCSB endpoint must be of type 'str'")

        self._ycsb_endpoint = endpoint

    @abstractmethod
    def launch(self):
        pass

    @abstractmethod
    def build(self, *args, **kwargs):
        pass

    @abstractmethod
    def generate_config(self, *args, **kwargs):
        warnings.warn(
            f"{self.__class__.__name__}.generate_config() is not implemented and should be overridden",
            UserWarning,
            stacklevel=2
        )
        pass

    def check_dependency(self, entry: ProbeEntry, requirement: str, host=None):
        if host is None:
            host = "localhost"
            success, output = run_local_probe(entry["probe"])
        else:
            success, output = run_remote_probe(entry["probe"], host, self.user, self.ssh_key)

        if not success:
            logging.error(f"Missing dependency '{entry['name']}' in {host} (requires version {requirement})")
            raise RuntimeError(output)

        host_version = extract_version(output, entry["version_regex"])
        if host_version is None:
            logging.error(f"Unable to get version for dependency '{entry['name']}' in {host}")
            raise RuntimeError()

        if version_satisfies(host_version, requirement):
            logging.info(f"{host} has dependency '{entry['name']}' version {host_version} (requires version {requirement})")
            return True

        logging.error(f"{host} has dependency '{entry["name"]}' version {host_version} (requires version {requirement})")
        sys.exit(f"{host} did not meet {entry["name"]} version requirement. Exiting distrobench")

    def ensure_repo_exists(self, dir_path, repo_url, commit=None):
        path = helper.get_repo_path_in_directory(dir_path, repo_url)

        if not path:
            logging.info(
                f"'{repo_url}' repo doesn't exist. Cloning the repository")
            git_clone_cmd = (
                f"cd {self.local_dir} && "
                f"git clone {repo_url}"
            )
            self.local_run_cmd(git_clone_cmd)
            path = helper.get_repo_path_in_directory(dir_path, repo_url)

        repo_commit = helper.get_commit(path)
        matching_commit = False
        if commit:
            matching_commit = repo_commit == commit

            logging.debug(f"Checking out commit: {commit}")
            git_reset_cmd = (
                f"cd {path} && "
                f"git reset --hard {commit}"
            )
            self.local_run_cmd(git_reset_cmd)

        return path, matching_commit

    def _build_ycsb(self):
        local_ycsb_dir = str(YCSB_DIR.resolve())

        if self.client_ip == "127.0.0.1":
            ycsb_dir = local_ycsb_dir
        else:
            ycsb_dir = "~/distro/ycsb"
            mkdir_cmd = f"mkdir -p {ycsb_dir}"
            self.remote_run_cmd(self.client_ip, mkdir_cmd)
            self.remote_rsync(self.client_ip, f"{local_ycsb_dir}/", ycsb_dir)

        build_cmd = (
            f"cd {ycsb_dir} && "
            f"mvn clean package -pl {self.ycsb_interface} -am -DskipTests"
        )

        if self.client_ip == "127.0.0.1":
            self.local_run_cmd(build_cmd)
            # subprocess.run(build_cmd, check=True, shell=True)
        else:
            self.remote_run_cmd(self.client_ip, build_cmd, True)

    def _load_ycsb(self, addr_list, record_count, args=None):
        if not addr_list:
            raise ValueError("addr_list cannot be empty")

        if self.client_ip == "127.0.0.1":
            ycsb_dir = str(YCSB_DIR.resolve())
        else:
            ycsb_dir = "~/distro/ycsb"

        ycsb_bin = f"{ycsb_dir}/bin/ycsb"
        load_insert_retry_limit = os.getenv("LOAD_INSERT_RETRY_LIMIT", 10)
        load_insert_retry_interval = os.getenv("LOAD_INSERT_RETRY_INTERVAL", 1)
        field_count = os.getenv("FIELD_COUNT", 1)
        field_length = os.getenv("FIELD_LENGTH", 100)
        load_thread_count = os.getenv("LOAD_THREAD_COUNT", 64)
        benchmark_seed = os.getenv("BENCHMARK_SEED", 42)

        logging.info(f"Loading {record_count} YCSB key-value pairs to {self.project_name}-{self.selected_protocol["name"]} using {load_thread_count} threads")

        load_cmd = (
            f"cd {ycsb_dir} && "
            f"{ycsb_bin} load {self.ycsb_interface} "
            "-p workload=site.ycsb.workloads.CoreWorkload "
            "-p insertorder=hashed "
            f"-p {self.ycsb_endpoint}={addr_list[0]} "
            f"-p recordcount={record_count} "
            f"-p core_workload_insertion_retry_limit={load_insert_retry_limit} "
            f"-p core_workload_insertion_retry_interval={load_insert_retry_interval} "
            f"-p fieldcount={field_count} "
            f"-p fieldlength={field_length} "
            f"-p threadcount={load_thread_count} "
            f"-p seed={benchmark_seed} "
        )

        if args is not None:
            load_cmd += f"-p {args}"

        if self.client_ip == "127.0.0.1":
            self.local_run_cmd(load_cmd)
        else:
            self.remote_run_cmd(self.client_ip, load_cmd)

    def _run_ycsb(self, addr_list, workload, args=None):
        if not addr_list:
            raise ValueError("addr_list cannot be empty")

        if self.client_ip == "127.0.0.1":
            ycsb_dir = str(YCSB_DIR.resolve())
        else:
            ycsb_dir = "~/distro/ycsb"

        ycsb_bin = f"{ycsb_dir}/bin/ycsb"
        workload_path = f"{ycsb_dir}/workloads/{workload["filename"]}"

        logging.info(f"Running YCSB {workload["name"]} benchmark on {self.project_name}-{self.selected_protocol["name"]} using {workload["thread_count"]} threads")

        run_cmd = (
            f"cd {ycsb_dir} && "
            f"{ycsb_bin} run {self.ycsb_interface} "
            f"-P {workload_path} -p {self.ycsb_endpoint}={addr_list[0]} "
            f"-p recordcount={workload["record_count"]} "
            f"-p operationcount={workload["operation_count"]} "
            f"-p insertproportion={workload["insert_proportion"]} "
            f"-p readproportion={workload["read_proportion"]} "
            f"-p updateproportion={workload["update_proportion"]} "
            f"-p readmodifywriteproportion={workload["read_modify_write_proportion"]} "
            f"-p readproportion={workload["read_proportion"]} "
            f"-p requestdistribution={workload["request_distribution"]} "
            f"-p fieldcount={workload["field_count"]} "
            f"-p fieldlength={workload["field_length"]} "
            f"-p seed={workload["seed"]} "
            f"-p threadcount={workload["thread_count"]} "
        )

        if args is not None:
            run_cmd += f"-p {args}"

        if self.client_ip != "127.0.0.1":
            run_cmd = [
                "ssh", "-i", self.ssh_key,
                f"{self.user}@{self.client_ip}",
                "bash -c", shlex.quote(run_cmd)
            ]
            process = subprocess.Popen(
                run_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        else:
            process = subprocess.Popen(
                run_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                shell=True
            )

        output = []
        for line in process.stdout:
            print(line, end='')
            output.append(line)

        return_code = process.wait()

        if return_code != 0:
            raise subprocess.CalledProcessError(
                return_code, process.args, output="".join(output))

        return output

    def _store_ycsb_result(self, result, workload):
        parsed_data = {}
        for line in result:
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
                if section not in parsed_data:
                    parsed_data[section] = {}
                parsed_data[section][key] = value
            except Exception:
                continue

        # Insert Parsed Data
        print(json.dumps(parsed_data, indent=2))
        keep_keys = {"READ","READ-FAILED", "UPDATE", "UPDATE-FAILED", "DELETE", "DELETE-FAILED", "INSERT", "INSERT-FAILED", "OVERALL"}
        final_result = {k: parsed_data[k]
                        for k in keep_keys if k in parsed_data}

        with open(self.output_file, "r") as f:
            data = json.load(f)

        result_data = {
            "thread_count": workload["thread_count"],
            "result": final_result
        }

        workload_data = {
            "name": workload["name"],
            "type": workload["type"],
            "operation_count": workload["operation_count"],
            "record_count": workload["record_count"],
            "request_distribution": workload["request_distribution"],
            "read_proportion": workload["read_proportion"],
            "update_proportion": workload["update_proportion"],
            "read_modify_write_proportion": workload["read_modify_write_proportion"],
            "insert_proportion": workload["insert_proportion"],
            "num_of_nodes": self.num_of_nodes,
            "seed": workload["seed"],
            "results": [result_data]
        }

        protocol_data = {
            "name": self.selected_protocol["name"],
            "language": self.selected_protocol.get("language", ""),
            "consistency": self.selected_protocol.get("consistency", ""),
            "persistency": self.selected_protocol.get("persistency", ""),
            "commit": self.project_commit,
            "workloads": [workload_data]
        }

        project_data = {
            "project": self.project_name,
            "repo": self.project_repository,
            "protocols": [protocol_data]
        }

        # Check if project already exists
        selected_project = next((p for p in data
                                 if p["project"] == self.project_name
                                 and p["repo"] == self.project_repository
                                 ), None)
        if selected_project is None:
            logging.info(
                f"{self.project_name} doesn't exist. Adding new project")
            data.append(project_data)
            helper.write_to_json(self.output_file, data, self.project_name,
                                 self.selected_protocol["name"], workload, self.project_commit)
            return

        # Check if protocol already exists
        protocols = selected_project["protocols"]
        selected_protocol = next((p for p in protocols
                                  if p["name"] == self.selected_protocol["name"]
                                  and p["language"] == self.selected_protocol.get("language", "")
                                  and p["consistency"] == self.selected_protocol.get("consistency", "")
                                  and p["persistency"] == self.selected_protocol.get("persistency", "")
                                  and p["commit"] == self.project_commit
                                  ), None)
        if selected_protocol is None:
            logging.info(
                f"{self.selected_protocol["name"]} doesn't exist. Adding new protocol")
            protocols.append(protocol_data)
            helper.write_to_json(self.output_file, data, self.project_name,
                                 self.selected_protocol["name"], workload, self.project_commit)
            return

        # Check if workload already exists
        workloads = selected_protocol["workloads"]
        selected_workload = next((w for w in workloads
                                  if w["name"] == workload["name"]
                                  and w["type"] == workload["type"]
                                  and w["operation_count"] == workload["operation_count"]
                                  and w["record_count"] == workload["record_count"]
                                  and w["request_distribution"] == workload["request_distribution"]
                                  and w["read_proportion"] == workload["read_proportion"]
                                  and w["update_proportion"] == workload["update_proportion"]
                                  and w["read_modify_write_proportion"] == workload["read_modify_write_proportion"]
                                  and w["insert_proportion"] == workload["insert_proportion"]
                                  and w["num_of_nodes"] == self.num_of_nodes
                                  and w["seed"] == workload["seed"]
                                  ), None)
        if selected_workload is None:
            logging.info(f"{workload["name"]} doesn't exist. Adding new workload")
            workloads.append(workload_data)
            helper.write_to_json(self.output_file, data, self.project_name,
                                 self.selected_protocol["name"], workload, self.project_commit)
            return

        # Check if thread count result already exists
        results_data = selected_workload["results"]
        selected_result = next((r for r in results_data
                                if r["thread_count"] == workload["thread_count"]
                                ), None)
        if selected_result is None:
            logging.info(f"{workload["thread_count"]} thread count doesn't exist. Adding new result on this thread count")
            results_data.append(result_data)
            helper.write_to_json(self.output_file, data, self.project_name,
                                 self.selected_protocol["name"], workload, self.project_commit)
            return

        # Thread count result already exists
        logging.info(f"{workload["thread_count"]} thread count already exist. Overriding previous result")
        selected_result["result"] = final_result
        helper.write_to_json(self.output_file, data, self.project_name,
                             self.selected_protocol["name"], workload, self.project_commit)

    def ycsb(self, addr_list, args=None) -> None:
        self._build_ycsb()

        default_record_count = int(os.getenv("DEFAULT_RECORD_COUNT", 1000000))
        default_operation_count = int(os.getenv("DEFAULT_OPERATION_COUNT", 500000))
        '''
        record_count = helper.get_positive_num("Enter Record Count", default_record_count)
        operation_count = helper.get_positive_num("Enter Operation Count", default_operation_count)
        '''
        record_count = default_record_count
        operation_count = default_operation_count

        # Load key-value pairs first before running benchmark
        #self._load_ycsb(addr_list, record_count, args)

        # Run Workload
        workload_text = [{
            "num": item["num"],
            "text": (
                f"[{item["type"]}] {item["name"]} ({item["request_distribution"]})\n"
                f"{int(item["insert_proportion"] * 100):10d}% insert\n"
                f"{int(item["read_proportion"] * 100):10d}% read\n"
                f"{int(item["update_proportion"] * 100):10d}% update\n"
                f"{int(item["read_modify_write_proportion"] * 100):10d}% read-modify-write"
            )
        } for item in WORKLOADS]
        workload_text.append({"num": 0,
                              "text": "Stop Benchmark"})

        field_count = os.getenv("FIELD_COUNT", 1)
        field_length = os.getenv("FIELD_LENGTH", 100)
        benchmark_seed = os.getenv("BENCHMARK_SEED", 42)
        thread_count_str = os.getenv("BENCHMARK_THREAD_COUNTS", "8,16,32,64,128")
        thread_counts = [int(item.strip()) for item in thread_count_str.split(',')]

        '''
        while True:
            num = helper.get_option(0, len(workload_text), workload_text)

            if num == 0:
                return
        '''
        #for num in range(1, 6):
        #for num in range(1, 3):
        #for num in [6, 7]:
        #for num in [7]:
        #for num in [3, 6, 7]:
        for num in [6]:
            selected_workload = WORKLOADS[num-1]
            selected_workload["operation_count"] = operation_count
            selected_workload["record_count"] = record_count
            selected_workload["field_count"] = field_count
            selected_workload["field_length"] = field_length
            selected_workload["seed"] = benchmark_seed

            # Run same workload Multiple times with different thread counts
            for thread_count in thread_counts:
                selected_workload["thread_count"] = thread_count
                result = self._run_ycsb(addr_list, selected_workload, args)
                self._store_ycsb_result(result, selected_workload)

                break_duration = 20
                #break_duration = 5
                logging.info(f"Taking {break_duration} second break after running {selected_workload["name"]} with {thread_count} thread(s)")
                time.sleep(break_duration)
                '''
                while (True):
                    user_input = input("Do you want to continue? (y/n): ").strip().lower()
                    if user_input == 'y':
                        print("Continuing the process...")
                        break
                '''
        cmd = "notify-send 'benchmark done' --urgency=critical"
        subprocess.run(cmd, check=True, shell=True)

    def local_run_cmd(self, cmd):
        logging.debug(f"Running: {cmd}")
        subprocess.run(cmd, check=True, shell=True)

    def remote_run_cmd(self, host, cmd, silent=False):
        run_cmd = (
            f"ssh -i {str(self.ssh_key)} {self.user}@{host} "
            f"{shlex.quote(cmd)}"
        )
        logging.debug(f"Running: {run_cmd}")

        run_args = {
            'check': True,
            'shell': True
        }
        if silent:
            run_args['stdout'] = subprocess.DEVNULL
            run_args['stderr'] = subprocess.DEVNULL

        subprocess.run(run_cmd, **run_args)

    def remote_rsync(self, host, source_files, target_dir):
        rsync_cmd = (
            f"rsync -avz -e 'ssh -i {self.ssh_key}' "
            f"{source_files} "
            f"{self.user}@{host}:{target_dir}/"
        )

        logging.debug(f"Running: {rsync_cmd}")
        subprocess.run(rsync_cmd, check=True, shell=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

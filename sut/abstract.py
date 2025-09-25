from abc import ABC, abstractmethod
import inspect
import json
import logging
from pathlib import Path
import shlex
import subprocess
import sys
import warnings

from src.utils import helper
from src.utils.dependency import (
    ProbeEntry,
    run_local_probe,
    run_remote_probe,
    extract_version,
    version_satisfies,
)

YCSB_DIR = Path("./src/ycsb")
YCSB_BIN = YCSB_DIR / "bin" / "ycsb"
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
        self.remote_dir = f"/home/{self.user}"

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
            logging.error(f"Missing dependency '{entry["name"]}' in {host} (requires version {requirement})")
            raise RuntimeError(output)

        host_version = extract_version(output, entry["version_regex"])
        if host_version is None:
            logging.error(f"Unable to get version for dependency '{entry["name"]}' in {host}")
            raise RuntimeError()

        if version_satisfies(host_version, requirement):
            logging.info(f"{host} has dependency '{entry["name"]}' version {host_version} (requires version {requirement})")
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
            ycsb_dir = f"/home/{self.user}/ycsb"
            self.remote_rsync(self.client_ip, f"{local_ycsb_dir}/", ycsb_dir)

        build_cmd = (
            f"cd {shlex.quote(ycsb_dir)} && "
            f"mvn clean package -pl {shlex.quote(self.ycsb_interface)} -am"
        )

        if self.client_ip == "127.0.0.1":
            self.local_run_cmd(build_cmd)
            # subprocess.run(build_cmd, check=True, shell=True)
        else:
            self.remote_run_cmd(self.client_ip, build_cmd, True)

    def _run_ycsb(self, addr_list, workload):
        if not addr_list:
            raise ValueError("addr_list cannot be empty")

        if self.client_ip == "127.0.0.1":
            ycsb_dir = str(YCSB_DIR.resolve())
        else:
            ycsb_dir = f"/home/{self.user}/ycsb"

        ycsb_bin = f"{ycsb_dir}/bin/ycsb"
        workload_path = f"{ycsb_dir}/workloads/{workload["text"]}"
        run_cmd = (
            f"cd {shlex.quote(ycsb_dir)} && "
            f"{shlex.quote(ycsb_bin)} load {shlex.quote(self.ycsb_interface)} "
            f"-P {shlex.quote(workload_path)} -p {shlex.quote(self.ycsb_endpoint)
                                                  }={shlex.quote(addr_list[0])} > /dev/null && "
            f"{shlex.quote(ycsb_bin)} run {shlex.quote(self.ycsb_interface)} "
            f"-P {shlex.quote(workload_path)} -p {shlex.quote(self.ycsb_endpoint)
                                                  }={shlex.quote(addr_list[0])}"
        )

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
        keep_keys = {"READ", "UPDATE", "DELETE", "INSERT", "OVERALL"}
        final_result = {k: parsed_data[k]
                        for k in keep_keys if k in parsed_data}

        with open(self.output_file, "r") as f:
            data = json.load(f)

        workload_data = {
            "name": workload["text"],
            "type": workload["type"],
            "num_of_nodes": self.num_of_nodes,
            "result": final_result
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
                                  if w["name"] == workload["text"]
                                  and w["type"] == workload["type"]
                                  and w["num_of_nodes"] == self.num_of_nodes
                                  ), None)
        if selected_workload is None:
            logging.info(
                f"{workload["text"]} doesn't exist. Adding new workload")
            workloads.append(workload_data)
            helper.write_to_json(self.output_file, data, self.project_name,
                                 self.selected_protocol["name"], workload, self.project_commit)
            return

        # Workload already exists
        logging.info(
            f"{workload["text"]} already exist. Overriding previous result")
        selected_workload["result"] = final_result
        helper.write_to_json(self.output_file, data, self.project_name,
                             self.selected_protocol["name"], workload, self.project_commit)

    def ycsb(self, addr_list) -> None:
        self._build_ycsb()

        num = helper.get_option(1, len(WORKLOADS), WORKLOADS)
        selected_workload = WORKLOADS[num-1]
        result = self._run_ycsb(addr_list, selected_workload)

        self._store_ycsb_result(result, selected_workload)

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

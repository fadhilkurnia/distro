import importlib.util
import inspect
import logging
import os
import sys

from dotenv import load_dotenv
from pathlib import Path
from src.utils import helper
from sut.abstract import Launcher


def main(nodes, ssh, client_ip, num_of_nodes, output_file):
    logging.info("Starting Distrobench...")
    SYSTEMS_UNDER_TEST_DIR = os.path.join(os.path.dirname(__file__), "sut")
    launchers = get_launchers(SYSTEMS_UNDER_TEST_DIR)

    max_project_name = max(len(item["project"]) for item in launchers)
    launcher_options = [{
        "num": i+1,
        "text": f"{item["project"].ljust(max_project_name)} - {item["repo"]}",
        "module": item["module"]
    } for i, item in enumerate(launchers)]

    num = helper.get_option(1, len(launcher_options), launcher_options, f"\n[Distrobench] Pick a launcher (1-{len(launcher_options)}):")
    selected_launcher_module = launcher_options[num-1]["module"]

    LauncherClass = None
    for name, obj in inspect.getmembers(selected_launcher_module):
        # Check if the member is a class, a subclass of Launcher, and not the abstract class itself
        if inspect.isclass(obj) and issubclass(obj, Launcher) and obj is not Launcher:
            LauncherClass = obj
            break

    if not LauncherClass:
        raise TypeError(f"Could not find a concrete subclass of Launcher in module {selected_launcher_module.__name__}")

    launcher_instance = LauncherClass(nodes, ssh, client_ip, num_of_nodes, output_file)
    launcher_instance.launch()


def get_launchers(base_dir):
    launchers = []
    for subdir in os.listdir(base_dir):
        subdir_path = os.path.join(base_dir, subdir)

        if os.path.isdir(subdir_path):
            launcher_path = os.path.join(subdir_path, "launcher.py")

            if not os.path.exists(launcher_path):
                logging.warning(f"No 'launcher.py' found in {subdir_path}")
                continue

            # Dynamically import the 'launcher.py' module
            module_name = f"{subdir}.launcher"
            spec = importlib.util.spec_from_file_location(module_name, launcher_path)
            launcher_module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = launcher_module
            spec.loader.exec_module(launcher_module)

            launcher = {
                "module": launcher_module,
                "project": subdir,
                "path": subdir_path,
                "repo": launcher_module.REPO
            }
            launchers.append(launcher)

    return launchers


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] [%(levelname)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    num_of_nodes = int(os.getenv("NUM_OF_NODES"))
    nodes = [{"private_ip": os.getenv(f"PRIVATE_IP{i}"),
             "public_ip": os.getenv(f"PUBLIC_IP{i}")}
             for i in range(1, num_of_nodes+1)]
    ssh = {"key": Path.cwd() / os.getenv("SSH_KEY"),
           "username": os.getenv("REMOTE_USERNAME"),
           "filename": os.getenv("SSH_KEY")
           }
    client_ip = os.getenv("CLIENT_IP")
    output_file = os.getenv("OUTPUT_FILE", "data.local.json")

    logging.info(f"Nodes ({num_of_nodes})\t\t: {[n["public_ip"] for n in nodes]}")
    logging.info(f"SSH Key File\t: {ssh["filename"]}")
    logging.info(f"Client IP\t\t: {client_ip}")
    logging.info(f"Output File\t: {output_file}")

    main(nodes, ssh, client_ip, num_of_nodes, output_file)

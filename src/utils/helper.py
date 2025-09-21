import sys
import subprocess
import json


def get_option(min, max, opts, header="\nOptions:") -> int:
    """
    Print opts to stdout then gets user number input.

    :param min: Smallest index for the option
    :type min: int
    :param max: Largest index for the option
    :type max: int
    :param opts: List of options
    :type opts: { num: int, text: str }
    """
    while True:
        print(header)
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


def get_commit(path):
    cmd = f"cd {path} && git rev-parse HEAD"
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"{path} is not a git repository")

    return result.stdout.strip()


def write_to_json(file, data, project_name, protocol_name, workload, commit):
    with open(file, "w") as f:
        json.dump(data, f, indent=2)

    output = (
        f"{workload["type"]} {workload["text"]} benchmark for "
        f"{project_name}:{protocol_name} ({commit}) "
        f"has been added to {file}."
    )
    print(output)

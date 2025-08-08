import sys


def get_option(min, max, opts) -> int:
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
        print("\nOptions:")
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

import argparse
import os
from subprocess import run
from pkg_resources import resource_string


def pack_docker():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", type=str, help="output file")
    parser.add_argument("-r", "--require", action="store_true",
                        help="generate requirements.txt")
    args = parser.parse_args()

    if os.path.exists("requirements.txt"):
        install_requirements = ""
    elif args.require:
        with open("requirements.txt", "w") as f:
            run(["pip", "freeze"], stdout=f)
        install_requirements = ""
    else:
        install_requirements = "#"

    temp = resource_string(__name__, "share/dockerfile/template").decode()
    content = temp.format(install_requirements=install_requirements)
    if args.output:
        with open(args.output, "w") as f:
            f.write(content)
    else:
        print(content)

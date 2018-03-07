import argparse
import os
from subprocess import run
from pkg_resources import resource_string
from ..common.consts import TOOL_CMD_PIP

FILE_REQUIREMENTS = "requirements.txt"
FILE_DOCKER_TEMPLATE = "../share/dockerfile/template"


def pack():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", type=str, help="output file")
    parser.add_argument("-r", "--require", action="store_true",
                        help="generate requirements.txt")
    args = parser.parse_args()

    if os.path.exists(FILE_REQUIREMENTS):
        install_requirements = ""
    elif args.require:
        with open(FILE_REQUIREMENTS, "w") as f:
            run([TOOL_CMD_PIP, "freeze"], stdout=f)
        install_requirements = ""
    else:
        install_requirements = "#"

    temp = resource_string(__name__, FILE_DOCKER_TEMPLATE).decode()
    content = temp.format(install_requirements=install_requirements)
    if args.output:
        with open(args.output, "w") as f:
            f.write(content)
    else:
        print(content)

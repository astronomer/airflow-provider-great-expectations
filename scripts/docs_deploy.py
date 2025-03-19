import subprocess
import sys

from packaging import version

import great_expectations_provider


def deploy_docs(deploy_type: str):
    _version = version.parse(great_expectations_provider.__version__)

    set_default = False

    if deploy_type == "release":
        # TODO: Re-enable this if else block when we are out of pre-release and mark a stable release 1.0.0
        # Tracking ticket: https://github.com/astronomer/airflow-provider-great-expectations/issues/187
        # if _version.pre is not None:
        #     command = ["mike", "deploy", "--push", "dev"]
        # else:
        command = [
            "mike",
            "deploy",
            "--push",
            "--update-aliases",
            str(_version),
            "latest",
        ]
        set_default = True
    else:
        command = ["mike", "deploy", "--push", "dev"]

    try:
        subprocess.run(command, capture_output=True, text=True, check=True)
        if set_default:
            default_command = ["mike", "set-default", "latest"]
            subprocess.run(default_command, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error deploying: {e.stderr}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Argument deploy type is required: 'dev' or 'release'")

    deploy_type = sys.argv[1]

    if deploy_type not in ["dev", "release"]:
        raise Exception(
            "Invalid argument provided. Valid deploy types are 'dev' or 'release'."
        )

    deploy_docs(deploy_type)

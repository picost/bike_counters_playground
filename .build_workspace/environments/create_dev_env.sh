#!/usr/bin/env bash
export DEV_ENV_NAME=dev
# Create a new virtual environment with the given name
uv venv /opt/$DEV_ENV_NAME
# Activate the virtual environment
source /opt/$DEV_ENV_NAME/bin/activate
apt update
apt install -y pandoc
# Install the required packages
uv pip install -r requirements_dev.txt
deactivate
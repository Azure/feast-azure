# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import os
import pathlib
from setuptools import find_packages

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# README file from repo root directory
repo_root = str(pathlib.Path(__file__).resolve().parent.parent)

# README file from Feast repo root directory
README_FILE = os.path.join(repo_root, "README.md")
with open(README_FILE, "r") as f:
    LONG_DESCRIPTION = f.read()

setup(
    name="feast-azure-provider",
    author="Microsoft",
    version="0.2.0",
    description="A Feast Azure Provider",
    URL="https://github.com/Azure/feast-azure",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    python_requires=">=3.7.0",
    packages=find_packages(exclude=("tests",)),
    install_requires=[
        "feast==0.15.0",
        "azure-storage-blob>=0.37.0",
        "azure-identity>=1.6.1" "SQLAlchemy>=1.4.19",
        "dill==0.3.4",
        "pyodbc>=4.0.30",
        "redis>=3.5.3",
        "redis-py-cluster>=2.1.3",
        "sqlalchemy>=1.4",
    ],
    extras_require={"dev": ["pytest", "mypy", "assertpy"]},
    # https://stackoverflow.com/questions/28509965/setuptools-development-requirements
    # Install dev requirements with: pip install -e .[dev]
    include_package_data=True,
    license="MIT",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)

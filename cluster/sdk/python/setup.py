# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import glob
import os
import re
import subprocess

from distutils.cmd import Command
from setuptools import find_packages

try:
    from setuptools import setup
    from setuptools.command.install import install
    from setuptools.command.develop import develop
    from setuptools.command.egg_info import egg_info
    from setuptools.command.sdist import sdist
    from setuptools.command.build_py import build_py

except ImportError:
    from distutils.core import setup
    from distutils.command.install import install
    from distutils.command.build_py import build_py

NAME = "feast-spark"
DESCRIPTION = "Spark extensions for Feast"
URL = "https://github.com/feast-dev/feast-spark"
AUTHOR = "Feast"
REQUIRES_PYTHON = ">=3.6.0"

REQUIRED = [
    "feast==0.9.5.2",
    "Click==7.*",
    "google-api-core==1.22.4",
    "google-cloud-bigquery==1.18.*",
    "google-cloud-storage==1.20.*",
    "google-cloud-core==1.0.*",
    "googleapis-common-protos==1.52.*",
    "google-cloud-bigquery-storage==0.7.*",
    "google-cloud-dataproc==2.0.2",
    "kubernetes==12.0.*",
    "grpcio-tools==1.31.0",
    "mypy-protobuf==2.5",
    "croniter==1.*",
    "azure-synapse-spark",
    "azure-synapse",
    "azure-identity",
    "azure-storage-file-datalake",
    "azure-storage-blob",
    "db-dtypes",
]

# README file from Feast repo root directory
repo_root = (
    subprocess.Popen(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE)
        .communicate()[0]
        .rstrip()
        .decode("utf-8")
)
README_FILE = os.path.join(repo_root, "README.md")
with open(os.path.join(README_FILE), "r") as f:
    LONG_DESCRIPTION = f.read()

# Add Support for parsing tags that have a prefix containing '/' (ie 'sdk/go') to setuptools_scm.
# Regex modified from default tag regex in:
# https://github.com/pypa/setuptools_scm/blob/2a1b46d38fb2b8aeac09853e660bcd0d7c1bc7be/src/setuptools_scm/config.py#L9
TAG_REGEX = re.compile(
    r"^(?:[\/\w-]+)?(?P<version>[vV]?\d+(?:\.\d+){0,2}[^\+]*)(?:\+.*)?$"
)


class BuildProtoCommand(Command):
    description = "Builds the proto files into python files."

    def initialize_options(self):
        import feast

        self.protoc = ["python", "-m", "grpc_tools.protoc"]  # find_executable("protoc")
        self.proto_folder = os.path.join(repo_root, "protos")
        self.this_package = os.path.dirname(__file__) or os.getcwd()
        self.feast_protos = os.path.join(os.path.dirname(feast.__file__), 'protos')
        self.sub_folders = ["api"]

    def finalize_options(self):
        pass

    def _generate_protos(self, path):
        proto_files = glob.glob(os.path.join(self.proto_folder, path))

        subprocess.check_call(self.protoc + [
                               '-I', self.proto_folder,
                               '-I', self.feast_protos,
                               '--python_out', self.this_package,
                               '--grpc_python_out', self.this_package,
                               '--mypy_out', self.this_package] + proto_files)

    def run(self):
        for sub_folder in self.sub_folders:
            self._generate_protos(f'feast_spark/{sub_folder}/*.proto')

        self._generate_protos('feast_spark/third_party/grpc/health/v1/*.proto')


class BuildCommand(build_py):
    """Custom build command."""

    def run(self):
        self.run_command('build_proto')
        build_py.run(self)


class DevelopCommand(develop):
    """Custom develop command."""

    def run(self):
        self.run_command('build_proto')
        develop.run(self)


setup(
    name=NAME,
    author=AUTHOR,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=("tests",)) + ['.'],
    install_requires=REQUIRED,
    # https://stackoverflow.com/questions/28509965/setuptools-development-requirements
    # Install dev requirements with: pip install -e .[dev]
    extras_require={
        "dev": ["mypy-protobuf==1.*", "grpcio-testing==1.*"],
        "validation": ["great_expectations==0.13.2", "pyspark==3.0.1"],
    },
    include_package_data=True,
    license="Apache",
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    entry_points={"console_scripts": ["feast-spark=feast_spark.cli:cli"]},
    use_scm_version={"root": "../", "relative_to": __file__, "tag_regex": TAG_REGEX},
    setup_requires=["setuptools_scm", "grpcio-tools==1.31.0", "google-auth==1.21.1", "feast==0.9.5.2", "mypy-protobuf==2.5"],
    cmdclass={
        "build_proto": BuildProtoCommand,
        "build_py": BuildCommand,
        "develop": DevelopCommand,
    },
)

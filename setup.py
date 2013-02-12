#!/usr/bin/env python

from distutils.command.build import build as Build
from distutils.core import setup
from distutils.errors import DistutilsExecError
import os.path
import subprocess


name = "pytypedbytes"

classifiers = [
    "Development Status :: 2 - Pre-Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 2",
    "Programming Language :: Python :: 2.6",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: Implementation :: CPython",
    ]


def read_long_description():
    with open("README.rst", "r") as f:
        long_description = f.read()
    return long_description


def get_version_from_git(match=None):
    """Return a version string for the current version.

    This function runs ``git --describe`` as a subprocess and captures
    the output."""
    args = ["git", "describe", "--always", "--dirty", "--debug"]
    if match is not None:
        args += ["--match", match]
    p = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    exit = p.wait()
    if exit != 0:
        raise DistutilsExecError(
            "Error running command '%s':\n%s" %
            (" ".join(args), p.stderr.read()))
    version = p.stdout.read().strip()
    return version


def construct_version_file_path():
    """Construct the path to the version file."""
    path = os.path.relpath(os.path.join(name, "version.py"))
    return path


def write_version_file():
    """Write the current version to a version file and return this
    version string."""
    version = get_version_from_git()
    path = construct_version_file_path()
    with open(path, "w") as f:
        f.write("# This file is autogenerated.\n")
        f.write("__version__ = '%s'\n" % (version))
    return version


class BuildAndWriteVersion(Build):
    """A subclass of the ``build`` class in distutils.command.build
    whose ``run()`` method that writes a version file."""
    description = __doc__

    def run(self):
        """Call the ``run()`` method of the superclass, then write the
        current version to a version file."""
        Build.run(self)
        _ = write_version_file()


if __name__ == "__main__":
    setup_kwargs = {
        "name": name,
        "version": get_version_from_git(),
        "author": "Steve M. Kim",
        "author_email": "steve@climate.com",
        "url": "https://github.com/TheClimateCorporation/py-typedbytes",
        "description": "A Python package for Hadoop typed bytes",
        "long_description": read_long_description(),
        "classifiers": classifiers,
        "license": "Apache 2.0",
        "cmdclass": {"build": BuildAndWriteVersion},
        "packages": [name, "%s.tests" % name],
        }
    setup(**setup_kwargs)

#!/usr/bin/env python

from distutils.core import setup
import os.path


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


def read_version():
    """Read the package version from source."""
    path = os.path.relpath(os.path.join(name, "__init__.py"))
    l = {}
    with open(path, "r") as f:
        exec(f, {}, l) # side effect mutation of l
    return l["__version__"]


if __name__ == "__main__":
    setup_kwargs = {
        "name": name,
        "version": read_version(),
        "author": "Steve M. Kim",
        "author_email": "steve@climate.com",
        "url": "https://github.com/TheClimateCorporation/py-typedbytes",
        "description": "A Python package for Hadoop typed bytes",
        "long_description": read_long_description(),
        "classifiers": classifiers,
        "license": "Apache 2.0",
        "packages": [name, "%s.tests" % name],
        }
    setup(**setup_kwargs)

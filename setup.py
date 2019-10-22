#!/usr/bin/env python

from setuptools import setup

VERSION = "1.0"

requires = [
    "simplejson==3.14.0",
    "slackclient==1.3.0",
    "python-dateutil==2.8.0",
    "cocore==1.2",
    "cocloud==0.2",
    "codb==1.2",
]

setup(
    name="maximilian",
    version=VERSION,
    author="Equinox",
    url="https://github.com/equinoxfitness/maximilian",
    scripts=[],
    install_requires=requires,
    classifiers=[
        "Development Status :: 1 - Production/Stable",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
    ],
)
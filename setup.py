#!/usr/bin/env python

import os
from setuptools import setup, find_packages

VERSION = open("VERSION").readline().rstrip()

def read_requirements():
    """Parse requirements from requirements.txt."""
    reqs_path = os.path.join('.', 'requirements.txt')
    with open(reqs_path, 'r') as f:
        requirements = [line.rstrip() for line in f]
    return requirements       

setup(
    name="rsqoop_runner",
    version=VERSION,
    url="https://bitbucket.org/equinoxfitness/maximilian3",
    scripts=[],
    py_modules=['rsqoop_runner'],
    install_requires=read_requirements()
)

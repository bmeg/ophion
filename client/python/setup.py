#!/usr/bin/env python

import io
import os
import re

from setuptools import setup


def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name='ophion',
    version=find_version("ophion", "__init__.py"),
    packages=["ophion"],
    description='A Graph Database client library',
    author='BMEG/OHSU',
    author_email='kellrott@gmail.com',
    url='https://github.com/bmeg/ophion',
    keywords=['client', 'graph', 'database'],  # arbitrary keywords
    python_requires='>=2.6, <3',
    install_requires=[],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 2.7',
    ],
)

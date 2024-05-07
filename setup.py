#  Copyright (c) 2020 Seven Bridges. See LICENSE

import pathlib
import os
from datetime import datetime
from setuptools import setup, find_packages

current_path = pathlib.Path(__file__).parent

name = "wrabbit"
version = open("wrabbit/version.py").read().split("=")[1].strip().strip("\"")
now = datetime.utcnow()
desc_path = pathlib.Path(current_path, "README.md")
long_description = desc_path.open("r").read()
requirements = os.path.join(current_path, 'requirements.txt')

setup(
    name=name,
    version=version,
    packages=find_packages(),
    platforms=['POSIX', 'MacOS', 'Windows'],
    python_requires='>=3.7',
    install_requires=open(requirements).read().splitlines(),
    author='Velsera',
    maintainer='Velsera',
    maintainer_email='pavle.marinkovic@velsera.com',
    author_email='pavle.marinkovic@velsera.com',
    description='Package that helps convert prepare Nextflow to be run on SevenBridges supported platforms.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3 :: Only'
    ],
    keywords='seven bridges velsera cwl common workflow language nextflow'
)

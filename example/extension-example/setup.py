#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

requirements = ["sparksampling"]

setup(
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="sparksampling-evaluation-extension-example",
    install_requires=requirements,
    include_package_data=True,
    keywords="sparksampling-evaluation-extension-example",
    name="sparksampling-evaluation-extension-example",
    packages=find_packages(include=["sparksampling.evaluation_extension.example_extension"]),
    version="0.1.0",
    zip_safe=False,
)

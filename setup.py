#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import os

requirements = []

if os.path.exists("./requirements.txt"):
    with open('requirements.txt') as f:
        setup_requirements = f.read().splitlines()
else:
    setup_requirements = []

test_requirements = []

setup(
    author="wh1isper",
    author_email='9573586@qq.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="pyspark-sampling",
    install_requires=requirements,
    include_package_data=True,
    keywords='sparksampling',
    name='sparksampling',
    entry_points={
        'console_scripts': [
            'sparksampling = sparksampling.app:main',
            'sparksamplinghost=sparksampling.watchdog:main'
        ],
    },
    packages=find_packages(include=['sparksampling', 'sparksampling.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='',
    version='0.1.0',
    zip_safe=False,
)

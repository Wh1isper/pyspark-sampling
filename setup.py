#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

from sparksampling._version import __version__

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    "sparksampling-proto",
    'grpcio>=1.35.0',
    'protobuf<4',
    'pyspark',
    'findspark',
    'traitlets',
    'pandas>=1.2',
    'requests',
    'kubernetes',
    'boto3',
    'grpcio-tools',
    'graphlib_backport',
]

test_requirements = ['pytest>=3', 'pytest-grpc', ]

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
    long_description=readme,
    include_package_data=True,
    keywords='sparksampling',
    name='sparksampling',
    entry_points={
        'console_scripts': [
            'sparksampling = sparksampling.app:main',
        ],
    },
    packages=find_packages(include=['sparksampling', 'sparksampling.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/Wh1isper/pyspark-sampling',
    version=__version__,
    zip_safe=False,
)

#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

from sparksampling_client._version import __version__

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    'grpcio>=1.35.0',
    'protobuf<4',
    'click'
]

test_requirements = ['pytest>=3']

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
    keywords='sparksampling_client',
    name='sparksampling_client',
    entry_points={
        'console_scripts': [
            'ssc = sparksampling_client.cli:cli',
        ],
    },
    packages=find_packages(include=['sparksampling_client', 'sparksampling_client.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/Wh1isper/pyspark-sampling',
    version=__version__,
    zip_safe=False,
)

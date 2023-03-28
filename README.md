![](https://img.shields.io/github/license/wh1isper/pyspark-sampling)
![](https://img.shields.io/docker/image-size/wh1isper/pysparksampling)
![](https://img.shields.io/pypi/pyversions/sparksampling)
![](https://img.shields.io/pypi/dm/sparksampling)

# pyspark-sampling

``sparksampling`` is a PySpark-based sampling and data quality assessment GRPC service that supports containerized
deployments and Spark On K8S

## Feature

- Common sampling methods: Random, Stratified, Simple
- Relationship Sampling based on DAG and Topological sorting
- Cloud Native and Spark on K8S support

# QUICK START

## Installation

The trial only requires direct installation using pypi

``pip install sparksampling``

run as

``sparksampling``

The service will start and listen on port 8530

## Docker

``docker run -p 8530:8530 wh1isper/pysparksampling:latest``


# Development

Using dev install

```shell
pip install -e .[test]
pre-commit install
```

run test

```shell
pytest -v
```

This is a Python Grpc Stub for ``sparksampling``

# sparksampling
`sparksampling` is a PySpark-based sampling and data quality assessment GRPC service that supports containerized deployments and Spark on K8S


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

# MORE

For more, see our github page: https://github.com/Wh1isper/pyspark-sampling/

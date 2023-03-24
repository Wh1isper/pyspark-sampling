# protos for datasamplnig

install grpc_tools

``` shell
pip install grpcio-tools
```

protobuf generate code for pyspark-sampling

``` shell
python -m grpc_tools.protoc -I./pb --python_out=./sparksampling_proto/ --grpc_python_out=./sparksampling_proto/ ./pb/sampling_service.proto
cd ./sparksampling_proto/ && 2to3 -n -w * && cd -
```

publish

```shell
rm -rf build dist && python -m build
twine upload dist/*

```

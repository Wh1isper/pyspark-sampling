# protos for datasamplnig

``` shell
python -m grpc_tools.protoc -I./pb --python_out=./sparksampling/proto --grpc_python_out=./sparksampling/proto ./pb/sampling_service.proto
cd ./sparksampling/proto && 2to3 -n -w * && cd ../..
```

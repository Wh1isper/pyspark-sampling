# pyspark-sampling

基于PySpark的数据采样与评估系统

”始于采样，不止采样“

## 特点

- 基于Spark的轻量化数据处理工具
- 自带采样和评估算法
- 支持用户自定义数据处理算法
- 提供HTTP服务和任务提交工具


## 如何使用

### 部署

#### 安装pyspark-sampling服务

```bash
pip install sparksampling
```

#### 配置数据库、Spark相关选项

更多配置信息见config.py

```
export SPARK_HOME="/path/to/spark_home"
export SAMPLING_SPARK_EXTRA_CONF_PATH="/path/to/config/json_file"
export SAMPLING_DB_USERNAME="database username"
export SAMPLING_DB_NAME="database name, default: sampling"
export SAMPLING_DB_HOST="database host, default: localhost"
export SAMPLING_DB_PASSWORD="database username"
export SAMPLING_SERVICE_DEBUG=0 #DEBUG默认启动 这里可以关闭
```

### 使用

提交工具见sparksampling_example.ipynb

也可以直接使用HTTP请求对任务进行提交

## 开发者指南



## 架构介绍



## RoadMap

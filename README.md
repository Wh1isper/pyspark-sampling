# pyspark-sampling

基于PySpark的数据采样与评估系统

”始于采样，不止采样“

## 特点

- 基于Spark的轻量化数据处理工具
- 自带采样和评估算法
- 支持用户自定义数据处理算法
- 提供HTTP服务和任务提交工具

## 开发平台

- Python 3.7
- Spark 3.0.1（with PySpark 3.0.0）
- MySQL  Ver 8.0.22 for Linux on x86_64 (MySQL Community Server - GPL)
- Hadoop 3.2.1
- OpenJDK Version "1.8.0_275"
  OpenJDK Runtime Environment (build 1.8.0_275-8u275-b01-0ubuntu1~18.04-b01)
  OpenJDK 64-Bit Server VM (build 25.275-b01, mixed mode)


## 如何使用

### 部署

#### 安装pyspark-sampling服务

```bash
pip install sparksampling
```

#### 配置mysql数据库

加载db文件夹下的两个sql文件evaluation_job.sql和smaple_job.sql

可以在mysql下使用source进行执行

**注意：如果出现问题请关闭MySQL大小写敏感**

#### 配置抽样系统

修改config.py或使用以下环境变量

SAMPLING_SPARK_EXTRA_CONF_PATH是json格式的配置文件，其键值参考[spark配置](https://spark.apache.org/docs/latest/configuration.html)

```
export SPARK_HOME="/path/to/spark_home"
export SAMPLING_SPARK_EXTRA_CONF_PATH="/path/to/config/json_file"
export SAMPLING_DB_USERNAME="database username"
export SAMPLING_DB_PASSWORD="database password"
export SAMPLING_DB_NAME="database name, default: sampling"
export SAMPLING_DB_HOST="database host, default: localhost"
export SAMPLING_SERVICE_DEBUG=0 #DEBUG默认启动 这里可以关闭
```

更多配置信息见config.py

### 使用

#### 启动服务

```bash
$sparksampling
```

或

```bash
$python app.py
```

#### 提交任务

API文档

演示用例见sparksampling_example.ipynb

也可以直接使用HTTP请求对任务进行提交

#### 自定义算法





## 开发者指南

### 使用本地安装

```
pip install -e ./
```

### 导入数据库

同上

### 配置单机Hadoop

如果你需要使用单机版的hadoop对hdfs或yarn进行测试，见如下教程：[Hadoop配置教程](https://wh1isper.github.io/2021/04/11/HadoopStandalone/)

### 配置单机Spark

配置单机版Spark以调试全部Spark功能，见如下教程：[Spark配置教程](https://wh1isper.github.io/2021/04/11/SparkStandalone.md/)

### 开发算法






## 架构介绍



## RoadMap

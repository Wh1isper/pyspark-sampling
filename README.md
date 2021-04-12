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

示例见customize/custom_config_example.py

配置`SAMPLING_CUSTOM_CONFIG_FILE_PATH`指向自定义代码文件

示例如下：

当前运行sparksamling目录下有文件*custom_config_example.py*

```python
# 此文件用于说明如何自定义添加代码，替换custom_config.py文件即可生效
from sparksampling.utilities.var import STATISTICS_BASIC_METHOD
from sparksampling.customize.dummy_job import DummyJob
from sparksampling.customize.dummy_dataio import DummyDataIO
compare_evaluation_code = STATISTICS_BASIC_METHOD
extra_statistics_job = {}
extra_evaluation_job = {}

extra_sampling_job = {
    "dummyJob": DummyJob
}
extra_dataio = {
    "dummyDatatype": DummyDataIO
}
```

配置：

```bash
export SAMPLING_CUSTOM_CONFIG_PATH="custom_config_example.py"
```

访问api时使用`"method": "dummyJob"`即可使用`DummyJob`；`"type": "dummyDatatype"`即可使用`DummyDataIO`

*output example:*

```bash
2021-04-12 16:04:24,845 INFO base_engine.py[28] Load data_io txt : TextDataIO
2021-04-12 16:04:24,845 INFO base_engine.py[28] Load data_io csv : CsvDataIO
2021-04-12 16:04:24,845 INFO base_engine.py[28] Load data_io dummyDatatype : DummyDataIO
2021-04-12 16:04:24,845 INFO base_engine.py[30] Load job random : SimpleRandomSamplingJob
2021-04-12 16:04:24,845 INFO base_engine.py[30] Load job stratified : StratifiedSamplingJob
2021-04-12 16:04:24,845 INFO base_engine.py[30] Load job smote : SmoteSamplingJob
2021-04-12 16:04:24,845 INFO base_engine.py[30] Load job dummyJob : DummyJob
```



## 开发者指南

### 使用本地安装

```bash
$pip install -e ./
```

### 导入数据库

同上

### 配置单机Hadoop

如果你需要使用单机版的hadoop对hdfs或yarn进行测试，见如下教程：[Hadoop配置教程](https://wh1isper.github.io/2021/04/11/HadoopStandalone/)

### 配置单机Spark

配置单机版Spark以调试全部Spark功能，见如下教程：[Spark配置教程](https://wh1isper.github.io/2021/04/11/SparkStandalone.md/)

### 开发算法

为本系统贡献算法代码或数据源适配代码，直接在core/job或core/dataio进行开发

参考以下步骤对算法进行开发：

#### 准备：确定是否有合适的dataio




## 架构介绍



## RoadMap

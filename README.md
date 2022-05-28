# 注意！ Attention！

This branch is no longer maintained, see [#3](https://github.com/Wh1isper/pyspark-sampling/issues/3)

这个分支不再维护，见  [#3](https://github.com/Wh1isper/pyspark-sampling/issues/3)

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
- MySQL Ver 8.0.22 for Linux on x86_64 (MySQL Community Server - GPL)
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
from sparksampling.var import STATISTICS_BASIC_METHOD
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

dataio开发相对简单，继承BaseDataIO即可，实现\_read和\_write方法即可

**推荐使用Spark DataFrame以及其相关API以获得最佳性能**

*使用别的数据结构请自行定义*

```python
class DummyDataIO(BaseDataIO):
    def __init__(self, spark, path):
        super(DummyDataIO, self).__init__(spark, path)
        self.write_path = "path to write"

    def _read(self, header=True, *args, **kwargs) -> DataFrame:
        pandas_df = pd.read_csv("this is not a path")
        return self.spark.createDataFrame(pandas_df)

    def _write(self, *args, **kwargs):
        # write a file or url
        # return write path or None for failed
        return self.write_path
```

job参考以下步骤对算法进行开发：

#### 1：确定任务类型

目前根据任务是否输出DataFrame到文件系统分为采样任务(sampling_job)和统计评估(statistics_job & evaluation_job)任务两类

根据任务同步执行或异步执行分为采样评估任务(sampling_job && evaluation_job)和统计任务(statistics_job)两类

见下表：

| job | 异步执行 | 输出 | 需要实现的函数 | | :------------: | :------: | :--: |
------------------------------------------------------------ | | sampling_job | 是 | 是 | def _generate(self, df:
DataFrame, *args, **kwargs) -> DataFrame: | | evaluation_job | 是 | 否 | def _statistics(self, df: DataFrame, *args, **
kwargs) -> dict: | | statistics_job | 否 | 否 | def _statistics(self, df: DataFrame, *args, **kwargs) -> dict: |

#### 2：根据任务类型在对应Engine注册

|      job      |       介绍       |      对应engine      |
| :-----------: | :--------------: | :------------------: |
|   simplejob   |   简单抽样任务   |  sampling_engine.py  |
|     mljob     | 机器学习采样任务 |  sampling_engine.py  |
| evaluationjob |     评估任务     | evaluation_engine.py |
| statisticsjob |     统计任务     | statistics_engine.py |

举例说明注册

*core/engine/sampling_engine.py*

```python
class SamplingEngine(SparkJobEngine):
    job_map = {
        # 在这里进行注册，如 "sampling job method name": JobClass
        SIMPLE_RANDOM_SAMPLING_METHOD: SimpleRandomSamplingJob,
        STRATIFIED_SAMPLING_METHOD: StratifiedSamplingJob,
        SMOTE_SAMPLING_METHOD: SmoteSamplingJob
    }

    job_map.update(extra_sampling_job)
```

#### 3：传入参数定义

以`SimpleRandomSamplingJob`举例说明

*core/job/simplejob/random_sampling.py*

```python
class SimpleRandomSamplingJob(BaseSamplingJob):
    # type_map定义了需要传入的参数和数据类型，在Engine进行配置，在Job调用check_type()进行校验
    # 如果这里的key与你想要传入的key不同，如API规定的"kseed"其实是这里的"seed"，在process_module中修改
    type_map = {
        'with_replacement': bool,
        'fraction': float,
        'seed': int
    }

    def __init__(self, *args, **kwargs):
        super(SimpleRandomSamplingJob, self).__init__(*args, **kwargs)
        self.check_type()  # 校验参数传递

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        # 实现这个方法，返回的DataFrame将被传入Dataio，写入文件系统
        return df.sample(withReplacement=self.with_replacement, fraction=self.fraction, seed=self.seed)
```

举例说明如何format参数

*handler/processmodule/ml_sampling_process_module.py*

```python
class MLSamplingProcessModule(SamplingProcessModule):
    def job_conf(self, conf):
        job_conf = self.__smote_conf(conf)
        job_conf.update(self.__customize_conf(conf))
        return job_conf

    def __smote_conf(self, conf):
        return {
            'k': conf.get('k', 3),
            'bucket_length': conf.get('bucket_length', 10),
            'multiplier': conf.get('multiplier', 2),
            'seed': conf.get('seed', random.randint(1, 65535)),
            'restore': conf.get('restore', True),
            'col_key': conf.get('key')
        }

    def __customize_conf(self, conf):
        # 在这里对参数进行format
        return {}
```

## 架构介绍

[架构介绍](https://github.com/Wh1isper/pyspark-sampling/blob/main/Architecture.md)

## RoadMap

[Roadmap](https://github.com/Wh1isper/pyspark-sampling/blob/main/roadmap.md)

**本文档将介绍如何配置单机版的Spark以及PySpark**

# Spark及相关环境下载

选择一个spark版本下载：https://spark.apache.org/downloads.html

然后下载并安装对应的scala版本

Note that, Spark 2.x is pre-built with Scala 2.11 except version 2.4.2, which is pre-built with Scala 2.12. Spark 3.0+ is pre-built with Scala 2.12.

这里使用Spark-3.0.1进行开发

# 配置spark

首先解压spark，然后设置SPARK_HOME，以下内容仅跟用户选择把spark放在哪里有关，实际上，你可以放在任何地方

```bash
tar -zxvf spark-3.0.1-bin-hadoop3.2.tgz -C /opt 
mv /opt/spark-3.0.1-bin-hadoop3.2/ /opt/spark-3.0.1 
# 在~/.bashrc添加，或存在另外一个文件里、需要的时候source 
export SPARK_HOME=/opt/spark-3.0.1 
export PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:$PATH 
```

## 配置hadoop yarn

让spark使用hadoop资源，只需要配置conf/spark-env.sh中的HADOOP_CONF_DIR为$HADOOP_HOME/etc/hadoop文件夹

如果使用本仓库提供的单机版hadoop教程，则HADOOP_CONF_DIR=/home/hadoop/hadoop/etc/hadoop/

```bash
cd /opt/spark-3.0.1 
cp conf/spark-env.sh.template conf/spark-env.sh 
echo "export HADOOP_CONF_DIR=/home/hadoop/hadoop/etc/hadoop/" >> conf/spark-env.sh
```

## 启动

local启动：

```bash
spark-shell --master local
```

yarn启动：

```bash
spark-shell --master yarn --deploy-mode client
```

# pysaprk搭建

创建虚拟环境

```bash
conda create -n {your_env_name} python=3.7 
# centos 
conda activate {your_env_name} 
# ubuntu 
source activate {your_env_name}
```

安装pyspark，注意版本对应

```bash
conda install pyspark==3.0.0
```

# Troubleshooting

## NoSuchMethod

注意pyspark版本，spark 3.0.1可以尝试pyspark 3.0.0或pyspark 3.0.1

## NoSuch...（没有这个类、对象等）

注意jdk版本，建议更新到最新java8u25x

注意scala版本，在保证spark版本和scala版本对应的前提下，使用最新2.12.1x或2.11.1x
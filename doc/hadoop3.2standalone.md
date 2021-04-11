**本文档将介绍如何配置单机版的Hadoop**

# 0 安装java-8

```bash
sudo apt update && apt upgrade 
sudo apt install openjdk-8-jdk
```

# 1 创建用户，配置免密ssh

```bash
sudo useradd -m hadoop -s /bin/bash 
sudo passwd hadoop 
sudo adduser hadoop sudo
```



```bash
su - hadoop  
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa 
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```



# 2 下载Hadoop

**以下内容使用hadoop用户完成**

```bash
wget https://downloads.apache.org/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz 
tar xvzf hadoop-*.tar.gz 
mv hadoop-3.2.1 hadoop
```



# 3 配置hadoop

修改~/.bashrc，在最后添加：

```bash
export HADOOP_HOME=/home/hadoop/hadoop export HADOOP_INSTALL=$HADOOP_HOME 
export HADOOP_MAPRED_HOME=$HADOOP_HOME export HADOOP_COMMON_HOME=$HADOOP_HOME 
export HADOOP_HDFS_HOME=$HADOOP_HOME export YARN_HOME=$HADOOP_HOME 
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native 
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin 
```

生效修改：

```bash
source ~/.bashrc 
```

设置hadoop使用的JAVA_HOME：

修改文件：`~/hadoop/etc/hadoop/hadoop-env.sh，添加：`

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 
```

# 4 配置文件

进入配置文件目录

```bash
cd $HADOOP_HOME/etc/hadoop
```

core-site.xml 配置hdfs地址

```
<configuration>
<property>
 <name>fs.default.name</name>
   <value>hdfs://localhost:9000</value>
</property>
</configuration>
```

hdfs-site.xml 配置namenode、datanode位置

```
<configuration> 
<property> 
  <name>dfs.replication</name> 
  <value>1</value> 
</property> 
<property>  
  <name>dfs.name.dir</name>    
  <value>file:///home/hadoop/hadoopdata/hdfs/namenode</value> 
</property> 
<property>  
  <name>dfs.data.dir</name>    
  <value>file:///home/hadoop/hadoopdata/hdfs/datanode</value> 
</property> 
</configuration>
```

mapred-site.xml，配置mapreduce框架为yarn

```
<configuration>
 <property>
  <name>mapreduce.framework.name</name>
   <value>yarn</value>
 </property>
</configuration>
```

yarn-site.xml 配置yarn

```
<configuration>
 <property>
  <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
 </property>
</configuration>
```

# 5 格式化namenode并启动

切换至Hadoop用户home目录，格式化namenode

```bash
cd ~ 
hdfs namenode -format
```

启动hadoop

```bash
cd $HADOOP_HOME/sbin 
./start-dfs.sh  
./start-yarn.sh 
```

# 6 访问ui

```
127.0.0.1:9870
```

# Troubleshooting

## 更改ip导致集群失效

删除hadoop用户home下的hadoopdata文件夹，重新格式化namenode并启动

这样做会丢失数据

# ` `
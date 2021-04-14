# 架构图

![SparkSamplingArchitecture](https://raw.githubusercontent.com/Wh1isper/pyspark-sampling/main/pic/SparkSamplingArchitecture.png?token=AKK5XDJGUPJ2GALAEKD2GATAOZO2S)

①    服务层与业务层交互：服务层应当将用户输入进行预处理，转化为业务层所能接收的方式，并将业务层传回的数据封装为服务器正确的输出形式

②    业务层与Spark抽样应用交互：业务层与Spark抽样应用交互，由业务层配置Spark抽样引擎，并决定何使启动引擎运行任务，同时对任务运行的状态进行监控，接收任务完成结果。

③    Spark抽样应用与计算层交互：Spark抽样应用建立于Spark大数据计算框架之上，通过调用Spark提供的接口对RDD或DataFrame进行处理，由引擎进行任务的生成和配置，并提供通用的管理接口，任务启动后及提交给Spark大数据计算框架运行。

④    计算层与数据层交互：计算层计算所得的结果不会再上送到应用层，而是根据dataio的配置，直接输出进数据层，这样可以避免单机内存瓶颈，由大数据计算框架直接输出到大数据存储中。

⑤    业务层与异步数据库引擎交互：业务层通过预先定义的orm，使用异步数据库引擎对任务状态进行实时更新，或针对用户的请求，使用异步数据库引擎对数据库进行检索，以实现查询功能。

⑥    异步数据库引擎与MySQL数据库交互：系统选用MySQL数据库作为系统数据库，主要用于记录用户请求和记录任务配置及运行情况，数据库引擎并不完全依赖MySQL，而是使用SQLAlchlemy框架进行支持，因此理论上支持其他SQL数据库。

# 核心模块调用示意

![SparkSamplingCore](https://raw.githubusercontent.com/Wh1isper/pyspark-sampling/main/pic/SparkSamplingCore.png?token=AKK5XDPY44IMIDXJ5RWUBQLAOZO74)

步骤①至步骤③是通过外部初始化对引擎进行配置，继而引擎对dataio和Job进行配置，当任务启动后，步骤④至步骤⑦表示Job从dataio中读取数据进行处理，并将产生的结果数据从dataio中输出，并将任务运行结果返回引擎，由引擎进一步处理后返回给上层的过程。

# 时序图



## 采样、评估任务时序

![SparkSamplingTimming1](https://raw.githubusercontent.com/Wh1isper/pyspark-sampling/main/pic/SparkSamplingTimming1.png?token=AKK5XDN4MXUSMDJGT3RMG73AOZPHW)

抽样评估流程时序图给出了本系统在抽样评估流程中各组件的运行时序，可以看出采用异步设计的模式下，由服务接口负责对接用户的输入以及给用户的输出，服务处理核心负责对用户请求进行引擎配置、数据库记录等操作，并完成对任务情况的监控。

## 统计任务时序

![SparkSamplingTimming2](https://raw.githubusercontent.com/Wh1isper/pyspark-sampling/main/pic/SparkSamplingTimming2.png?token=AKK5XDOON6576SKJ4FYE57DAOZPJG)

统计流程时序图中给出了本系统在查询流程中各组件的运行时序，与抽样评估流程的时序不同，数据库将不再记录任务，而是提供抽样和评估任务的任务信息供统计流程直接查询，以简化用户操作。由于采用同步设计，统计流程的处理时间不宜过长，否则可能会导致HTTP请求超时等问题。

## 查询任务时序

![SparkSamplingTimming3](https://raw.githubusercontent.com/Wh1isper/pyspark-sampling/main/pic/SparkSamplingTimming3.png?token=AKK5XDNGSGWW3C7JOTMK5YLAOZPKC)

查询流程时序图较为简单，只需要通过服务处理核心对数据库进行查询，再由服务接口进行封装，即可实现对抽样和评估任务状态的查询。
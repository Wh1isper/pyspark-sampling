 ## CI/CD:
 - 单元测试（Unit Testing）
    - **What?** 单元测试行覆盖率达95%。
   - **Why?** 单元测试可以帮助开发者在修改一些代码之后验证修改的正确性，也可以减少bug的产生。目前单元测试的行覆盖率在85%左右。
- 性能测试（Performance Testing）
  - **What?** 针对各算法完成性能测试方案及性能测试
  - **Why?** 目前系统提供的采样和评估算法并未进行性能测试，性能开销不明
 - 容器化（Container）
   - **What?** 需要Dockerfile让系统拥有容器化能力
   - **Why?** 容器化在现今云计算的地位非比寻常，提供docker支持将大大方便构建、部署和测试

 ## Features
 - 任务管理优化（Better Job Management）
   - **What?** 探索使用其他任务队列管理任务执行，或与HTTP服务分离开
   - **Why?** 目前使用Tornado的io_loop管理HTTP服务和Spark任务，Spark任务阻塞执行时会阻塞整个io_loop，导致同时运行的任务有限。通过启动多个Tornado实例可以缓解这一点，但仍不能完全解决。
 - 任务配置优化（Better Job Configuration）
   - **What?** HTTP API任务配置优化
   - **Why?** 目前任务配置采用json文件的形式，期待用户反馈并进行优化
 - gRPC服务支持（Support gRPC Service）
   - **What?** 增加gRPC服务支持
   - **Why?** 增加gRPC服务给予用户更多选择，特别是容器化部署时
- 采样算法、评估算法新增（Add More Algorithm）
  - **What?** 持续集成采样算法和评估算法
  - **Why?** 帮助用户更好地进行数据处理，做到开箱可用

 ## Maintenance
 - Issue & Bug triage
    - **What?** Triage issues and fix bugs in existing featutes 
    - **Why?** Operational maintenance of the extension
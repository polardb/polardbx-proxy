# 简介
PolarDB-X Proxy 是使用 Java 开发的高性能 PolarDB-X 标准版代理，具备自动识别集群主节点、无感高可用切换、读写分离、实例级连接池等功能。可以部署于 PolarDB-X 标准版之前，提供更加简易便捷的使用体验。

![](https://intranetproxy.alipay.com/skylark/lark/0/2025/png/208438/1750400592419-d983bffc-27b6-49bc-9d3f-8df8673d3f28.png)

# 支持功能
+ 读写分离 & 基于活跃请求数的负载均衡
+ 备库一致性读
+ 事务级连接池
+ 支持快速 HA 检测 & 幂等重试连接保持
+ 支持 Prepared Statement 性能提升 & 透传

# 限制
+ 协议：
    - 不支持压缩协议
    - 不支持开启 SSL
    - 不支持
        * COM_REFRESH（deprecated）
        * COM_PROCESS_INFO（deprecated）
        * COM_PROCESS_KILL（deprecated）
        * COM_CHANGE_USER
        * COM_REGISTER_SLAVE
    - 暂不支持，后续考虑支持
        * COM_RESET_CONNECTION
        * COM_BINLOG_DUMP
+ Capabilities：
    - 依赖 CLIENT_PROTOCOL_41，客户端必须支持
    - 默认开启 CLIENT_FOUND_ROWS，暂不支持修改
+ 认证仅支持 mysql_native_password
+ 功能受限（由于使用事务级连接池带来的影响）：
    - 不支持临时表
    - 禁止使用 lock table
    - FOUND_ROWS()、ROW_COUNT()、LAST_INSERT_ID() 和 CONNETION_ID() 可能返回错误结果
    - wait_timeout 不会生效
    - show processlist 只会返回主节点上的数据，由于事务级连接池，非活跃会话可能看不到，同时显示的ip端口信息和客户端因为代理的存在，只会显示为认证时匹配的host
    - 协议层返回的 connection id 为 proxy 分配，支持 kill query/connection，processlist 中看到的为主节点中后端连接池中会话，无法 kill

# Docker 快速开始
## 环境要求
- Linux 64 位 X86 或者 ARM 架构
- Docker（推荐最新版本）
- 内存推荐 16GB，最少 4GB
- 3307 和 8083 端口需要可用

## 启动
- backend_address: 数据库地址(格式：ip:port，leader 或者 follower 的地址)
- backend_username: 数据库用户名
- backend_password: 数据库密码(必须有密码，必须使用 mysql_native_password，填写明文密码)
- memory: Proxy使用内存(单位B，请正确配置，否则可能会导致OOM，推荐 16GB，最少 4GB)
 
> 注意：config.properties的其他配置都可以通过-e参数进行指定，可以覆盖默认值

手动下载docker镜像
```
# dockerhub
docker pull polardbx/polardbx-proxy:latest
# 国内镜像仓库
docker pull polardbx-opensource-registry.cn-beijing.cr.aliyuncs.com/polardbx/polardbx-proxy:latest
```

快速启动proxy
```shell
# polardbx-proxy自带了quick_start.sh脚本
wget https://raw.githubusercontent.com/polardb/polardbx-proxy/refs/heads/main/polardbx-proxy/quick_start.sh

# 基本脚本快速启动
bash ./quick_start.sh -e backend_address=xx.xx.xx.xx:xxxx -e backend_username=xxxx -e backend_password=xxxx -e memory=4294967296
```

## 连接
```shell
mysql -Ac -h127.1 -P3307 -uxxx -pxxxx
```

# 二进制部署包
## 打包命令
maven3.6.3或更高版本
在根目录执行以下命令（无须进入子目录）
```java
mvn clean -DskipTests package -Denv=release
```

## 部署方式
### 环境要求
JDK 11（推荐） 或更高版本（未经过验证）

```shell
[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/logs/_system]
$java --version
openjdk 11.0.11.11-AJDK 2021-12-23
OpenJDK Runtime Environment (Alibaba AJDK) (build 11.0.11.11-AJDK+145-Alibaba)
OpenJDK 64-Bit Server VM (Alibaba AJDK) (build 11.0.11.11-AJDK+145-Alibaba, mixed mode)
```

### 标准版xcluster三节点集群配置要求
- cluster_info中配置的ip地址需为proxy可连接地址
- cluster_info中配置的paxos_port（即系统表information_schema中alisql_cluster_global显示的端口）和服务端口server port(一般配置为3306)的差值要保持一致
  - 例如：三节点的server port分别为3306、3307、3308， cluster_info中配置的paxos_port则需要配置为11306、11307、11308

### 解压及部署目录
+ <font style="color:#DF2A3F;">目录名称必须包含</font>**<font style="color:#DF2A3F;">polardbx-proxy</font>**，后面可以加上其他内容便于区分

```shell
[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo]
$ll
total 21084
-rw-r--r-- 1 chenyu.zzy users 21583007 Jun 20 15:07 proxy-server-5.4.20-SNAPSHOT.tar.gz

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo]
$mkdir polardbx-proxy-0620-node-0

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo]
$tar -xzf proxy-server-5.4.20-SNAPSHOT.tar.gz -C polardbx-proxy-0620-node-0

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo]
$cd polardbx-proxy-0620-node-0/

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0]
$ll
total 16
drwxr-xr-x 2 chenyu.zzy users 4096 Jun 20 15:10 bin
drwxr-xr-x 2 chenyu.zzy users 4096 Jun 20 15:10 conf
drwxr-xr-x 2 chenyu.zzy users 4096 Jun 20 15:10 lib
drwxr-xr-x 2 chenyu.zzy users 4096 Jun 20 15:01 logs

```

+ 编辑文件 conf/config.properties，配置后端集群地址和用户名密码
    - cpus 设置为分配的 CPU 数量
        * 可以配置为 0，表示自动获取 Runtime.getRuntime().availableProcessors()
        * cpus * reactor_factor 为异步事件框架的总线程数
    - cluster_node_id 为代理集群的节点 id
        * 例如目前 PolarDB-X 标准版部署了 4 个 Proxy 节点，则这个分别设置为 0，1，2，3
    - frontend_port 为代理暴露给前端应用连接的端口
    - backend_address 为当前 PolarDB-X 标准版 Leader 节点的地址
    - backend_username 为当前 PolarDB-X 标准版设置的超级管理员账号
        * 该必须具备 Super 权限
    - backend_password 为当前 PolarDB-X 标准版设置的超级管理员密码
        * 密码不能为空
        * 未设置秘钥环境变量时候为明文
        * 设置 dnPasswordKey 环境变量后，这里设置为对应加密的密文

```properties
# global settings
worker_threads=4
cpus=0
reactor_factor=1
cluster_node_id=0
# frontend configuration
frontend_port=3307
# backend configuration
backend_address=127.0.0.1:3306
backend_username=root
backend_password=123456
```

### 启动 Proxy
+ 进入 bin 目录，执行 startup.sh 启动
    - 注意，这里默认会获取当前内存大小，默认配置堆内存
    - 如果需要指定总占用内存，带上 -m 16384，参数，配置使用内存，单位为MB
+ 启动后进入上级 logs 目录可以查看log，系统 log 位于 logs/_system/proxy.log

```shell
[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin]
$./startup.sh -m 16384
mkdir: cannot create directory ‘/home/admin’: Permission denied
./startup.sh: line 52: /home/admin/bin/server_env.sh: Permission denied
cat: /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../bin/proxy.pid: No such file or directory
k28a09207.eu95sqa: stopping proxy  ...
kill: usage: kill [-s sigspec | -n signum | -sigspec] pid | jobspec ... or kill -l [sigspec]
Oook! cost:0
metaDb env config not found: /home/chenyu.zzy/env/env.properties, then use server_env.sh instead.
cd to /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin for workaround relative path
LOG CONFIGURATION : /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../conf/logback.xml
PROXY CONFIGURATION : /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../conf/config.properties
CLASSPATH : /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../conf:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/animal-sniffer-annotations-1.24.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/annotations-24.1.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/annotations-4.1.1.4.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/checker-qual-3.43.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/error_prone_annotations-2.27.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/failureaccess-1.0.2.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-api-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-context-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-core-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-netty-shaded-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-protobuf-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-protobuf-lite-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-stub-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-util-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/gson-2.11.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/guava-33.3.1-android.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/hamcrest-core-1.3.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/j2objc-annotations-3.0.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/jcl-over-slf4j-2.0.12.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/jsr305-3.0.2.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/log4j-over-slf4j-2.0.12.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/logback-classic-1.5.3.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/logback-core-1.5.3.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/lombok-1.18.32.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/perfmark-api-0.27.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/protobuf-java-3.25.5.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proto-google-common-protos-2.48.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-common-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-core-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-net-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-parser-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-rpc-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-server-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/slf4j-api-2.0.12.jar:
cd to /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin for continue

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin]
$cd ..

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0]
$cd logs/

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/logs]
$ll
total 12
-rw-r--r-- 1 chenyu.zzy users 2920 Jun 20 15:26 gc.log
-rw-r--r-- 1 chenyu.zzy users  693 Jun 20 15:26 proxy-console.log
drwxr-xr-x 2 chenyu.zzy users 4096 Jun 20 15:26 _system

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/logs]
$cd _system/

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/logs/_system]
$ll
total 8
-rw-r--r-- 1 chenyu.zzy users 8043 Jun 20 15:26 proxy.log

```

+ 查看 proxy.log 如下表示启动成功

```shell
2025-06-20 15:26:36.855 [main] INFO  com.alibaba.polardbx.proxy.server.ProxyLauncher - ## start the proxy server
2025-06-20 15:26:36.860 [main] INFO  com.alibaba.polardbx.proxy.config.ConfigLoader - server config: {worker_threads=4, backend_username=xxx, frontend_port=3307, backend_password=xxx, backend_address=127.0.0.1:6991, cpus=4, cluster_node_id=0, reactor_factor=1}
2025-06-20 15:26:36.869 [main] INFO  com.alibaba.polardbx.proxy.net.NIOWorker - NIOWorker start with 4 processors and 16.0 MB buf per processor.
2025-06-20 15:26:36.933 [main] INFO  com.alibaba.polardbx.proxy.serverless.HaManager - HA manager admin pool initializing...
2025-06-20 15:26:37.101 [HA-Manager] INFO  com.alibaba.polardbx.proxy.serverless.HaManager - Backend cluster state changed to: [{"tag":"11.167.60.147:6991","host":"11.167.60.147","port":6991,"xport":34991,"paxos_port":14991,"role":"Leader","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:37 GMT+08:00"},{"tag":"11.167.60.147:6992","host":"11.167.60.147","port":6992,"xport":-1,"paxos_port":14992,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:37 GMT+08:00"},{"tag":"11.167.60.147:6993","host":"11.167.60.147","port":6993,"xport":-1,"paxos_port":14993,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:37 GMT+08:00"},{"tag":"127.0.0.1:6991","host":"11.167.60.147","port":6991,"xport":34991,"paxos_port":14991,"role":"Leader","peers":[{"tag":"11.167.60.147:6991","host":"11.167.60.147","port":6991,"xport":34991,"paxos_port":14991,"role":"Leader","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:37 GMT+08:00"},{"tag":"11.167.60.147:6992","host":"11.167.60.147","port":6992,"xport":-1,"paxos_port":14992,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:37 GMT+08:00"},{"tag":"11.167.60.147:6993","host":"11.167.60.147","port":6993,"xport":-1,"paxos_port":14993,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:37 GMT+08:00"}],"version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:37 GMT+08:00"}]
2025-06-20 15:26:37.113 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RW pool changed to: 127.0.0.1:6991, cost 7 ms
2025-06-20 15:26:37.119 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RO pool leader: 127.0.0.1:6991 with token sl0fHgHOWRfkgAELh0Jz added, cost 5 ms
2025-06-20 15:26:37.119 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RO pool select table update to: [{127.0.0.1:6991@1}]
2025-06-20 15:26:37.128 [HA-Manager] INFO  com.alibaba.polardbx.proxy.serverless.HaManager - Backend cluster admin pool changed to: /127.0.0.1:6991, cost 5 ms
2025-06-20 15:26:37.128 [main] INFO  com.alibaba.polardbx.proxy.serverless.HaManager - HA manager admin pool initialized.
2025-06-20 15:26:37.131 [main] INFO  c.a.polardbx.proxy.serverless.SmoothSwitchoverMonitor - SmoothSwitchoverMonitor started.
2025-06-20 15:26:37.132 [main] INFO  com.alibaba.polardbx.proxy.privilege.PrivilegeRefresher - Backend privilege initializing...
2025-06-20 15:26:37.135 [main] INFO  com.alibaba.polardbx.proxy.privilege.PrivilegeRefresher - Backend privilege initialized.
2025-06-20 15:26:37.421 [main] INFO  com.alibaba.polardbx.proxy.cluster.NodeWatchdog - Node watchdog registering...
2025-06-20 15:26:37.431 [LeaderWatchdog] INFO  com.alibaba.polardbx.proxy.cluster.NodeWatchdog - Gain leadership. owner: 11.167.60.147@42421@1750404396234 nowUTC: 1750404397421
2025-06-20 15:26:37.434 [main] INFO  com.alibaba.polardbx.proxy.cluster.NodeWatchdog - Node watchdog registered.
2025-06-20 15:26:37.434 [main] INFO  com.alibaba.polardbx.proxy.ProxyServer - ==================== Proxy started.
2025-06-20 15:26:42.135 [HA-Manager] INFO  com.alibaba.polardbx.proxy.serverless.HaManager - Backend cluster state changed to: [{"tag":"11.167.60.147:6991","host":"11.167.60.147","port":6991,"xport":34991,"paxos_port":14991,"role":"Leader","peers":[{"tag":"11.167.60.147:6991","host":"11.167.60.147","port":6991,"xport":34991,"paxos_port":14991,"role":"Leader","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"},{"tag":"11.167.60.147:6992","host":"11.167.60.147","port":6992,"xport":-1,"paxos_port":14992,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"},{"tag":"11.167.60.147:6993","host":"11.167.60.147","port":6993,"xport":-1,"paxos_port":14993,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"}],"version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"},{"tag":"11.167.60.147:6992","host":"11.167.60.147","port":6992,"xport":34992,"paxos_port":14992,"role":"Follower","peers":[{"tag":"11.167.60.147:6991","host":"11.167.60.147","port":-1,"xport":-1,"paxos_port":14991,"role":"Leader","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"}],"version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"},{"tag":"11.167.60.147:6993","host":"11.167.60.147","port":6993,"xport":-1,"paxos_port":14993,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"},{"tag":"127.0.0.1:6991","host":"11.167.60.147","port":6991,"xport":34991,"paxos_port":14991,"role":"Leader","peers":[{"tag":"11.167.60.147:6991","host":"11.167.60.147","port":6991,"xport":34991,"paxos_port":14991,"role":"Leader","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"},{"tag":"11.167.60.147:6992","host":"11.167.60.147","port":6992,"xport":-1,"paxos_port":14992,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"},{"tag":"11.167.60.147:6993","host":"11.167.60.147","port":6993,"xport":-1,"paxos_port":14993,"role":"Follower","version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"}],"version":"8.0.32-X-Cluster-8.4.20-20241014","cluster_id":6990,"update_time":"2025-06-20 15:26:42 GMT+08:00"}]
2025-06-20 15:26:42.142 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RW pool changed to: 11.167.60.147:6991, cost 5 ms
2025-06-20 15:26:42.150 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RO pool follower: 11.167.60.147:6992 with token Kf7H1PFnIngx3EkZIlSh added, cost 7 ms
2025-06-20 15:26:42.156 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RO pool leader: 11.167.60.147:6991 with token sl0fHgHOWRfkgAELh0Jz added, cost 5 ms
2025-06-20 15:26:42.156 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Kick customized RO node 11.167.60.147:6992 caused by unknown latency.
2025-06-20 15:26:42.156 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RO pool select table update to: [{11.167.60.147:6991@1}]
2025-06-20 15:26:42.156 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RO pool: 127.0.0.1:6991 removed
2025-06-20 15:26:42.162 [HA-Manager] INFO  com.alibaba.polardbx.proxy.serverless.HaManager - Backend cluster admin pool changed to: /11.167.60.147:6991, cost 5 ms
2025-06-20 15:26:47.165 [HA-Manager] INFO  c.a.polardbx.proxy.serverless.ReadWriteSplittingPool - Backend cluster RO pool select table update to: [{11.167.60.147:6991@1}, {11.167.60.147:6992@1}]
```

+ 如果有异常报错，请根据报错信息排查问题

### 重启 Proxy
+ 进入 bin 目录，执行 restart.sh

```shell
[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin]
$./restart.sh
k28a09207.eu95sqa: stopping proxy 54882 ...
Oook! cost:0
mkdir: cannot create directory ‘/home/admin’: Permission denied
/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../bin/startup.sh: line 52: /home/admin/bin/server_env.sh: Permission denied
cat: /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../bin/proxy.pid: No such file or directory
k28a09207.eu95sqa: stopping proxy  ...
kill: usage: kill [-s sigspec | -n signum | -sigspec] pid | jobspec ... or kill -l [sigspec]
Oook! cost:0
metaDb env config not found: /home/chenyu.zzy/env/env.properties, then use server_env.sh instead.
cd to /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin for workaround relative path
LOG CONFIGURATION : /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../conf/logback.xml
PROXY CONFIGURATION : /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../conf/config.properties
CLASSPATH : /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../conf:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/animal-sniffer-annotations-1.24.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/annotations-24.1.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/annotations-4.1.1.4.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/checker-qual-3.43.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/error_prone_annotations-2.27.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/failureaccess-1.0.2.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-api-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-context-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-core-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-netty-shaded-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-protobuf-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-protobuf-lite-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-stub-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/grpc-util-1.69.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/gson-2.11.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/guava-33.3.1-android.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/hamcrest-core-1.3.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/j2objc-annotations-3.0.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/jcl-over-slf4j-2.0.12.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/jsr305-3.0.2.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/log4j-over-slf4j-2.0.12.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/logback-classic-1.5.3.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/logback-core-1.5.3.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/lombok-1.18.32.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/perfmark-api-0.27.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/protobuf-java-3.25.5.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proto-google-common-protos-2.48.0.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-common-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-core-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-net-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-parser-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-rpc-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/proxy-server-5.4.20-SNAPSHOT.jar:/u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin/../lib/slf4j-api-2.0.12.jar:
cd to /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin for continue
```

### 关闭 Proxy
+ 进入 bin 目录，执行 shutdown.sh

```shell
[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/bin]
$./shutdown.sh
k28a09207.eu95sqa: stopping proxy 42421 ...
Oook! cost:0
```

### 使用 mysql 命令行客户端登录验证
```shell
[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0/logs/_system]
$mysql -Ac -h127.1 -upolardbx_root -P3307
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 32
Server version: 8.0.32-X-Proxy-1.0.0 Source distribution

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> select version();
+----------------------------------+
| version()                        |
+----------------------------------+
| 8.0.32-X-Cluster-8.4.20-20241014 |
+----------------------------------+
1 row in set (0.01 sec)
```

### 配置文件使用密文密码
+ backend_password 可以指定为密文，需要在环境变量中指定 dnPasswordKey，加解密代码如下，请自行生成秘钥并填写到 config.properties 中
+ 或者可以将生成的秘钥写入到 config.properties 中，同时 backend_password 指定为密文

```properties
# global settings
worker_threads=4
cpus=4
reactor_factor=1
cluster_node_id=0
# frontend configuration
frontend_port=3307
# backend configuration
backend_address=127.0.0.1:3306
backend_username=root

# 加密后的123456
backend_password=wflYk5saIuyUdCjuOkX6NQ==
# AES秘钥
dn_password_key=1111111111111111
```

+ 生成秘钥可使用如下命令

```shell
[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0]
$java -classpath .:./lib/proxy-core-5.4.20-SNAPSHOT.jar com.alibaba.polardbx.proxy.privilege.SecurityUtil
Usage: java SecurityUtil <plainPassword> <key>

[chenyu.zzy@k28a09207.eu95sqa /u01/chenyu.zzy/proxy_demo/polardbx-proxy-0620-node-0]
$java -classpath .:./lib/proxy-core-5.4.20-SNAPSHOT.jar com.alibaba.polardbx.proxy.privilege.SecurityUtil 123456 1111111111111111
wflYk5saIuyUdCjuOkX6NQ==
```

### 负载均衡
PolarDB-Proxy 默认开启事务级读写分离，使用 follower 和 learner 节点分摊：

+ 显式开启事务时流量路由到 leader 节点
+ 自动提交事务读请求会根据负载均衡规则路由到延迟满足要求的其他节点上
+ 默认开启备库一致性读

配置参数：

+ enable_read_write_splitting，是否开启读写分离，默认 true
+ enable_follower_read，是否允许 follower 节点承担读流量，默认 true
+ enable_leader_in_ro_pools，是否允许 leader 节点参与纯读流量负载均衡，默认 true
+ read_weights，负载均衡流量比例，默认缺省，权重全是1
+ latency_check_timeout，备节点延迟探测超时时间，默认 3000ms
+ latency_check_interval，备节点延迟探测间隔时间，默认 1000ms
+ latency_record_count，延迟分析历史点记录数量，默认 100
+ slave_read_latency_threshold，允许承担读请求的备节点最大延迟阈值，默认 3000ms
+ fetch_lsn_timeout，获取 leader 节点 LSN 超时时间，默认 1000ms
+ fetch_lsn_retry_times，获取 leader 节点 LSN 最大重试次数，默认 3
+ enable_stale_read，是否允许备库弱一致性读，默认 false
+ enable_sql_log, 是否打印 SQL 语句，默认 true

#### 典型场景
##### 关闭SQL日志
config.properties 中加入以下配置，关闭 SQL 语句打印，以提升性能
```properties
enable_sql_log=false
```

##### 备库一致性读
默认配置下，会在请求备库前，在 leader 节点上获取日志位点，在备库上应用后，再执行查询，实现具备外部一致性的读，即可以保证跨事务、跨 session 的写后读。

**注意**，当使用该模式时，如果业务流量中的写操作不频繁，建议在DN集群中设置```set global consensus_weak_read_refresh_timeout=0;```，以确保follower、learner节点能快速重放日志，避免备库一致性读的性能抖动。

##### 高性能读
config.properties 中加入以下配置，跳过日志位点重放等待，在放弃一定数据新鲜度的条件下，提升备库读的性能，达到读性能线性扩展。

```properties
enable_stale_read=true
```

##### 流量比例控制
config.properties 中加入以下配置，指定读请求在各个节点上的流量比例（Weighted Least Connection 负载均衡算法）。注意，这时候需要配置全部承担读流量的节点信息（如果需要 leader 节点也承担纯读流量负载均衡，也需要配置 leader 节点信息），否则未被配置的节点不会被路由流量。

```properties
# 到10.0.0.1:3306，10.0.0.2:3306，10.0.0.3:3306的流量比例分别为1:2:3
read_weights=10.0.0.1:3306@1,10.0.0.2:3306@2,10.0.0.3:3306@3
```

# 运维指令
+ 本地 127.0.0.1 可以通过 polardbx_root 用户名免密码登录

## show [full] backend
+ 显示后端连接，及对应连接上的字符集、Prepared Statement、变量等

```sql
mysql> show backend;
+---------------------------+---------+--------------+----------------+----------------------------------------------+----+---------------+--------------------+--------------------+--------------------+-------------------------+-------------------+
| id                        | user    | capabilities | privilege_host | connection                                   | db | state         | client_charset     | connection_charset | results_charset    | idle_prepared_statement | changed_variables |
+---------------------------+---------+--------------+----------------+----------------------------------------------+----+---------------+--------------------+--------------------+--------------------+-------------------------+-------------------+
| 11.167.60.147:6991-144377 | diamond | 0x10bb74f    | god            | /11.167.60.147:35970 <-> /11.167.60.147:6991 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | count: 0                | usr[] sys[]       |
| 11.167.60.147:6992-107022 | diamond | 0x10bb74f    | god            | /11.167.60.147:53157 <-> /11.167.60.147:6992 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | count: 0                | usr[] sys[]       |
| 11.167.60.147:6991-144378 | diamond | 0x10bb74f    | NULL           | /11.167.60.147:35972 <-> /11.167.60.147:6991 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | count: 0                | usr[] sys[]       |
| 11.167.60.147:6991-144379 | diamond | 0x10bb74f    | NULL           | /11.167.60.147:35991 <-> /11.167.60.147:6991 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | count: 0                | usr[] sys[]       |
| 11.167.60.147:6991-144380 | diamond | 0x10bb74f    | NULL           | /11.167.60.147:35992 <-> /11.167.60.147:6991 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | count: 0                | usr[] sys[]       |
+---------------------------+---------+--------------+----------------+----------------------------------------------+----+---------------+--------------------+--------------------+--------------------+-------------------------+-------------------+
5 rows in set (0.01 sec)

mysql> show full backend;
+---------------------------+---------+--------------+----------------+----------------------------------------------+----+---------------+--------------------+--------------------+--------------------+-------------------------+-------------------+
| id                        | user    | capabilities | privilege_host | connection                                   | db | state         | client_charset     | connection_charset | results_charset    | idle_prepared_statement | changed_variables |
+---------------------------+---------+--------------+----------------+----------------------------------------------+----+---------------+--------------------+--------------------+--------------------+-------------------------+-------------------+
| 11.167.60.147:6991-144377 | diamond | 0x10bb74f    | god            | /11.167.60.147:35970 <-> /11.167.60.147:6991 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | NULL                    | usr[] sys[]       |
| 11.167.60.147:6992-107022 | diamond | 0x10bb74f    | god            | /11.167.60.147:53157 <-> /11.167.60.147:6992 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | NULL                    | usr[] sys[]       |
| 11.167.60.147:6991-144378 | diamond | 0x10bb74f    | NULL           | /11.167.60.147:35972 <-> /11.167.60.147:6991 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | NULL                    | usr[] sys[]       |
| 11.167.60.147:6991-144379 | diamond | 0x10bb74f    | NULL           | /11.167.60.147:35991 <-> /11.167.60.147:6991 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | NULL                    | usr[] sys[]       |
| 11.167.60.147:6991-144380 | diamond | 0x10bb74f    | NULL           | /11.167.60.147:35992 <-> /11.167.60.147:6991 | NULL | Authenticated | utf8mb4_general_ci | utf8mb4_general_ci | utf8mb4_general_ci | NULL                    | usr[] sys[]       |
+---------------------------+---------+--------------+----------------+----------------------------------------------+----+---------------+--------------------+--------------------+--------------------+-------------------------+-------------------+
5 rows in set (0.00 sec)
```

## show cluster
+ 显示当前后端集群信息

```sql
mysql> show cluster;
+--------------------+---------------+------+-------+------------+----------+---------+--------------+-------------+----------+------------+-------------+
| address            | host          | port | xport | paxos port | role     | token   | commit index | apply index | rtt(ms)  | delay(ms)  | update time |
+--------------------+---------------+------+-------+------------+----------+---------+--------------+-------------+----------+------------+-------------+
| 11.167.60.147:6991 | 11.167.60.147 | 6991 | 34991 |      14991 | Leader   | sl0f*** |       308264 |      308264 | 0.208219 |          0 | 115 ms ago  |
| 11.167.60.147:6992 | 11.167.60.147 | 6992 | 34992 |      14992 | Follower | Kf7H*** |       308263 |      308263 | 0.196991 | 1002.14185 | 114 ms ago  |
| 11.167.60.147:6993 | 11.167.60.147 | 6993 |    -1 |      14993 | Follower | NULL    |         NULL |        NULL |     NULL |       NULL | NULL        |
| 127.0.0.1:6991     | 11.167.60.147 | 6991 | 34991 |      14991 | Leader   | NULL    |         NULL |        NULL |     NULL |          0 | NULL        |
+--------------------+---------------+------+-------+------------+----------+---------+--------------+-------------+----------+------------+-------------+
4 rows in set (0.00 sec)
```

## show [full] frontend
+ 显示前端连接信息

```sql
mysql> show frontend;
+----+---------+--------------+-----------------+----------------+----+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------+
| id | user    | capabilities | host            | privilege_host | db | state         | client_charset     | connection_charset | results_charset    | prepared_statement | transaction_status | changed_variables |
+----+---------+--------------+-----------------+----------------+----+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------+
| 48 | diamond | 0x10ba605    | 127.0.0.1:41676 | god            | NULL | Authenticated | utf8mb3_general_ci | utf8mb3_general_ci | utf8mb3_general_ci | count: 0           | NULL               | usr[] sys[]       |
+----+---------+--------------+-----------------+----------------+----+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------+
1 row in set (0.00 sec)

mysql> show full frontend;
+----+---------+--------------+-----------------+----------------+----+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------+
| id | user    | capabilities | host            | privilege_host | db | state         | client_charset     | connection_charset | results_charset    | prepared_statement | transaction_status | changed_variables |
+----+---------+--------------+-----------------+----------------+----+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------+
| 48 | diamond | 0x10ba605    | 127.0.0.1:41676 | god            | NULL | Authenticated | utf8mb3_general_ci | utf8mb3_general_ci | utf8mb3_general_ci | NULL               | NULL               | usr[] sys[]       |
+----+---------+--------------+-----------------+----------------+----+---------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------------+
1 row in set (0.00 sec)
```

## show properties
+ 显示 Proxy 配置信息

```sql
mysql> show properties;
+------------------------------------+----------------------------------------+
| key                                | value                                  |
+------------------------------------+----------------------------------------+
| backend_connect_timeout            | 3000                                   |
| node_ip                            |                                        |
| general_service_port               | 8083                                   |
| backend_pool_refresh_interval      | 49000                                  |
| enable_stale_read                  | false                                  |
| backend_ha_check_timeout           | 3000                                   |
| latency_record_count               | 100                                    |
| update_lease_timeout               | 3000                                   |
| backend_pool_refresh_threads       | 4                                      |
| backend_pool_refresh_task_interval | 1000                                   |
| backend_password                   | ******                                 |
| dynamic_config_file                | dynamic.json                           |
| read_weights                       |                                        |
| node_lease                         | 10000                                  |
| prepared_statement_cache_size      | 100                                    |
| smooth_switchover_check_interval   | 100                                    |
| backend_rw_max_pooled_size         | 600                                    |
| query_retransmit_slow_retry_delay  | 1000                                   |
| query_retransmit_fast_retries      | 10                                     |
| log_sql_param_max_length           | 4096                                   |
| worker_threads                     | 4                                      |
| privilege_refresh_interval         | 10000                                  |
| latency_check_interval             | 1000                                   |
| backend_ha_check_interval          | 5000                                   |
| backend_username                   | diamond                                |
| query_retransmit_fast_retry_delay  | 100                                    |
| enable_leader_in_ro_pools          | true                                   |
| privilege_refresh_timeout          | 10000                                  |
| enable_follower_read               | true                                   |
| backend_pool_refresh_timeout       | 3000                                   |
| backend_ha_worker_threads          | 8                                      |
| slave_read_latency_threshold       | 3000                                   |
| backend_address                    | 127.0.0.1:6991                         |
| fetch_lsn_timeout                  | 1000                                   |
| backend_pool_refresh_sql           | /* PolarDB-X-Proxy Refresh */ select 1 |
| log_sql_max_length                 | 4096                                   |
| tcp_ensure_minimum_buffer          | false                                  |
| enable_read_write_splitting        | true                                   |
| global_variables_refresh_interval  | 60000                                  |
| smooth_switchover_enabled          | true                                   |
| cpus                               | 4                                      |
| max_allowed_packet                 | 1073741824                             |
| backend_admin_max_pooled_size      | 2                                      |
| fetch_lsn_retry_times              | 3                                      |
| frontend_port                      | 3307                                   |
| timer_threads                      | 1                                      |
| smooth_switchover_wait_timeout     | 10000                                  |
| cluster_node_id                    | 0                                      |
| backend_ro_max_pooled_size         | 600                                    |
| general_service_timeout            | 3000                                   |
| query_retransmit_timeout           | 20000                                  |
| reactor_factor                     | 1                                      |
| latency_check_timeout              | 3000                                   |
+------------------------------------+----------------------------------------+
53 rows in set (0.01 sec)
```

## show reactor
+ 显示异步事件驱动框架工作统计信息

```sql
mysql> show reactor;
+-----------------+---------+--------+-----------+-------+--------+----------+----------+-------+-------+------+
| name            | sockets | events | registers | reads | writes | connects | buffer   | block | total | idle |
+-----------------+---------+--------+-----------+-------+--------+----------+----------+-------+-------+------+
| NIO-Processor-0 |       3 |  21549 |       587 | 11624 |      0 |        2 | 16777216 |  8192 |  2048 | 2044 |
| NIO-Processor-1 |       1 |   5431 |       586 |  3000 |      0 |        1 | 16777216 |  8192 |  2048 | 2047 |
| NIO-Processor-2 |       1 |   5417 |       586 |  2989 |      0 |        1 | 16777216 |  8192 |  2048 | 2047 |
| NIO-Processor-3 |       1 |  20445 |       586 | 11299 |      0 |        1 | 16777216 |  8192 |  2048 | 2047 |
+-----------------+---------+--------+-----------+-------+--------+----------+----------+-------+-------+------+
4 rows in set (0.00 sec)
```

## show ro
+ 显示只读连接池信息

```sql
mysql> show ro;
+--------------------+--------+---------+------+------------+----------+---------+----------+-----------+-------------+
| address            | weight | running | idle | max pooled | role     | token   | rtt(ms)  | delay(ms) | update time |
+--------------------+--------+---------+------+------------+----------+---------+----------+-----------+-------------+
| 11.167.60.147:6991 |      1 |       0 |    1 |        600 | Leader   | sl0f*** |   0.2315 |         0 | 297 ms ago  |
| 11.167.60.147:6992 |      1 |       0 |    1 |        600 | Follower | Kf7H*** | 0.182476 |  0.792129 | 296 ms ago  |
+--------------------+--------+---------+------+------------+----------+---------+----------+-----------+-------------+
2 rows in set (0.00 sec)
```

## show rw
+ 显示读写连接池信息

```sql
mysql> show rw;
+--------------------+--------+---------+------+------------+--------+---------+----------+-----------+-------------+
| address            | weight | running | idle | max pooled | role   | token   | rtt(ms)  | delay(ms) | update time |
+--------------------+--------+---------+------+------------+--------+---------+----------+-----------+-------------+
| 11.167.60.147:6991 |      1 |       0 |    1 |        600 | Leader | sl0f*** | 0.195226 |         0 | 373 ms ago  |
+--------------------+--------+---------+------+------------+--------+---------+----------+-----------+-------------+
1 row in set (0.00 sec)
```

# 日志
+ logs 目录会<font style="color:rgb(38, 38, 38);">记录全部</font><font style="color:rgb(38, 38, 38);background-color:rgba(0, 0, 0, 0.06);">COM_QUERY、COM_STMT_EXECUTE、COM_STMT_FETCH</font><font style="color:rgb(38, 38, 38);">的请求，也会记录相关阶段的耗时信息</font>

```sql
2025-01-1711:39:57.854-[user=rds_polardb_x,host=10.0.3.248,port=46546,schema=sysbench,lsn=132039650]SELECT c FROM sbtest8 WHERE id=546506# [state:OK,retry:0,total_time:3549.445us,retransmit_delay:0.0us,fetch_lsn:1918.52us,schedule:45.885us,wait_lsn:1572.403us] # 193706f190c0003b
```

+ <font style="color:rgb(38, 38, 38);">PS 会记录原始模板和执行参数</font>

```sql
2025-01-1714:18:50.068-[user=rds_polardb_x,host=10.0.3.248,port=59846,schema=tpcc,autocommit=0]INSERTINTO bmsql_order_line ( ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)[@1=10520,@2=10,@3=2,@4=11,@5=73730,@6=2,@7=4,@8=272.76,@9='VMatoPuHoA61eJbtNWnweQMF']# [state:OK,retry:0,total_time:156.37us] # 19372b4e55400005-62
```

+ <font style="color:rgb(38, 38, 38);">对于无感切换等待的情况，会记录 wait_leader 时间</font>

```sql
2025-02-05 17:38:13.061 - [user=rds_polardb_x,host=10.0.3.248,port=44010,schema=tpcc,autocommit=0] UPDATE bmsql_district     SET d_ytd = d_ytd + ?     WHEREd_w_id = ? AND d_id = ? [@1=4133.16,@2=15,@3=6] # [state:OK,retry:0,total_time:216331.371us,retransmit_delay:0.0us,fetch_lsn:0.0us,schedule:502.563us,wait_lsn:0.0us,wait_leader:215005.581us] # 194fcf25fb400000-1
```


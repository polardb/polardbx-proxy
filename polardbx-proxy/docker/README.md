# Docker镜像构建
## 环境
- 端口 3307 8083 需要可用
- 内存建议16G以上
- 磁盘建议100G以上
- CPU建议4核以上

## 依赖
- JDK 11
- Maven >= 3.6.3
- Docker

## 构建
```shell
bash ./docker_build.sh
```

## 运行
- backend_address: 数据库地址
- backend_username: 数据库用户名
- backend_password: 数据库密码
- memory: Proxy使用内存（单位B，请正确配置，否则可能会导致OOM，推荐 16GB，最少 4GB）
```shell
bash ./run.sh -e backend_address=xx.xx.xx.xx:xxxx -e backend_username=xxxx -e backend_password=xxxx -e memory=17179869184
```

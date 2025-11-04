#!/bin/bash

function usage() {
        echo "About"
        echo "  This is manage script. You can imitate manage server through startup.sh command."
        echo "Usage:"
        echo "  startup.sh [<options>] [args]"
        echo "Options: "
        echo "  -h               Show Help"
        echo "  -d port          Enable debug port"
        echo "  -i idc           Server idc name"
        echo "  -w wisp          Enable jvm wisp"
        echo "  -g cgroup        Enable jvm cgroup"
        echo "  -f conf          Conf file, Default conf file is $BASE/conf/config.properties"
        echo "  -s instanceId    PolarDB-X instance id"
        echo "  -l loggerRoot    Root path of logger, Default path is $BASE/log/"
        echo "  -m memorySize    Memory size"
        echo "  -M mysqlProtocol Using MySQL protocol for source data stream"
        echo "  -H sourceUrl     Url of source data stream"
        echo "  -U sourceUser    User name of source data stream"
        echo "  -P sourcePwd     Password of source data stream"
        echo "  -b enableBianque Enable bian que"
        echo "  -a key=value[;]  Args config, Example: -a k1=v1;k2=v2"
        echo "Examples:"
        echo "1. startup.sh -d 8080"
        echo "      startup with debug port 8080"
        echo "2. startup.sh -M"
        echo "      startup with MySQL protocol for streaming"
        exit 0;
}

current_path=`pwd`
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
export LANG=en_US.UTF-8
export BASE=$base

function kill_and_clean() {
  . ${base}/bin/shutdown.sh
}

function prepare() {
  mkdir -p /home/admin/bin/
  source /etc/profile
  declare -xp > /home/admin/bin/server_env.sh
}

kill_and_clean

args=$@
debugPort=
serverArgs=
instanceId=
idc=
loggerRoot=..
wisp=
cgroup=
mysqlProtocol=
sourceUrl=
sourceUser=
sourcePwd=
mem_size=

proxy_config=$base/conf/config.properties
git_properties=$base/conf/git.properties
logback_configurationFile=$base/conf/logback.xml
release_note=$base/../releaseNote
base_log=$base/logs
pidfile=$base/bin/proxy.pid
KERNEL_VERSION=`uname -r`
enable_bianque=true

checkuser=`whoami`
if [ x"$checkuser" = x"root" ];then
   echo "Can not execute under root user!";
   exit 1;
fi

TEMP=`getopt -o hl:d:i:w:g:f:s:m:MH:U:P:b:a: -- "$@"`
eval set -- "$TEMP"
while true ; do
  case "$1" in
        -h) usage; shift ;;
        -l) loggerRoot=$2; shift 2;;
        -d) debugPort=$2; shift 2;;
        -i) idc=`echo $2|sed "s/'//g"`; shift 2 ;;
        -w) wisp=`echo $2|sed "s/'//g"`; shift 2 ;;
        -g) cgroup=`echo $2|sed "s/'//g"`; shift 2 ;;
        -f) proxy_config=`echo $2|sed "s/'//g"`; shift 2 ;;
        -s) instanceId=`echo $2|sed "s/'//g"`; shift 2 ;;
        -m) mem_size=`echo $2|sed "s/'//g"`; shift 2 ;;
        -M) mysqlProtocol=true; shift 2 ;;
        -H) sourceUrl=`echo $2|sed "s/'//g"`; shift 2 ;;
        -U) sourceUser=`echo $2|sed "s/'//g"`; shift 2 ;;
        -P) sourcePwd=`echo $2|sed "s/'//g"`; shift 2 ;;
        -b) enable_bianque=`echo $2|sed "s/'//g"`; shift 2 ;;
        -a)
          config=`echo $2|sed "s/'//g"`
          if [ "$serverArgs" == "" ]; then
            serverArgs="$config"
          else
            serverArgs="$serverArgs;$config"
          fi
          shift 2;;
        --) shift;;
        *)
            shift;
            if [ $# -eq 0 ]; then
                break;
            fi
            ;;
  esac
done

source /etc/profile
# load env for polardbx by server_env.sh
if [ -f /home/admin/bin/server_env.sh ] ; then
    source /home/admin/bin/server_env.sh >> /dev/null 2>&1
fi

# try to load metadb env by env.properties
config="${HOME}/env/env.properties"
if [ -f "$config" ]
then
	echo "metaDb env config found: $config"
	while IFS='=' read -r key value
	do
		## '.' replace as '-'
		key=$(echo $key | tr '.' '_')
		## ignore the comment of properties
		[[ -z $(echo "$key" | grep -P '\s*#+.*' ) ]] \
			&& export "${key}=${value}"
	done < "$config"
else
	echo "metaDb env config not found: $config, then use server_env.sh instead."
fi

if [ x"$debugPort" != "x" ]; then
	DEBUG_SUSPEND="n"
	JAVA_DEBUG_OPT="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=$debugPort,server=y,suspend=$DEBUG_SUSPEND"
fi

if [ x"$metaDbAddr" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dmetadb_url=$metaDbAddr"
fi

if [ x"$metaDbName" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dmetadb_db_name=$metaDbName"
fi

if [ x"$metaDbProp" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dmetadb_prop=$metaDbProp"
fi

if [ x"$metaDbUser" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dmetadb_user_name=$metaDbUser"
fi

if [ x"$metaDbPasswd" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dmetadb_password=$metaDbPasswd"
fi

if [ x"$metaDbXprotoPort" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dmetadb_xproto_port=$metaDbXprotoPort"
fi

if [ x"$ins_id" != "x" ]; then
  PROXY_OPTS=" $PROXY_OPTS -Dins_id=$ins_id"
fi

if [ x"$ins_id" != "x" ]; then
  PROXY_OPTS=" $PROXY_OPTS -Dins_ip=$ins_ip"
fi

if [ x"$proxyPort" != "x" ]; then
  PROXY_OPTS=" $PROXY_OPTS -Druntime_rest_bind_port_range=$proxyPort"
fi

if [ x"$mysqlProtocol" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dsource_connection_mysql_protocol=$mysqlProtocol"
fi

if [ x"$sourceUrl" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dsource_connection_url=$sourceUrl"
elif [ x"$polarx_url" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dsource_connection_url=$polarx_url"
fi

if [ x"$sourceUser" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dsource_connection_username=$sourceUser"
elif [ x"$polarx_username" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dsource_connection_username=$polarx_username"
fi

if [ x"$sourcePwd" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dsource_connection_password=$sourcePwd"
elif [ x"$polarx_password" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dsource_connection_password=$polarx_password"
fi

if [ x"$serverArgs" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dserver_args=$serverArgs"
fi

if [ x"$instanceId" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dpolardbx_instance_id=$instanceId"
else
  PROXY_OPTS=" $PROXY_OPTS -Dpolardbx_instance_id=pxc-unknown"
fi

if [ x"$idc" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Didc=$idc"
fi

if [ x"$loggerRoot" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -DloggerRoot=$loggerRoot"
fi

if [ x"$oss_access_key_id" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Doss_access_key_id=$oss_access_key_id"
fi

if [ x"$oss_access_key_secret" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Doss_access_key_secret=$oss_access_key_secret"
fi

if [ x"$cipher_key" != "x" ]; then
	PROXY_OPTS=" $PROXY_OPTS -Dcipher_key=$cipher_key"
fi

if [ x"$enable_bianque" == "xtrue" ]; then
  BIANQUE_LIB_FILEPATH="/home/admin/bianquejavaagent/output/lib/libjava_bianque_agent.so"
  BIANQUE_SERVER_PORT=9874
  timeout 1 bash -c "nc -vw 1 127.0.0.1 $BIANQUE_SERVER_PORT 2>/tmp/bianquenc.log 1>/dev/null"
  connectedCnt=`cat /tmp/bianquenc.log | grep Connected | wc -l`
  if [ -f $BIANQUE_LIB_FILEPATH ] && [ $connectedCnt -gt 0 ] ; then
	  BIANQUE_AGENT_OPTS=" -agentpath:$BIANQUE_LIB_FILEPATH=local_path=/home/admin/bianquejavaagent/output"
  fi
fi

if [ ! -d $base_log ] ; then
	mkdir -p $base_log
fi
if [ ! -d $base_log/_system ] ; then
	mkdir -p $base_log/_system
fi

## set java path(use native java first)
TAOBAO_JAVA="/opt/taobao/java_coroutine/bin/java"
ALIBABA_JAVA="/usr/alibaba/java/bin/java"
JAVA=$(which java)
if [ ! -f $JAVA ]; then
  if [ -f $TAOBAO_JAVA ] ; then
  	JAVA=$TAOBAO_JAVA
  	JGROUP="/opt/taobao/java_coroutine/bin/jgroup"
  elif [ -f $ALIBABA_JAVA ] ; then
  	JAVA=$ALIBABA_JAVA
  else
    echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.5) in your PATH." 2>&2
    exit 1
  fi
fi
JGROUP=$(which jgroup)

JavaVersion=`$JAVA -version 2>&1 |awk 'NR==1{ gsub(/"/,""); print $3 }' | awk  -F '.' '{print $1}'`

if [ -f $pidfile ] ; then
	echo "found $pidfile , Please run shutdown.sh first ,then startup.sh" 2>&2
    exit 1
fi

str=`file -L $JAVA | grep 64-bit`
if [ -n "$str" ]; then
    freecount=`free -m | grep 'Mem' |awk '{print $2}'`

    if [ x"$mem_size" != "x" ]; then
        freecount=$mem_size
    fi

    if [ x"$memory" != "x" ]; then
        freecount=`expr $memory / 1024 / 1024`
    fi

    if [ $freecount -lt 2048 ] ; then
        JAVA_OPTS="-server -Xms1024m -Xmx1024m "
    elif [ $freecount -le 4096 ] ; then
        JAVA_OPTS="-server -Xms2g -Xmx2g "
    elif [ $freecount -le 8192 ] ; then
        JAVA_OPTS="-server -Xms4g -Xmx4g "
    elif [ $freecount -le 16384 ] ; then
        JAVA_OPTS="-server -Xms10g -Xmx10g -XX:MaxDirectMemorySize=3g"
    elif [ $freecount -le 32768 ] ; then
        JAVA_OPTS="-server -Xms24g -Xmx24g -XX:MaxDirectMemorySize=6g"
    elif [ $freecount -le 65536 ] ; then
        JAVA_OPTS="-server -Xms50g -Xmx50g -XX:MaxDirectMemorySize=12g"
    elif [ $freecount -le 131072 ] ; then
        JAVA_OPTS="-server -Xms100g -Xmx100g -XX:MaxDirectMemorySize=24g"
    elif [ $freecount -le 262144 ] ; then
        JAVA_OPTS="-server -Xms220g -Xmx220g -XX:MaxDirectMemorySize=32g"
    elif [ $freecount -gt 262144 ] ; then
        JAVA_OPTS="-server -Xms220g -Xmx220g -XX:MaxDirectMemorySize=32g"
    fi
else
	echo "not support 32-bit java startup"
	exit
fi

#2.6.32-220.23.2.al.ali1.1.alios6.x86_64 not support Wisp2
if [ "$wisp" == "wisp" ] && [ "$KERNEL_VERSION" != "2.6.32-220.23.2.al.ali1.1.alios6.x86_64" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:+UseWisp2"
fi

#disable netty-native in order to support Wisp2
   PROXY_OPTS=" $PROXY_OPTS -Dio.grpc.netty.shaded.io.netty.transport.noNative=true -Dio.netty.transport.noNative=true"

if [ "$cgroup" == "cgroup" ] ; then
    JAVA_OPTS="$JAVA_OPTS -XX:+MultiTenant -XX:+TenantCpuThrottling -XX:+TenantCpuAccounting"
    PROXY_OPTS=" $PROXY_OPTS -Dcom.alibaba.wisp.threadAsWisp.black=name:ap-processor-*"
    echo "jgroup path=$JGROUP, dockerId=$dockerId"

    if [ x"$dockerId" != "x" ]; then
        JAVA_OPTS="$JAVA_OPTS -Dcom.alibaba.tenant.jgroup.rootGroup=docker/$dockerId"
        sudo $JGROUP -u admin -g admin -r docker/$dockerId
    else
        sudo $JGROUP -u admin -g admin
    fi
fi

# in docker container, limit cpu cores
if [ x"$cpu_cores" != "x" ]; then
    JAVA_OPTS="$JAVA_OPTS -XX:ActiveProcessorCount=$cpu_cores"
fi

#https://workitem.aone.alibaba-inc.com/req/33334239
JAVA_OPTS="$JAVA_OPTS -Dtxc.vip.skip=true "

JAVA_OPTS="$JAVA_OPTS -Xss4m -XX:-UseBiasedLocking -XX:-OmitStackTraceInFastThrow "

if [ $JavaVersion -ge 11 ] ; then
  JAVA_OPTS="$JAVA_OPTS"
else
  JAVA_OPTS="$JAVA_OPTS -XX:+UseFastAccessorMethods"
fi

# For CMS and ParNew
#JAVA_OPTS="$JAVA_OPTS -XX:SurvivorRatio=10 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=75"
# For G1
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent "

if [ $JavaVersion -ge 11 ] ; then
  JAVA_OPTS="$JAVA_OPTS"
else
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintAdaptiveSizePolicy -XX:+PrintTenuringDistribution"
fi

export LD_LIBRARY_PATH=../lib/native

JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Dcom.alibaba.java.net.VTOAEnabled=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8 -Ddruid.logType=slf4j"

if [ $JavaVersion -ge 11 ] ; then
  JAVA_OPTS=" $JAVA_OPTS -Xlog:gc*:$base_log/_system/gc.log:time "
  JAVA_OPTS="$JAVA_OPTS"
else
  JAVA_OPTS=" $JAVA_OPTS -Xloggc:$base_log/_system/gc.log -XX:+PrintGCDetails "
  JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime"
fi

JAVA_OPTS=" $JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$base_log -XX:+CrashOnOutOfMemoryError -XX:ErrorFile=$base_log/hs_err_pid%p.log"

PROXY_OPTS=" $PROXY_OPTS -Dlogback.configurationFile=$logback_configurationFile -Dserver.conf=$proxy_config -Dgit.properties=$git_properties"

if [ -e "$release_note" ]
then
  PROXY_OPTS=" $PROXY_OPTS -Drelease_note=$release_note"
fi

if [ -e "$proxy_config" -a -e $logback_configurationFile ]
then
	# Caution: put polardbx-calcite first to avoid conflict
	CALCITEPATH=$(echo "$base"/lib/*.jar | awk 'BEGIN{RS="[ \n]"} /polardbx-calcite/ {printf "%s:",$0}')
	OTHERPATH=$(echo "$base"/lib/*.jar | awk 'BEGIN{RS="[ \n]"} !/polardbx-calcite/ && !/^$/ {printf "%s:",$0}')
	CLASSPATH="$CALCITEPATH$OTHERPATH$CLASSPATH"

 	CLASSPATH="$base/conf:$CLASSPATH";

 	echo "cd to $bin_abs_path for workaround relative path"
  	cd $bin_abs_path

# For ECS and 2.6.32 kernel only
if [ "${idc%%_*}" == "ecs" ] && [ "$KERNEL_VERSION" == "2.6.32-220.23.2.al.ali1.1.alios6.x86_64" ]; then
   # Calculate the number of logical CPU cores and bind them to the proxy process except for Core 0
   NUM_OF_CORES=`grep -i processor /proc/cpuinfo | wc -l`
   if [ $NUM_OF_CORES -gt 8 ]; then
      TASKSET="taskset -c 1-$((NUM_OF_CORES-1))"
   fi
fi

	echo LOG CONFIGURATION : $logback_configurationFile
	echo PROXY CONFIGURATION : "$proxy_config"
	echo CLASSPATH : $CLASSPATH
	$TASKSET $JAVA $BIANQUE_AGENT_OPTS $JAVA_OPTS $JAVA_DEBUG_OPT $PROXY_OPTS -classpath .:$CLASSPATH com.alibaba.polardbx.proxy.server.ProxyLauncher 1>>$base_log/_system/proxy-console.log 2>&1 &
	echo "$! #@# $args" > $pidfile
	echo "cd to $current_path for continue"
  	cd $current_path
else
	usage
fi

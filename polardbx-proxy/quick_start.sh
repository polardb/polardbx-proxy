#!/bin/bash

function usage() {
    echo "Usage:"
    echo "  quick_start.sh [CONFIG]"
    echo "example:"
    echo "  quick_start.sh -e backend_address=10.0.3.4:3306 \\"
    echo "                 -e backend_username=root \\"
    echo "                 -e backend_password=admin \\"
    echo "                 -e memory=4294967296"
    exit
}

function check_port() {
    local port=$1
    local TL=$(which telnet)
    if [ -f $TL ]; then
        data=`echo quit | telnet 127.0.0.1 $port| grep -ic connected`
        echo $data
        return
    fi

    local NC=$(which nc)
    if [ -f $NC ]; then
        data=`nc -z -w 1 127.0.0.1 $port | grep -ic succeeded`
        echo $data
        return
    fi
    echo "0"
    return
}

function getMyIp() {
    case "`uname`" in
        Darwin)
         myip=`echo "show State:/Network/Global/IPv4" | scutil | grep PrimaryInterface | awk '{print $3}' | xargs ifconfig | grep inet | grep -v inet6 | awk '{print $2}'`
         ;;
        *)
         myip=`ip route get 1 | awk '/src/ {for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}'`
         ;;
  esac
  echo $myip
}

CONFIG=${@:1}
PORT_LIST="3307 8083"
PORTS=""
for PORT in $PORT_LIST ; do
    #exist=`check_port $PORT`
    exist="0"
    if [ "$exist" == "0" ]; then
        PORTS="$PORTS -p $PORT:$PORT"
    else
        echo "port $PORT is used , pls check"
        exit 1
    fi
done

NET_MODE=""
case "`uname`" in
    Darwin)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
    Linux)
        bin_abs_path=$(readlink -f $(dirname $0))
        NET_MODE="--net=host"
        PORTS=""
        ;;
    *)
        bin_abs_path=`cd $(dirname $0); pwd`
        NET_MODE="--net=host"
        PORTS=""
        ;;
esac

if [ $# -eq 0 ]; then
    usage
elif [ "$1" == "-h" ] ; then
    usage
elif [ "$1" == "help" ] ; then
    usage
fi


LOCALHOST=`getMyIp`
cmd="docker run -d -it -h $LOCALHOST -e node_ip=$LOCALHOST $CONFIG --name=polardbx-proxy $NET_MODE $PORTS polardbx-opensource-registry.cn-beijing.cr.aliyuncs.com/polardbx/polardbx-proxy"
echo $cmd
eval $cmd

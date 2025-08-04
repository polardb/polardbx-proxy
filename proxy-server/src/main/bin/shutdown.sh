#!/bin/bash

cygwin=false;
linux=false;
case "`uname`" in
    CYGWIN*)
        bin_abs_path=`cd $(dirname $0); pwd`
        cygwin=true
        ;;
    Linux*)
        bin_abs_path=$(readlink -f $(dirname $0))
        linux=true
        ;;
    *)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
esac

get_pid() { 
    STR=$1
    PID=$2
    if $cygwin; then
        JAVA_CMD="$JAVA_HOME\bin\java"
        JAVA_CMD=`cygpath --path --unix $JAVA_CMD`
        JAVA_PID=`ps |grep $JAVA_CMD |awk '{print $1}'`
    else
        if $linux; then
            if [ ! -z "$PID" ]; then
                JAVA_PID=`ps -C java -f --width 2000|grep "$STR"|grep "$PID"|grep -v grep|awk '{print $2}'`
            else 
                JAVA_PID=`ps -C java -f --width 2000|grep "$STR"|grep -v grep|awk '{print $2}'`
            fi
        else
            if [ ! -z "$PID" ]; then
                JAVA_PID=`ps aux |grep "$STR"|grep "$PID"|grep -v grep|awk '{print $2}'`
            else 
                JAVA_PID=`ps aux |grep "$STR"|grep -v grep|awk '{print $2}'`
            fi
        fi
    fi
    echo $JAVA_PID;
}

base=${bin_abs_path}/..
debugPort=
other=
#eth0=`ifconfig bond0 | grep --word-regexp inet | awk '{print $2}' | cut -d":" -f2`
eth0="0.0.0.0"

TEMP=`getopt -o d:p:m:c:a:f:i:s:hD -- "$@"`
eval set -- "$TEMP"
while true ; do
  case "$1" in
        -h) usage; shift ;;
        -D) other=1; shift ;;
        -m) other=1; shift 2 ;;
        -d) debugPort=$2; shift 2;;
        -i) other=1; shift 2 ;;
        -c) other=1; shift 2 ;;
        -f) other=1; shift 2 ;;
        -s) other=1; shift 2 ;;
        -a) other=1; shift 2;;
        --) shift;;
        *)  
            shift;
            if [ $# -eq 0 ]; then
                break;
            fi
            ;; 
  esac
done

pidfile=$base/bin/proxy.pid
name="polardbx-proxy"

#get pid by ps command instead of reading from pidfile
pid=`ps -C java -f --width 2000|grep polardbx-proxy|awk '{print $2}'`
if [ x"$other" == "x" ]; then
    args=`cat $pidfile | awk -F'#@#' '{print $2}'`
fi
if [ x"$debugPort" != "x" ]; then
    args="$args -d $debugPort"
fi

if [ "$pid" == "" ] ; then
    pid=`get_pid "$name"`
fi

echo -e "`hostname`: stopping proxy $pid ... "
kill $pid

cost=0
timeout=40
while [ $timeout -gt 0 ]; do
    gpid=`get_pid "$name" "$pid"`
    if [ "$gpid" == "" ] ; then
        echo "Oook! cost:$cost"
        rm -rf $pidfile
        break;
    fi
    sleep 1
    let timeout=timeout-1
    let cost=cost+1
done

if [ $timeout -eq 0 ] ; then
    kill -9 $pid
    gpid=`get_pid "$name" "$pid"`
    if [ "$gpid" == "" ] ; then
        echo "Oook! cost:$cost"
        rm -rf $pidfile
    else
        echo "Check kill pid ${pid} failed."
        exit 1
    fi
fi

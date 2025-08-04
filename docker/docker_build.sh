#!/bin/bash

set -e

current_path=`pwd`
case "`uname`" in
    Darwin)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
    Linux)
        bin_abs_path=$(readlink -f $(dirname $0))
        ;;
    *)
        bin_abs_path=`cd $(dirname $0); pwd`
        ;;
esac
BASE=${bin_abs_path}

cd $BASE/../ && mvn -DskipTests=true clean package
docker build -f docker/Dockerfile -t polardbx/polardbx-proxy $BASE/../

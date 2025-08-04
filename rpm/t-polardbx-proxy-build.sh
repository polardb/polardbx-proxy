#!/bin/sh

dos2unix $1/script/*.sh
version=`grep -i -E "^Version:" $2.spec|sed  "s/Version\s*:\s*//" `
prefix=/home/admin
RELEASE=$4
echo "POLARDBX_SQL_VERSION: "$POLARDBX_SQL_VERSION
if [ ${#RELEASE} -gt 12 ]; then
    RELEASE=`date -d "${RELEASE:0:8} ${RELEASE:8:2}:${RELEASE:10:2}:${RELEASE:12:2}" +%s|cut -c -8`
	#if [ ${#RELEASE} -le 6 ] ; then
	#	RELEASE="2$$RELEASE"
	#fi
fi
DATE_RELEASE="`date +%Y%m%d`_$RELEASE"
export RELEASE
export DATE_RELEASE
create=$(which rpm_create)
if [ "x$create" == "x" ] || [ ! -f $create ] ; then
    create="./rpm_create"
fi
cmd="$create $2.spec -v $version -r $DATE_RELEASE -p $prefix"
echo $cmd
eval $cmd

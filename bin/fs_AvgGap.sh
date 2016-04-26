#########################################################################
# File Name: fs_user-active-count.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Wed Mar 16 20:47:49 2016
#########################################################################
#! /bin/bash

PATH_PRE="`pwd`"
PATH_NOW="`dirname $0`"
cd "${PATH_NOW}"
source ../conf/shell.conf
cd "${PATH_PRE}"

if [ 4 -ne $# ]; then
	echo "[ERROR] Usage: fs_AvgGap <vvd_fp> <out_fp> <w_tid> <w_len>"
	exit 255
fi

vvd_fp=$1
out_fp=$2
w_tid=$3
w_len=$4

hdfs dfs -rmr ${out_fp}

class=com.houjp.tianyi.regression.feature.AvgGap

spark-submit \
	--class $class \
	${LOCAL_JAR_FP} \
	--vvd_fp ${vvd_fp} \
	--out_fp ${out_fp} \
	--w_tid ${w_tid} \
	--w_len ${w_len}

if [ 0 -eq $? ]; then
	echo "[INFO] $class success."
else
	echo "[ERROR] $class meet error!"
fi
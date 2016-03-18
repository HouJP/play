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

if [ 2 -ne $# ]; then
	echo "[ERROR] Usage: out_smooth <ans_fp> <smooth_ans_fp>"
	exit 255
fi

ans_fp=$1
smooth_ans_fp=$2

hdfs dfs -rmr $smooth_ans_fp

class=com.houjp.tianyi.regression.out.DaySmooth

spark-submit \
	--class $class \
	${LOCAL_JAR_FP} \
	--ans_fp ${ans_fp} \
	--smooth_ans_fp ${smooth_ans_fp}

if [ 0 -eq $? ]; then
	echo "[INFO] $class success."
else
	echo "[ERROR] $class meet error!"
fi
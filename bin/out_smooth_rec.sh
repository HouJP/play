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

if [ 3 -ne $# ]; then
	echo "[ERROR] Usage: out_smooth <ans_pre_fp> <ans_aft_fp> <smooth_ans_rec_fp>"
	exit 255
fi

ans_pre_fp=$1
ans_aft_fp=$2
smooth_ans_rec_fp=$3

hdfs dfs -rmr $smooth_ans_rec_fp

class=com.houjp.tianyi.regression.out.DaySmoothRecover

spark-submit \
	--class $class \
	${LOCAL_JAR_FP} \
	--ans_pre_fp ${ans_pre_fp} \
	--ans_aft_fp ${ans_aft_fp} \
	--smooth_ans_rec_fp ${smooth_ans_rec_fp}

if [ 0 -eq $? ]; then
	echo "[INFO] $class success."
else
	echo "[ERROR] $class meet error!"
fi
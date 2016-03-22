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
	echo "[ERROR] Usage: cmd <t_wid> <w_len>"
	exit 255
fi

t_wid=$1
w_len=$2
vvd_fp=${HDFS_PROJECT_PT}/data/raw/video-visit-data.txt
ubd_fp=${HDFS_PROJECT_PT}/data/raw/user-behavior-data
label_fp=${HDFS_PROJECT_PT}/data/stat/label_l1_index

out_fp=${HDFS_PROJECT_PT}/data/fs/l1-15-continue-min_${t_wid}_${w_len}.txt

spark_cores_max=20
spark_executor_memory=20g


hdfs dfs -rmr $out_fp

class=com.houjp.tianyi.classification.feature.L115ContinueMin

spark-submit \
	--class $class \
	--conf spark.cores.max=${spark_cores_max} \
	--conf spark.executor.memory=${spark_executor_memory} \
	${LOCAL_JAR_FP} \
	--vvd_fp ${vvd_fp} \
	--ubd_fp ${ubd_fp} \
	--label_fp ${label_fp} \
	--out_fp ${out_fp} \
	--t_wid ${t_wid} \
	--w_len ${w_len}

if [ 0 -eq $? ]; then
	echo "[INFO] $class success."
else
	echo "[ERROR] $class meet error!"
fi
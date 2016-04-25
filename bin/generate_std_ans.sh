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

function run {

	#check path
	echo "HDFS_DATA_PT=${HDFS_DATA_PT}? yes/No"
	read ack
	if [ "yes"x != "$ack"x ]; then
		echo "HDFS_DATA_PT($HDFS_DATA_PT) wrong!"
		return 255
	fi

	local month_id=$1
	local hdfs_input_fp=$HDFS_DATA_PT/data-sets/ijcai2016_koubei_train
	#local hdfs_input_fp=$HDFS_DATA_PT/data-sets-split/ijcai2016_koubei_train_p2
	local hdfs_output_fp=$HDFS_DATA_PT/ans/std_ans

	# rm output
	hdfs dfs -rmr ${hdfs_output_fp}

	class=com.houjp.ijcai16.preprocess.STDAnsGenerator

	spark-submit \
		--class $class \
		${LOCAL_JAR_FP} \
		--input_fp ${hdfs_input_fp} \
		--output_fp ${hdfs_output_fp} \
		--month_id $month_id

	if [ 0 -ne $? ]; then
		echo "[ERROR] $class meet error!"
		return 255
	else
		echo "[INFO] $class run success."
	fi
}

if [ 1 -ne $# ]; then
	echo "[ERROR] Usage: generate_std_ans <month_id>"
	exit 255
fi

run $1

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

	local hdfs_input_fp=$1
	local hdfs_output_fp=$2
	local month_sid=$3
	local month_len=$4

	# rm output
	hdfs dfs -rmr ${hdfs_output_fp}

	class=com.houjp.ijcai16.preprocess.STDAnsGenerator

	spark-submit \
		--class $class \
		${LOCAL_JAR_FP} \
		--input_fp ${hdfs_input_fp} \
		--output_fp ${hdfs_output_fp} \
		--month_sid $month_sid \
		--month_len $month_len

	if [ 0 -ne $? ]; then
		echo "[ERROR] $class meet error!"
		return 255
	else
		echo "[INFO] $class run success."
	fi
}

if [ 4 -ne $# ]; then
	echo "[ERROR] Usage: generate_std_ans <input_fp> <output_fp> <month_sid> <month_len>"
	exit 255
fi

run $1 $2 $3 $4

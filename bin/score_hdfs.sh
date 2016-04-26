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

	local time_stamp=`date +%s`
	((time_stamp = time_stamp % 1000))

	echo "[INFO] time_stamp=$time_stamp"

	local hdfs_user_ans_fp=$1
	local hdfs_std_ans_fp=$2
	local hdfs_merchant_info_fp=$3

	class=com.houjp.ijcai16.postprocess.Score

	spark-submit \
		--class $class \
		${LOCAL_JAR_FP} \
		--user_ans_fp ${hdfs_user_ans_fp} \
		--std_ans_fp ${hdfs_std_ans_fp} \
		--merchant_info_fp ${hdfs_merchant_info_fp}

	if [ 0 -ne $? ]; then
		echo "[ERROR] $class meet error!"
		return 255
	else
		echo "[INFO] $class run success."
	fi
}

if [ 3 -ne $# ]; then
	echo "[ERROR] Usage: score <user_ans_fp> <std_ans_fp> <merchant_info_fp>"
	echo "e.g. ./score_hdfs.sh /user/houjp/play/data/ans/base_p2_11 /user/houjp/play/data/ans/std_ans /user/houjp/play/data/data-sets/ijcai2016_merchant_info"
	exit 255
fi

run $1 $2 $3

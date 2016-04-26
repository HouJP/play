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
	if [ "ack"x == "$ack"x ]; then
		echo "HDFS_DATA_PT($HDFS_DATA_PT) wrong!"
		return 255
	fi

	# clear path
	hdfs dfs -rmr ${HDFS_DATA_PT}/data-sets-split/user_p1 
	hdfs dfs -rmr ${HDFS_DATA_PT}/data-sets-split/user_p2 
	hdfs dfs -rmr ${HDFS_DATA_PT}/data-sets-split/ijcai2016_koubei_train_p1 
	hdfs dfs -rmr ${HDFS_DATA_PT}/data-sets-split/ijcai2016_koubei_train_p2 
	hdfs dfs -rmr ${HDFS_DATA_PT}/data-sets-split/ijcai2016_taobao_p1
	hdfs dfs -rmr ${HDFS_DATA_PT}/data-sets-split/ijcai2016_taobao_p2

	class=com.houjp.ijcai16.preprocess.Split

	spark-submit \
		--class $class \
		${LOCAL_JAR_FP} \
		--data_pt ${HDFS_DATA_PT}

	if [ 0 -ne $? ]; then
		echo "[ERROR] $class meet error!"
		return 255
	else
		echo "[INFO] $class run success."
	fi

	hdfs dfs -getmerge ${HDFS_DATA_PT}/data-sets-split/user_p1 ${LOCAL_DATA_PT}/data-sets-split/user_p1
	hdfs dfs -getmerge ${HDFS_DATA_PT}/data-sets-split/user_p2 ${LOCAL_DATA_PT}/data-sets-split/user_p2
	hdfs dfs -getmerge ${HDFS_DATA_PT}/data-sets-split/ijcai2016_koubei_train_p1 ${LOCAL_DATA_PT}/data-sets-split/ijcai2016_koubei_train_p1
	hdfs dfs -getmerge ${HDFS_DATA_PT}/data-sets-split/ijcai2016_koubei_train_p2 ${LOCAL_DATA_PT}/data-sets-split/ijcai2016_koubei_train_p2
	hdfs dfs -getmerge ${HDFS_DATA_PT}/data-sets-split/ijcai2016_taobao_p1 ${LOCAL_DATA_PT}/data-sets-split/ijcai2016_taobao_p1
	hdfs dfs -getmerge ${HDFS_DATA_PT}/data-sets-split/ijcai2016_taobao_p2 ${LOCAL_DATA_PT}/data-sets-split/ijcai2016_taobao_p2

}

if [ 0 -ne $# ]; then
	echo "[ERROR] Usage: splitting"
	exit 255
fi

run 

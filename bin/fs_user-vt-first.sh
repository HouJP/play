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

vvd_fp=${HDFS_PROJECT_PT}/data/raw/video-visit-data.txt
out_fp=${HDFS_PROJECT_PT}/data/fs/user-vt-first.txt
t_wid=6
w_len=5

hdfs dfs -rmr $out_fp

class=com.houjp.tianyi.classification.feature.UserVTFirst

spark-submit \
	--class $class \
	${LOCAL_JAR_FP} \
	--vvd_fp ${vvd_fp} \
	--out_fp ${out_fp} \
	--t_wid ${t_wid} \
	--w_len ${w_len}

if [ 0 -eq $? ]; then
	echo "[INFO] $class success."
else
	echo "[ERROR] $class meet error!"
fi
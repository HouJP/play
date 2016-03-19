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
	echo "[ERROR] Usage: fs_merge <gty_fp> <hjp_fp> <out_fp>"
	exit 255
fi

gty_fp=$1
hjp_fp=$2
out_fp=$3

hdfs dfs -rmr ${out_fp}

class=com.houjp.tianyi.regression.feature.FeatureMerge

spark-submit \
	--class $class \
	${LOCAL_JAR_FP} \
	--gty_fp ${gty_fp} \
	--hjp_fp ${hjp_fp} \
	--out_fp ${out_fp} 

if [ 0 -eq $? ]; then
	echo "[INFO] $class success."
else
	echo "[ERROR] $class meet error!"
fi
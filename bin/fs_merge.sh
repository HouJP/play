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

fs_pt=${HDFS_PROJECT_PT}/data/fs/
fs_name=user-active-count_user-visit-count_user-vt-first_user-vt-last

hdfs dfs -rmr ${fs_pt}/${fs_name}.txt

class=com.houjp.tianyi.classification.feature.FeatureMerge

spark-submit \
	--class $class \
	${LOCAL_JAR_FP} \
	--fs_pt ${fs_pt} \
	--fs_name ${fs_name} 
	
if [ 0 -eq $? ]; then
	echo "[INFO] $class success."
else
	echo "[ERROR] $class meet error!"
fi
#########################################################################
# File Name: run.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Thu Mar 17 22:20:22 2016
#########################################################################
#! /bin/bash

PATH_PRE="`pwd`"
PATH_NOW="`dirname $0`"
cd "${PATH_NOW}"
source ../conf/shell.conf
cd "${PATH_PRE}"

function run() {
	rm -rf run.log

	local t_wid=$1
	local w_len=$2
	local fs_name=$3

	# ./fs_user-active-count.sh ${t_wid} ${w_len}
	# if [ 0 -ne $? ]; then
	# 	echo "[ERROR] ./fs_user-active-count.sh ${t_wid} ${w_len} meet error!"
	# 	return 255
	# else
	# 	echo "[INFO] ./fs_user-active-count.sh ${t_wid} ${w_len} success." 
	# fi

	# ./fs_user-visit-count.sh ${t_wid} ${w_len}
	# if [ 0 -ne $? ]; then
	# 	echo "[ERROR] ./fs_user-visit-count.sh ${t_wid} ${w_len} meet error!"
	# 	return 255
	# else
	# 	echo "[INFO] ./fs_user-visit-count.sh ${t_wid} ${w_len} success."
	# fi

	# ./fs_user-vt-first.sh ${t_wid} ${w_len}
	# if [ 0 -ne $? ]; then
	# 	echo "[ERROR] ./fs_user-vt-first.sh ${t_wid} ${w_len} meet error!" 
	# 	return 255
	# else
	# 	echo "[INFO] ./fs_user-vt-first.sh ${t_wid} ${w_len} success." 
	# fi

	# ./fs_user-vt-last.sh ${t_wid} ${w_len}
	# if [ 0 -ne $? ]; then
	# 	echo "[ERROR] ./fs_user-vt-last.sh ${t_wid} ${w_len} meet error!" 
	# 	return 255
	# else
	# 	echo "[INFO] ./fs_user-vt-last.sh ${t_wid} ${w_len} success."
	# fi

	sh fs_l1-label-number.sh ${t_wid} ${w_len}
	if [ 0 -ne $? ]; then
		echo "[ERROR] fs_l1-label-number ${t_wid} ${w_len} meet error!" 
		return 255
	else
		echo "[INFO] fs_l1-label-number ${t_wid} ${w_len} success."
	fi

	./fs_merge.sh ${t_wid} ${w_len} ${fs_name} 
	if [ 0 -ne $? ]; then
		echo "[ERROR] ./fs_merge.sh ${t_wid} ${w_len} ${fs_name} meet error!" 
		return 255
	else
		echo "[INFO] ./fs_merge.sh ${t_wid} ${w_len} ${fs_name} success."
	fi

	./fs_mylibsvm.sh ${t_wid} ${w_len} ${fs_name} 
	if [ 0 -ne $? ]; then
		echo "[ERROR] ./fs_mylibsvm.sh ${t_wid} ${w_len} ${fs_name} meet error!" 
		return 255
	else
		echo "[INFO] ./fs_mylibsvm.sh ${t_wid} ${w_len} ${fs_name} success." 
	fi

	hdfs dfs -getmerge 	${HDFS_PROJECT_PT}/data/fs/mylibsvm_${fs_name}_${t_wid}_${w_len}.txt \
						${LOCAL_PROJECT_PT}/data/fs/mylibsvm_${fs_name}_${t_wid}_${w_len}.txt 
	if [ 0 -ne $? ]; then
		echo "[ERROR] hdfs dfs -getmerge meet error!"
		return 255
	else
		echo "[INFO] hdfs dfs -getmerge success."
	fi
}


w_len=5
fs_name=s1-fs_l1-label-number

t_wid=6

run $t_wid $w_len $fs_name
if [ 0 -ne $? ]; then
	echo "[ERROR] run $t_wid $w_len $fs_name meet error!"
	exit 255
fi

t_wid=7

run $t_wid $w_len $fs_name
if [ 0 -ne $? ]; then
	echo "[ERROR] run $t_wid $w_len $fs_name meet error!"
	exit 255
fi
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

	./fs_user-active-count.sh ${t_wid} ${w_len} &>> run.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] ./fs_user-active-count.sh ${t_wid} ${w_len} meet error!" | tee -a run.log
		return 255
	else
		echo "[INFO] ./fs_user-active-count.sh ${t_wid} ${w_len} success." | tee -a run.log
	fi

	./fs_user-visit-count.sh ${t_wid} ${w_len} &>> run.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] ./fs_user-visit-count.sh ${t_wid} ${w_len} meet error!" | tee -a run.log
		return 255
	else
		echo "[INFO] ./fs_user-visit-count.sh ${t_wid} ${w_len} success." | tee -a run.log
	fi

	./fs_user-vt-first.sh ${t_wid} ${w_len} &>> run.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] ./fs_user-vt-first.sh ${t_wid} ${w_len} meet error!" | tee -a run.log
		return 255
	else
		echo "[INFO] ./fs_user-vt-first.sh ${t_wid} ${w_len} success." | tee -a run.log
	fi

	./fs_user-vt-last.sh ${t_wid} ${w_len} &>> run.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] ./fs_user-vt-last.sh ${t_wid} ${w_len} meet error!" | tee -a run.log
		return 255
	else
		echo "[INFO] ./fs_user-vt-last.sh ${t_wid} ${w_len} success." | tee -a run.log
	fi

	./fs_merge.sh ${t_wid} ${w_len} ${fs_name} &>> run.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] ./fs_merge.sh ${t_wid} ${w_len} ${fs_name} meet error!" | tee -a run.log
		return 255
	else
		echo "[INFO] ./fs_merge.sh ${t_wid} ${w_len} ${fs_name} success." | tee -a run.log
	fi

	./fs_mylibsvm.sh ${t_wid} ${w_len} ${fs_name} &>> run.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] ./fs_mylibsvm.sh ${t_wid} ${w_len} ${fs_name} meet error!" | tee -a run.log
		return 255
	else
		echo "[INFO] ./fs_mylibsvm.sh ${t_wid} ${w_len} ${fs_name} success." | tee -a run.log
	fi

	hdfs dfs -getmerge 	${HDFS_PROJECT_PT}/data/fs/mylibsvm_${fs_name}_${t_wid}_${w_len}.txt \
						${LOCAL_PROJECT_PT}/data/fs/mylibsvm_${fs_name}_${t_wid}_${w_len}.txt  &>> run.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] hdfs dfs -getmerge meet error!" | tee -a run.log
		return 255
	else
		echo "[INFO] hdfs dfs -getmerge success." | tee -a run.log
	fi
}

t_wid=6
w_len=5
fs_name=user-active-count_user-visit-count_user-vt-first_user-vt-last_s1-fs

run $t_wid $w_len $fs_name
if [ 0 -ne $? ]; then
	echo "[ERROR] run $t_wid $w_len $fs_name meet error!"
	exit 255
fi

t_wid=7
w_len=5
fs_name=user-active-count_user-visit-count_user-vt-first_user-vt-last_s1-fs

run $t_wid $w_len $fs_name
if [ 0 -ne $? ]; then
	echo "[ERROR] run $t_wid $w_len $fs_name meet error!"
	exit 255
fi
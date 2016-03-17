#########################################################################
# File Name: run.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Thu Mar 17 22:20:22 2016
#########################################################################
#! /bin/bash


function run() {
	local t_wid=$1
	local w_len=$2
	local fs_name=$3

	./fs_user-active-count.sh ${t_wid} ${w_len}
	./fs_user-visit-count.sh ${t_wid} ${w_len}
	./fs_user-vt-first.sh ${t_wid} ${w_len}
	./fs_user-vt-last.sh ${t_wid} ${w_len}
	./fs_merge.sh ${t_wid} ${w_len} ${fs_name}
	./fs_mylibsvm.sh ${t_wid} ${w_len} ${fs_name}

	hdfs dfs -getmerge 	${HDFS_PROJECT_PT}/data/fs/mylibsvm_${fs_name}_${t_wid}_${w_len}.txt \
						${LOCAL_PROJECT_PT}/data/fs/mylibsvm_${fs_name}_${t_wid}_${w_len}.txt
}

t_wid=6
w_len=5
fs_name=user-active-count_user-visit-count_user-vt-first_user-vt-last

run $t_wid $w_len $fs_name

t_wid=7
w_len=5
fs_name=user-active-count_user-visit-count_user-vt-first_user-vt-last

run $t_wid $w_len $fs_name
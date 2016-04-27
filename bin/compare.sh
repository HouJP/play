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

function run() {
	local class=com.houjp.tianyi.regression.postprocess.Compare

	spark-submit \
		--class $class \
		${LOCAL_JAR_FP} \
		--a1_fp $1 \
		--a2_fp $2 \
		--std_fp $3 \
		--out_fp $4

	if [ 0 -eq $? ]; then
		echo "[INFO] $class success."
	else
		echo "[ERROR] $class meet error!"
		return 255
	fi
}

if [ 4 -ne $# ]; then
	echo "[ERROR] Usage: compare <a1_fp> <a2_fp> <std_fp> <out_fp>"
	exit 255
fi

run $1 $2 $3 $4
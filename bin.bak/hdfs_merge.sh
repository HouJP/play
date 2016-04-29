#########################################################################
# File Name: merge_files.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Sat Jan 30 01:35:47 2016
#########################################################################
#! /bin/bash

PATH_PRE="`pwd`"
PATH_NOW="`dirname $0`"
cd ${PATH_NOW}
#source ../conf/conf.sh
cd ${PATH_PRE}

function merge_files() {
    local fp=$1

	echo "[INFO] Merge files ${fp} ..."

    # If directory exist, delete old merged file. Or return.
    if [ -d ${fp} ]; then
        rm -rf ${fp}.bak
    else
        return 0
    fi

    ls ${fp} | while read line
    do
        cat ${fp}/${line} >> ${fp}.bak
    done

    rm -rf ${fp}
	mv ${fp}.bak ${fp}

	echo "[INFO] Merge files ${fp} done."
}

if [ 1 -ne $# ]; then
	echo "[ERROR] Usage: ./merge_files.sh <data_fp>"
	exit 1
fi

merge_files ${1}

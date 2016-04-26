#########################################################################
# File Name: run_regression.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Tue Apr 26 20:49:40 2016
#########################################################################
#! /bin/bash

PATH_PRE="`pwd`"
PATH_NOW="`dirname $0`"
cd "${PATH_NOW}"
source ../conf/shell.conf
cd "${PATH_PRE}"

./fs_AvgGap ${HDFS_PROJECT_PT}/data/raw/video-visit-data.txt ${HDFS_PROJECT_PT}/data/fs/fs_AvgGap_6_5 6 5
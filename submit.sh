#########################################################################
# File Name: submit.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Thu Feb 18 11:10:18 2016
#########################################################################
#! /bin/bash

git add --all ./
git commit -m "${1}"
git push origin master

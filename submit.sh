#########################################################################
# File Name: submit.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Mon Apr 25 11:12:18 2016
#########################################################################
#! /bin/bash

git add --all .
git commit -m "$1"
git push origin master

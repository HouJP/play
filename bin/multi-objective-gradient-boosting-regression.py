#! /usr/bin/python

from sklearn.linear_model import Ridge  
from sklearn.datasets import load_svmlight_file
import numpy as np  
import sys
import json

def train(mid):
	Xs = []
	ys = []

	for i in range(1, 11):
		fp = "%s/data/fs/v%d_fs_%s_byweek.libsvm" % (env['project_pt'], i, mid)
		X, y = load_svmlight_file(fp)
		Xs.append(X)
		ys.append(y)

	return 0

def run(train_id, test_id):
	train(train_id)

	return 0

if __name__ == "__main__":
	print "[INFO] multi-objective gradient boosting regression ..."

	if (3 != len(sys.argv)):
		print "[ERROR]: check parameters!"
		sys.exit(1)

	train_id = sys.argv[1]
	test_id = sys.argv[2]

	env = {}
	with open("../conf/py.conf", 'r') as f:
		env = json.load(f)

	run(train_id, test_id)

	print "[INFO] multi-objective gradient boosting regression."
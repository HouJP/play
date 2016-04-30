#! /usr/bin/python

from sklearn.linear_model import Ridge  
from sklearn.datasets import load_svmlight_file
import numpy as np  
import sys
import json
import math

def inner_product(v1, v2):
	ss = 0.0
	l = max(len(v1), len(v2))
	for i in range(l):
		ss += v1[i] * v2[i]
	return ss

def squared_sum(v):
	ss = 0.0
	for vi in v:
		ss += vi * vi
	return ss

def vlen(v):
	return math.sqrt(squared_sum(v))

def normalize(matrix):
	for i in range(len(matrix)):
		l = vlen(matrix[i])
		for j in range(len(matrix[i])):
			matrix[i][j] /= l

	return

def cos_similarity(v1, v2):
	l1 = vlen(v1)
	l2 = vlen(v2)
	if (l1 < 1e-6 or l2 < 1e-6):
		return 0.0
	else:
		return inner_product(v1, v2) / l1 / l2

def mean_cos_similarity(m1, m2, n):
	ans = 0.0
	for i in range(n):
		ans += cos_similarity(m1[i], m2[i])
	return ans / n

def cal_gradient(ys, vs, ls, n):
	for i in range(n):
		a = squared_sum(ys[i])
		b = squared_sum(vs[i])
		c = inner_product(ys[i], vs[i])
		# print "[INFO] a=%f, b=%f, c=%f" % (a, b, c)

		for j in range(10):
			ls[i][j] = (-1.0) * math.pow(a, -0.5) * (c * math.pow(b, -1.5) * vs[i][j] - ys[i][j] * math.pow(b, -0.5))


	return

# load libsvm files
# @return	size of dataset
def load_libsvm_files(mid, Xs, ys):
	n = 0
	for i in range(1, 11): 
		fp = "%s/data/fs/v%d_fs_%s_byweek.libsvm.small" % (env['project_pt'], i, mid) # test
		X, y = load_svmlight_file(fp)
		Xs.append(X)
		if (0 == n):
			n = len(y)
			for yi in y:
				ys.append([yi])
		else:
			for i in range(0, n):
				ys[i].append(y[i])
		print "[INFO] load %s done." % fp
	return n

def train_predict(train_id, test_id):
	# load libsvm files for training dataset
	Xs_train = []
	ys_train = []
	n_train = load_libsvm_files(train_id, Xs_train, ys_train)
	# load libsvm files for testing dataset
	Xs_test = []
	ys_test = []
	n_test = load_libsvm_files(test_id, Xs_test, ys_test)

	# normalize training dataset
	normalize(ys_train)

	unit = math.sqrt(1.0 / 10)
	# generate values before iter#1 for training dataset
	vs_train = []
	for i in range(0, n_train):
		vs_train.append([unit for j in range(10)])
	print "[INFO] mean_cos_similarity(train)=%f" % mean_cos_similarity(ys_train, vs_train, n_train)
	# generate values before iter#1 for testing dataset
	vs_test = []
	for i in range(0, n_test):
		vs_test.append([unit for j in range(10)])
	print "[INFO] mean_cos_similarity(test)=%f" % mean_cos_similarity(ys_test, vs_test, n_test)

	# generate labels before iter#1
	ls_train = []
	for i in range(0, n_train):
		ls_train.append([0.0 for j in range(10)])

	for iter in range(params['n_round']):
		print "[INFO] iter#%d ..." % iter

		cal_gradient(ys_train, vs_train, ls_train, n_train)
	
		for j in range(10):
			print "[INFO] iter#%d, model#%d ..." % (iter, j)
			l = np.array([ls_train[i][j] for i in range(n_train)])
			clf = Ridge(alpha = params['alpha'])
			clf.fit(Xs_train[j], l)
			# predict for training dataset
			pred = clf.predict(Xs_train[j])
			
			# predict for testing dataset
			pred = clf.predict(Xs_test[j])

			print "[INFO] iter#%d, model#%d done." % (iter, j)

		print "[INFO] iter#%d done." % iter

	return 0

def run(train_id, test_id):
	train_predict(train_id, test_id)

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

	params = {}
	with open("../conf/multi-objective-gradient-boosting-regression.params") as f:
		params = json.load(f)

	run(train_id, test_id)

	print "[INFO] multi-objective gradient boosting regression."
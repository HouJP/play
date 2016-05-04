#! /usr/bin/python

from sklearn.tree import DecisionTreeRegressor
from sklearn.datasets import load_svmlight_file
import numpy as np  
import sys
import json
import math
import time

def t_now():
	return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))

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
	# print "[%s] [INFO] in normalize ..." % t_now()
	for i in range(len(matrix)):
		l = vlen(matrix[i])
		for j in range(len(matrix[i])):
			matrix[i][j] /= l
	# print "[%s] [INFO] in normalize done" % t_now()

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
	#print "[%s] [INFO] in cal_gradient ..." % t_now()
	for i in range(n):
		#a = squared_sum(ys[i])
		#b = squared_sum(vs[i])
		c = inner_product(ys[i], vs[i])
		# print "[%s] [INFO] a=%f, b=%f, c=%f" % (time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())), a, b, c)

		for j in range(10):
			ls[i][j] = (-1.0) * (c * vs[i][j] - ys[i][j])
			#ls[i][j] = (-1.0) * math.pow(a, -0.5) * (c * math.pow(b, -1.5) * vs[i][j] - ys[i][j] * math.pow(b, -0.5))

	#print "[%s] [INFO] in cal_gradient done" % t_now()

	return

# load libsvm files
# @return	size of dataset
def load_libsvm_files(mid, Xs, ys):
	n = 0
	for i in range(1, 11): 
		fp = "%s/data/fs/v%d_fs_%s_byweek.%s" % (env['project_pt'], i, mid, env['file_suffix']) # test
		X, y = load_svmlight_file(fp)
		Xs.append(X)
		if (0 == n):
			n = len(y)
			for yi in y:
				ys.append([yi])
		else:
			for i in range(0, n):
				ys[i].append(y[i])
		print "[%s] [INFO] load %s done." % (time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())), fp)
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

	# models
	model = []

	# ans
	ans_train = []
	ans_test = []

	# generate predictions for training dataset
	ps_train = []
	for i in range(0, n_train):
		ps_train.append([0.0 for j in range(10)])

	# generate predictions for testing dataset
	ps_test = []
	for i in range(0, n_test):
		ps_test.append([0.0 for j in range(10)])

	# fit models
	for i in range(10):
		l = np.array([ys_train[i][j] for i in range(n_train)])
		clf = ensemble.GradientBoostingRegressor(**params)
		clf.fit(Xs_train[i], l)
		ans_train.append(clf.staged_predict(Xs_train))
		ans_test.append(clf.staged_predict(Xs_test))

	# predict for testing data set
	for i in range(params['n_estimators']):
		for j in range(10):
			for k in range(n_train):
				ps_train[k][j] = ans_train[j][i][k]
			for k in range(n_test):
				ps_test[k][j] = ans_test[j][i][k]
		print "%s,%d,%f,%f" % (t_now(), i + 1, mean_cos_similarity(ys_train, ps_train, n_train), mean_cos_similarity(ys_test, ps_test, n_test))

	return 0

def run(train_id, test_id):
	train_predict(train_id, test_id)

	return 0

if __name__ == "__main__":
	print "[%s] [INFO] gradient boosting for regression ..." % t_now()

	if (3 != len(sys.argv)):
		print "[%s] [ERROR]: check parameters!" % t_now()
		sys.exit(1)

	train_id = sys.argv[1]
	test_id = sys.argv[2]

	env = {}
	with open("../conf/py.conf", 'r') as f:
		env = json.load(f)

	params = {}
	with open("../conf/gb_reg.params") as f:
		params = json.load(f)
	print "[%s] [INFO] params: %s\n" % (t_now(), str(params))

	run(train_id, test_id)

	print "[%s] [INFO] gradient boosting for regression done\n" % t_now()
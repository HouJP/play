import sys
import numpy as np
from sklearn.datasets import load_svmlight_file
from sklearn.ensemble import GradientBoostingClassifier
import json

def run(train_fp, test_fp, params):
	X_train, y_train = load_svmlight_file(train_fp)
	X_test, y_test = load_svmlight_file(test_fp)

	print params

	model = GradientBoostingClassifier( \
		n_estimators = params['n_estimators'], \
		learning_rate = params['learning_rate'], \
		max_depth = params['max_depth'],  \
		min_samples_split = params['min_samples_split'], \
		min_samples_leaf = params['min_samples_leaf'], \
		min_weight_fraction_leaf = params['min_weight_fraction_leaf'], \
		subsample = params['subsample'], \
		max_features = params['max_features'], \
		random_state = params['random_state'] \
		).fit(X_train.toarray(), y_train)

	preds_01 = model.predict_proba(X_test.toarray())
	preds = []
	for i in preds_01:
		preds.append(i[1])
	#print preds
	
	thresh = sorted(preds)[int((1.0 - params['rate']) * len(preds))]
	print ('error=%f' % (  sum(1 for i in range(len(preds)) if int(preds[i]>thresh)!=y_test[i]) /float(len(preds))))

	pred_1_cnt = 0
	pred_1_cnt_true = 0
	pred_0_cnt = 0
	pred_0_cnt_true = 0
	for i in range(len(preds)):
		if preds[i] >= thresh:
			pred_1_cnt = pred_1_cnt + 1
			if 1 == y_test[i]:
				pred_1_cnt_true = pred_1_cnt_true + 1
		else:
			pred_0_cnt = pred_0_cnt + 1
			if 0 == y_test[i]:
				pred_0_cnt_true = pred_0_cnt_true + 1
	print('cnt(preds 1)=%d, cnt(correct 1) = %d, precision(1) = %f' % (pred_1_cnt, pred_1_cnt_true, pred_1_cnt_true / float(pred_1_cnt)))
	print('cnt(preds 0)=%d, cnt(correct 0) = %d, precision(0) = %f' % (pred_0_cnt, pred_0_cnt_true, pred_0_cnt_true / float(pred_0_cnt)))


if __name__ == "__main__":
	print("[INFO] bc_sklearn_gbdt ...")

	if (3 != len(sys.argv)):
		print "[ERROR] Usage: bc_sklearn_gbdt <train_fp> <test_fp>"
		sys.exit(1)

	train_fp = sys.argv[1]
	test_fp = sys.argv[2]

	params = {}
	with open("bc_sklearn_gbdt.params", 'r') as f:
		params = json.load(f)

	run(train_fp, test_fp, params)

	print("[INFO] bc_sklearn_gbdt done.")
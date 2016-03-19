import sys
import numpy as np
import xgboost as xgb
import json

def run(train_fp, test_fp, params):
	dtrain = xgb.DMatrix(train_fp)
	dtest = xgb.DMatrix(test_fp)

	print params

	watchlist = [(dtest, 'eval'), (dtrain, 'train')]
	model = xgb.train(params, dtrain, params['n_estimators'], watchlist)

	preds = model.predict(dtest)
	y_test = dtest.get_label()

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
	print("[INFO] bc_xgb ...")

	if (3 != len(sys.argv)):
		print "[ERROR] Usage: bc_xgb <train_fp> <test_fp>"
		sys.exit(1)

	train_fp = sys.argv[1]
	test_fp = sys.argv[2]

	params = {}
	with open("bc_xgb.params", 'r') as f:
		params = json.load(f)

	run(train_fp, test_fp, params)

	print("[INFO] bc_xgb done.")
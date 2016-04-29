import sys
import xgboost as xgb
import numpy as np

def run(mylibsvm_fp, libsvm_fp):
	mylibsvm_f = open(mylibsvm_fp, 'r')
	libsvm_f = open(libsvm_fp, 'w')

	for line in mylibsvm_f:
		line = line.strip()
		subs = line.split("\t", 1)
		libsvm_f.write(subs[1] + "\n")

	mylibsvm_f.close()
	libsvm_f.close()

if __name__ == "__main__":
	print("[INFO] mylibsvm 2 libsvm ...")

	argv_cnt = len(sys.argv)
	if (3 != argv_cnt):
		print "[ERROR] Usage: ./fs_libsvm.py <mylibsvm_fp> <libsvm_fp>"
		sys.exit(1)

	mylibsvm_fp = sys.argv[1]
	libsvm_fp = sys.argv[2]

	run(mylibsvm_fp, libsvm_fp)

	print("[INFO] mylibsvm 2 libsvm done.")
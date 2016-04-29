#! /usr/bin/python

import sys
import json

def run(mid):
	uid = []

	f = open("%s/data/fs/v1_fs_%s_byweek" % (env['project_pt'], mid))
	for s in f:
		uid.append(s.strip().split("\t")[0])
	f.close()

	for i in range(1, 11):
		f = open("%s/data/fs/v%d_fs_%s_byweek" % (env['project_pt'], i, mid))
		id = 0
		for s in f:
			if (s.strip().split("\t")[0] != uid[id]):
				print "[ERROR] %s is not %s" % (uid[id], s.strip().split("\t")[0])
				print "[ERROR] mid=%s, vid=%d, lid=%d, find error!" % (mid, i, id)
				return 255
			id += 1
		f.close()

if __name__ == "__main__":
	print "[INFO] check order of uid ..."

	if (2 != len(sys.argv)):
		print "[ERROR]: check parameters!"
		sys.exit(1)

	mid = sys.argv[1]

	env = {}
	with open("../conf/py.conf", 'r') as f:
		env = json.load(f)

	run(mid)

	print "[INFO] check order of uid done."

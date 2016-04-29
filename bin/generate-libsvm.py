#! /usr/bin/python

import sys
import json

def run(mid):
	fr = open("%s/data/fs/v1_fs_%s_byweek" % (env['project_pt'], mid))
	fw = open("%s/data/fs/uid_fs_%s_byweek" % (env['project_pt'], mid), 'w')
	for s in fr:
		fw.write(s.strip().split("\t")[0] + "\n")
	fr.close()
	fw.close()

	for i in range(1, 11):
		fr = open("%s/data/fs/v%d_fs_%s_byweek" % (env['project_pt'], i, mid))
		fw = open("%s/data/fs/v%d_fs_%s_byweek.libsvm" % (env['project_pt'], i, mid), 'w')
		for s in fr:
			subs = s.strip().split("\t")
			fw.write(subs[1] + " " + subs[2] + "\n")
		fr.close()
		fw.close()

if __name__ == "__main__":
	print "[INFO] generate libsvm ..."

	if (2 != len(sys.argv)):
		print "[ERROR]: check parameters!"
		sys.exit(1)

	mid = sys.argv[1]

	env = {}
	with open("../conf/py.conf", 'r') as f:
		env = json.load(f)

	run(mid)

	print "[INFO] generate libsvm."
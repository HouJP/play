import sys

def run(in_fp_1, in_fp_2, out_fp, size):
	in_1 = open(in_fp_1, 'r')
	in_2 = open(in_fp_2, 'r')

	uids = set()
	uid1 = []
	lb1 = []
	uid2 = []
	lb2 = []
	ans = []
	value = []
	max_n = 250000

	cor = 0

	for line in in_1:
		line = line.strip()
		subs = line.split("\t")
		uid1.append(subs[1])
		lb1.append(int(subs[2]))
	for line in in_2:
		line = line.strip()
		subs = line.split("\t")
		uid2.append(subs[1])
		lb2.append(int(subs[2]))

	for i in range(max_n):
		if (size <= len(ans)):
			print "[INFO] finish merge sort, at line " + str(i)
			print "[INFO] correct = %d" % cor
			print "[INFO] precision = %f" % (1.0 * cor / size)
			break
		if (uid1[i] in uids):
			ans.append(uid1[i])
			if (1 == lb1[i]):
				cor += 1
		else:
			uids.add(uid1[i])
		if (uid2[i] in uids):
			ans.append(uid2[i])
			if (1 == lb2[i]):
				cor += 1
		else:
			uids.add(uid2[i])

	in_1.close()
	in_2.close()

	out = open(out_fp, 'w')
	for line in ans:
		#print line
		out.write(line + "\t1.0\n")

	out.close()



if __name__ == "__main__":
	print "[INFO] bc merge sort ..."

	if (5 != len(sys.argv)):
		print "[ERROR] Usage: bc_merge_sort <in_fp_1> <in_fp_2> <out_fp> <size>"
		sys.exit(1)

	in_fp_1 = sys.argv[1]
	in_fp_2 = sys.argv[2]
	out_fp = sys.argv[3]
	size = int(sys.argv[4])

	run(in_fp_1, in_fp_2, out_fp, size)

	print "[INFO] bc merge sort done."
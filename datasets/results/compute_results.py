expected = list()
results = list()
positives = 0
tp = 0
fp = 0


f1 = open("processed.txt")
f2 = open("../febrl1_dups.txt")


line = f1.readline()
while line:
	if line == "\n":
		line = f1.readline()
		continue
	splt = line.split(" ")
	temp = list()
	for i in splt:
		if "\n" in i:
			temp.append(int(i.split("\n")[0]))
		else:
			temp.append(int(i))
	results.append(temp)
	line = f1.readline()


line = f2.readline()
while line:
	if line == "\n":
		line = f2.readline()
		continue
	splt = line.split(" ")
	temp = list()
	for i in splt:
		if "\n" in i:
			temp.append(int(i.split("\n")[0]))
		else:
			temp.append(int(i))
	expected.append(temp)
	line = f2.readline()


for i in expected:
	temp = len(i) * (len(i) - 1) / 2
	positives += temp


for lst in results:
	for i in range(0, len(lst)):
		for lst2 in expected:
			expected_list = list()
			if lst[i] in lst2:
				expected_list = lst2
				break
		for j in range(i+ 1 , len(lst)):
			if lst[j] in expected_list:
				tp += 1
			else:
				fp += 1



precision = tp / (tp + fp)

recall = tp / positives
f1 = 2 * round(precision, 2) * round(recall, 2) / (round(precision, 2) + round(recall, 2))

print("Positives: " + str(positives) + "\n\n")
print("True positives: " + str(tp) + "\n")
print("False negatives: " + str(positives - tp) + "\n")
print("False positives: " + str(fp) + "\n")
print("Precision: " + str(precision) + "\n")
print("Recall: " + str(recall) + "\n")
print("F1: " + str(f1) + "\n")



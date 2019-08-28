f = open("test.txt")

result = dict()
line = f.readline()
while line:
	splt = line.split(" - ")
	if int(splt[0]) in result:
		result[int(splt[0])].append(int(splt[1]))
	else:
		result[int(splt[0])] = list()
	line = f.readline()


f2 = open("processed.txt", "w")

lines = str()
for i in result:
	if len(result[i]) == 0:
		continue;
	line = str(i)
	for j in result[i]:
		line += " " + str(j)
	lines += (line + "\n")

f2.write(lines)
f2.close()


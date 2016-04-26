file = open('cov_small.data')
#print file.read()
data = file.read()
lines = data.split("\n")
for i in range(len(lines)):
	lines[i] = str(i) + "," + lines[i]
out = open('converted.csv', 'w')
for line in lines:
	if len(line) < 55:
		print line
	out.write(line + '\n')
out.close()



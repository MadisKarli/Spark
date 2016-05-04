file = open('covdata.csv')
#print file.read()
data = file.read()
lines = data.split("\n")
for i in range(len(lines)):
	lines[i] = str(i) + "," + lines[i]
out = open('convertedcovdata.csv', 'w')
for line in lines:
	if len(line) < 55:
		print line
	else:
		out.write(line + '\n')
out.close()



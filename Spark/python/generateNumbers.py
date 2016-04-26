from random import uniform
with open("correlationData.csv", "w") as file:
	text = ""
	for j in range(50):
		text = ""
		for i in range(1000000):
			first = uniform(0.0, 999.9)
			second = uniform(100.0, 1999.9)
			text += str(first) + ", " + str(second) + "\n"
		file.write(text)
	first = uniform(0.0, 999.9)
	second = uniform(100.0, 1999.9)
	file.write(str(first) + ", " + str(second))

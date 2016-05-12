from random import randint, choice
import string

#This script creates test data for Bayes Naive classifier.
#Limits are taken from initial dataset but for the sake of performance only important columns are calculated and others are filled in with dummy data
#sometimes classes are assigned by random - this is done to add some confusion

classes = [1,2,3,4,5,6,7]
with open("forestdata50mil.csv", "w") as file:
	for i in range(50000000):
		text = ""
		importantnr = randint(1859,3855)
		#a = randint(0,1397)
		#astr = ","+str(a)
		b = randint(0,1)
		bstr = "," + str(b)

		if 1859 <= importantnr <= 2144:
			c = 1
		elif 2145 <= importantnr <= 2430:
			c = 2
		elif 2431 <= importantnr <= 2715:
			c = 3
		elif 2716 <= importantnr <= 3000:
			c = 4
		elif 3001 <= importantnr <= 3285:
			c = 5
		elif 3286 <= importantnr <= 3570:
			c = 6
		elif 3571 <= importantnr <= 3855:
			c = 7
		else:
			c = 100
		wildcard = randint(1,5)
		if wildcard == 3:
			c = choice(classes)

		text += str(i) +","+ str(importantnr) + ","+str(randint(0,360))+","+str(randint(0,66))+","+str(randint(0,1397))+","+str(randint(-173,601))+","+str(randint(0,7117))+","+str(randint(0,254))+","+str(randint(0,254))+","+str(randint(0,254))+","+str(randint(0,7173))+ + 43 * bstr +",1" + "," +str(c)+"\n"
		file.write(text)
#285
#1859-2144
#2145-2430
#2431-2715
#2716-3000
#3001-3285
#3286-3570
#3571-3855
#133350
#b 0-360
#c 0-66
#d 0-1397
#e -173-601
#f 0-7117
#g 0-254
#h 0-254
#i 0-254
#j 0-7173

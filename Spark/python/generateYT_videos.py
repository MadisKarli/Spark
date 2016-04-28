from random import uniform
import random
import string
codecs = ['h264', 'flv1', 'mpeg4', 'vp8', 'none']
categories = ['Music', 'People & Blogs', 'Nonprofits & Activis', 'Sports', 'News & Politics', 'Gaming', 'Comedy', 'Film & Animation', 'Entertainment', 'Howto & Style', 'Autos & Vehicles', 'Education', 'Pets & Animals', 'Travel & Events', 'Science & Technology', 'Shows']

with open("generated YT 50 million.tsv", "w") as file:
	text = ""
	for j in range(50000000):
		text = ""
		id_ = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(4)) + '-' + ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(5))
		duration = int(uniform(1,25845))
		bitrate1 = int(uniform(0,22421))
		bitrate2 = int(uniform(0,22229))
		height = int(uniform(100,2592))
		width = int(uniform(88,1944))
		framerate = int(uniform(0,59.94))
		estimatedframerate = int(uniform(0,30.02))
		codec = random.choice(codecs)
		category = random.choice(categories)
		#link = 'http://r4---sn-ovgq0oxu-5goe.c.youtube.com/videoplayback?id=' + ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(753))
		link = 'http://r4---sn-ovgq0oxu-5goe.c.youtube.com/videoplayback?key=yt1&ip=86.50.94.176&ms=au&ratebypass=yes&ipbits=8&source=youtube&mv=m&sparams=cp%2Cid%2Cip%2Cipbits%2Citag%2Cratebypass%2Csource%2Cupn%2Cexpire&mt=1377888632&upn=a-TUDF8iLHA&cp=U0hWTFlMUF9JS0NONl9RRlREOjgwZUlWdElqQTlw&id=5ac723fa2e634e37&itag=43&fexp=916606%2C932200%2C930102%2C904499%2C916611%2C929117%2C929121%2C929906%2C929907%2C929922%2C929127%2C929129%2C929131%2C929930%2C936403%2C925726%2C925720%2C925722%2C925718%2C929917%2C929933%2C920302%2C913428%2C919811%2C913563%2C904830%2C919373%2C930803%2C908536%2C904122%2C932211%2C938701%2C936308%2C909549%2C900816%2C912711%2C904494%2C904497%2C939903%2C900375%2C906001&sver=3&expire=1377912341&signature=388698978786D63FD923420DA9E9A9830AAD8D87.C4A193EDD06B970BEE4EF736CD8F366255EE183B'
		text = id_ + "\t" + str(duration) + "\t" + str(bitrate1) + "\t" + str(bitrate2) + "\t" + str(height) + "\t" + str(width) + "\t" + str(framerate) + "\t" + str(estimatedframerate) + "\t" + codec + "\t" + category + "\t" + link + "\t" + "\n"

		file.write(text)



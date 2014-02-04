import sys


a = 5
b = 10
c = 20
d = 5

def pointDist(x1,y1,x2,y2):
	return ((x1-x2)**2 + (y1-y2)**2)**.5

def genYellow(yulx, yuly, ylrx, ylry):
	result = []
	for i in range((ylrx - yulx)*100 + 1):
		x1 = yulx + i*.01
		for j in range((yuly - ylry)*100 + 1):
			y1 = ylry + j*.01
			result.append((x1,y1))
	return result

def runit(yulx, yuly, ylrx, ylry, bulx, buly, blrx, blry):
	yellow = genYellow(yulx, yuly, ylrx, ylry)
	blue = genYellow(bulx, buly, blrx, blry)
	for pt in yellow:
		dy = pointDist(pt[0], pt[1], a, b)
		db = pointDist(pt[0], pt[1], c, d)
		if dy > db:
			print 'nope, yellow point ({},{}) has dist {} from a,b and dist {} from c,d'.format(pt[0], pt[1], dy, db)
			break
	for pt in blue:
		dy = pointDist(pt[0], pt[1], a, b)
		db = pointDist(pt[0], pt[1], c, d)
		if dy < db:
			print 'nope, blue point ({},{}) has dist {} from a,b and dist {} from c,d'.format(pt[0], pt[1], dy, db)
			break
			

if __name__ == "__main__":
	args = [int(i) for i in sys.argv[1:]]
	runit(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7])
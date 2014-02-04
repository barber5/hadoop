import sys

def doit(mult):
	rs = [1, 2, 3, 4, 6, 8, 12, 24]
	bs = [24/i for i in rs]
	for i in range(len(rs)):
		r = rs[i]
		b = bs[i]		
		dmg = mult*(1-(1-.2**r)**b) + (1-.5**r)**b
		print 'r: {}, b: {}, dmg: {}'.format(rs[i], bs[i], dmg)

if __name__ == "__main__":
	doit(int(sys.argv[1]))
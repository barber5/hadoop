import sys


def compIt(I, P):
	print 'I={} P={}'.format(I, P)
	bst = 5*4*P
	tm = 4*(float(I*(I-1))/2)
	print 'bst is {}'.format(bst)
	print 'tm is {}'.format(tm)
	print 'bst beats tm: {}\n\n'.format(bst < tm)

def compAll():	
	compIt(1000, 120000)
	compIt(100000, 900000000)
	compIt(200000, 5000000000)
	compIt(1000000, 120000000000)
	
if __name__ == "__main__":
	#compIt(int(sys.argv[1]), int(sys.argv[2]))
	compAll()
import sys, operator

def apriori(filename, support):	
	with open(filename, 'r') as f:
		items = {}
		i = 0
		while True:
			i+=1			
			line = f.readline()			
			if line == '':
				break
			lineArr = line.split(' ')[:-1]
			for item in lineArr:
				if item not in items:
					items[item] = 0
				items[item] += 1
		singles = {}
		for k,v in items.iteritems():
			if v > support - 1:
				singles[k] = v
		f.seek(0,0)
		itempairs = {}
		while True:
			line = f.readline()
			if line == '':
				break
			lineArr = line.split(' ')[:-1]
			for i, item in enumerate(lineArr):
				if item in singles:
					for j in range(i+1, len(lineArr)):
						it2 = lineArr[j]
						if it2 in singles:
							dub = [item, it2]							
							dub.sort()							
							double = tuple(dub)
							if double not in itempairs:
								itempairs[double] = 0
							itempairs[double] += 1
		doubles = {}
		for k,v in itempairs.iteritems():
			if v > support - 1:
				doubles[k] = v


		f.seek(0,0)
		itemtriplets = {}
		while True:
			line = f.readline()
			if line == '':
				break
			lineArr = line.split(' ')[:-1]
			for i, item in enumerate(lineArr):
				if item in singles:
					for j in range(i+1, len(lineArr)):
						it2 = lineArr[j]
						if it2 in singles:
							for k in range(j+1, len(lineArr)):
								it3 = lineArr[k]
								if it3 in singles:
									p1l = [item, it2]
									p1l.sort()
									p1 = tuple(p1l)
									p2l = [item, it3]
									p2l.sort()
									p2 = tuple(p2l)
									p3l = [it2, it3]
									p3l.sort()
									p3 = tuple(p3l)
									if p1 in doubles and p2 in doubles and p3 in doubles:
										trip = [item, it2, it3]
										trip.sort()
										tri = tuple(trip)
										if tri not in itemtriplets:
											itemtriplets[tri] = 0
										itemtriplets[tri] += 1

		triples = {}
		for k,v in itemtriplets.iteritems():
			if v > support - 1:
				triples[k] = v		
		f.close()		
		return singles, doubles, triples

def findDoubles(singles, doubles):
	rules = {}
	for k,v in doubles.iteritems():
		k1 = k[0]
		k2 = k[1]
		supp1 = singles[k1]
		supp2 = singles[k2]
		rules[(k1, k2)] = float(v) / supp1
		rules[(k2, k1)] = float(v) / supp2
	sorted_rules = sorted(rules.iteritems(), key=operator.itemgetter(1))
	sorted_rules.reverse()
	return sorted_rules

def findTriples(doubles, triples):
	rules = {}
	for k,v in triples.iteritems():
		k1 = k[0]
		k2 = k[1]
		k3 = k[2]
		l12 = [k1,k2]
		l12.sort()
		supp12 = doubles[tuple(l12)]
		l23 = [k2, k3]
		l23.sort()
		supp23 = doubles[tuple(l23)]
		l13 = [k1, k3]
		l13.sort()
		supp13 = doubles[tuple(l13)]
		rules[(k1,k2,k3)] = float(v) / supp12
		rules[(k1,k3,k2)] = float(v) / supp13
		rules[(k2,k3,k1)] = float(v) / supp23
	sorted_rules = sorted(rules.iteritems(), key=operator.itemgetter(1))
	sorted_rules.reverse()
	return sorted_rules

if __name__ == "__main__":
	singles, doubles, triples = apriori(sys.argv[1], int(sys.argv[2]))
	#sorted_double = findDoubles(singles, doubles)
	sorted_triple = findTriples(doubles, triples)
	for k in sorted_triple:
		print k
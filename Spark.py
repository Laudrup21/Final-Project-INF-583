from pyspark import SparkContext, SparkConf
from tqdm import tqdm
import numpy as np
import sys

edgefile = str(sys.argv[1])
nodefile = str(sys.argv[2])
if len(sys.argv) >= 4 :
    iterations = int(sys.argv[3])
else :
    iterations = 5

# Creation of a new spark context
sc = SparkContext()

# Preprocessing of the edges file
rdd1 = sc.textFile(edgefile)
#print([e for e in rdd1.keys().collect() if int(e) > 9])
rdd2 = rdd1.flatMap(lambda x : [(int(x[0]), int(e)) for e in x.split(' ')[1:]])
#print([e for e in rdd2.keys().collect() if e > 9])
adj = rdd2.map(lambda x : (x[0], x[1], 1.0))

# Preprocessing of the nodes file
nodes = sc.textFile(nodefile).map(lambda x : (int(x.split(' ')[0]), ' '.join(x.split(' ')[1:])))

# Number of the nodes and number of edges
N = int(nodes.count())
M = int(adj.count())
print("The graph has "+str(N)+" nodes and "+str(M)+" edges.")

onestep = True #False

if not onestep :
    # A matrix M is stored as a pairRDD with key = j (the column index) and value = ("M", i, Mij)
    A = adj.map(lambda x : (x[1], ("A", x[0], x[2])) ) 
    v = sc.parallelize([i for i in range(N)]).map(lambda x : (x, ("B", 0, 1./N)))
    #"""
    def f(var):
        """
        Args:
            var (tuple): (key, value) where key is the index of a column, a value is a list of elements that have that key in 2 sources RDD

        Returns:
            tuple : the same key, the value is a list, where the values from different sources are multiplied with each other
        """
        listA = [(e[1], e[2]) for e in var[1] if e[0] == "A"]
        listB = [(e[1], e[2]) for e in var[1] if e[0] == "B"]
        liste = []
        for a in listA :
            for b in listB : 
                liste.append((a[0], b[0], a[1]*b[1]))
        return (var[0], liste)
    
    print([e for e in A.values().collect() if e[1] > 10])
    
    for i in tqdm(range(iterations)):
        next_ = (A.union(v)).groupByKey().mapValues(list).map(lambda x : f(x)).flatMap(lambda x : x[1]).map(lambda x : ((x[0], x[1]), x[2]))
        next = next_.reduceByKey(lambda a, b : a+b) # ((i,j), sum AikBkj)
        print(next.take(10))
        #print(str(i)+" "+str(next.count()))
        norm = np.sqrt(next.map(lambda x : x[1]**2).reduce(lambda a, b : a+b))
        print(" iteration "+str(i+1)+" : norm = "+str(norm))
        v = next.map(lambda x : (x[0][0], ("B", x[0][1], x[1]/norm)))
        #print(v.take(10))
    index_max, val = v.max(lambda x : x[1][2])
    max_importance = val[2]
    index_max = v.filter(lambda x : x[1][2] == max_importance).keys().collect()
    name_max = [nodes.lookup(e)[0] for e in index_max]
    if len(name_max) <= 1 :
        print("The most important page is "+str(name_max)+" with an importance value of "+str(max_importance))
    else :
        print("The most important pages are "+str(name_max)+" with an importance value of "+str(max_importance))

else :
    v = sc.parallelize([i for i in range(N)]).map(lambda x : (x, 0, 1./N))
    len_v = v.map(lambda x : x[1]).reduce(lambda x,y: max(x,y)) + 1
    #len_A = adj.map(lambda x : x[0]).reduce(lambda x,y: max(x,y)) + 1

    A = adj.map( lambda x : ((x[0], 0), ("A", x[1], x[2])) )
    for k in range(1, len_v):
        A = A.union(adj.map( lambda x : ((x[0], k), ("A", x[1], x[2]))))

    B = v.map( lambda x : ((0, x[1]), ("B", x[0], x[2])) )
    for p in range(1, N):
        B = B.union(v.map( lambda x : ((p, x[1]), ("B", x[0], x[2])) ))

    def g(var):
        listA = [(e[1],e[2]) for e in var[1] if e[0] == "A"]
        listB = [(e[1],e[2]) for e in var[1] if e[0] == "B"]
        listA.sort(key = lambda t : t[0])
        listB.sort(key = lambda t : t[0])
        
        m, n = len(listA), len(listB)
        z = 0
        r = 0
        sum_ = 0
        
        while(z < m and r < n):
            if listA[z][0] == listB[r][0] :
                sum_ += listA[z][1]*listB[r][1]
                z += 1
                r += 1
            elif listA[z][0] < listB[r][0]:
                z += 1
            elif listA[z][0] > listB[r][0]:
                r += 1 
        return (var[0][0], var[0][1], sum_)

    for i in tqdm(range(iterations)):
        next = (A.union(B)).groupByKey().map(lambda x : g(x)).filter(lambda x : x[2] != 0) 
        norm = np.sqrt(next.map(lambda x : x[2]**2).reduce(lambda a, b : a+b))
        print(" iteration "+str(i+1)+" : norm = "+str(norm))
        B = next.map( lambda x : ((0, x[1]), ("B", x[0], x[2]/norm)) )
        for p in range(1, N):
            B = B.union( next.map( lambda x : ((p, x[1]), ("B", x[0], x[2]/norm)) ))
    
    last = next.map(lambda x : ((x[0], x[1]), x[2]/norm))
    index_max, val = last.max(lambda x : x[1])
    max_importance = val
    index_max = last.map(lambda x : (x[0][0], x[1])).filter(lambda x : x[1] == max_importance).keys().collect()
    name_max = [nodes.lookup(e)[0] for e in index_max]
    max_importance = [e for e in last.map(lambda x : (x[0][0], x[1])).lookup(index_max[0])]
    if len(name_max) <= 1 :
        print("The most important page is "+str(name_max)+" with an importance value of "+str(max_importance))
    else :
        print("The most important pages are "+str(name_max)+" with an importance value of "+str(max_importance))
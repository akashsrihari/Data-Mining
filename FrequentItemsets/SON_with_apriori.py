from pyspark import SparkContext
from operator import add
import sys, re, os
import itertools

filename = sys.argv[1]
sup_ratio = float(sys.argv[2])
output_filename = sys.argv[3]
sc = SparkContext()
num_partitions = 2
lines = sc.textFile(filename,num_partitions)
N = len(lines.collect())

def apriori(x):
    baskets = list(x)
    num_baskets = len(baskets)
    sup_count = sup_ratio*num_baskets
    all_frequents = []
    all_unique_singles = list(set(itertools.chain.from_iterable(baskets)))
    for i in range(1,len(all_unique_singles)+1):
        if i == 1:
            candidates = list(all_unique_singles)
        elif i==2:
            candidates = list(set(itertools.combinations(all_frequents[i-2],i)))
        else:
            candidates = list(set(itertools.combinations(set(itertools.chain.from_iterable(all_frequents[i-2])),i)))
                
        counter = {}
        for j in candidates:
            counter[j] = 0

        for j in baskets:
            if i == 1:
                for k in candidates:
                    if k in j:
                        counter[k] += 1

            else:
                superset = set(j)
                for k in candidates:
                    subset = set(k)
                    if subset.issubset(superset):
                        counter[k] += 1
        oput = []
        for j in counter:
            if counter[j] >= sup_count:
                oput.append(j)
        all_frequents.append(oput)

    return list(itertools.chain.from_iterable(all_frequents))

all_items = lines.map(lambda x:map(int,x.encode("utf-8").split(","))).mapPartitions(apriori).map(lambda x:(x,1)).reduceByKey(lambda a,b:1).collect()

def counterForFinalItems(x):
    baskets = list(x)
    items_with_frequencies = []
    for i in all_items:
        counter=0
        if type(i[0]) == tuple:
            subset = set(i[0])
            for j in baskets:
                superset = set(j)
                if subset.issubset(superset):
                    counter+=1
            
        else:
            for j in baskets:
                if i[0] in j:
                    counter+=1
        items_with_frequencies.append((i[0],counter))
            
    return items_with_frequencies

def stringtoint(x):
    if type(x[0])==int:
        lister=[]
        lister.append(1)
        lister.append(int(x[0]))
        return (tuple(lister),x[1])
    else:
        lister = []
        lister.append(len(x[0]))
        appender = map(lambda a:int(a),list(x[0]))
        appender.sort()
        lister.extend(appender)
        return (tuple(lister),x[1])

def finalmap(x):
    if len(x[0])<=2:
        return (x[0][1],x[1])
    else:
        lister=[]
        for i in range(1,len(x[0])):
            lister.append(x[0][i])
        return (tuple(lister),x[1])

final_frequent_items = lines.map(lambda x:map(int,x.encode("utf-8").split(","))).mapPartitions(counterForFinalItems).reduceByKey(lambda a,b:a+b).filter(lambda x:x[1]>=sup_ratio*N).map(stringtoint).sortByKey().map(finalmap).collect()

f = open(output_filename,"w+")
for i in final_frequent_items:
    if type(i[0])!=tuple:
        f.write(str(i[0]))
        f.write("\n")
    else:
        string = ""
        string += "("
        
        for j in i[0]:
            string+=str(j)
            string+=","
        string = string[:-1]
        string += ")"
        string += "\n"
        f.write(string)
f.close()
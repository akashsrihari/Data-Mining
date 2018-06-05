from __future__ import print_function
import re,sys,os
from operator import add
from pyspark.sql import SparkSession

def computeContribs(urls, rank):
    for url in urls:
        yield (url, rank)

def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return int(parts[0]), int(parts[1])

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    reverse_links = lines.map(lambda urls: parseNeighbors(urls)).map(lambda x: (x[1],x[0])).distinct().groupByKey().cache()
    h = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    for iteration in range(int(sys.argv[2])):
        a = links.join(h).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])).reduceByKey(add)
        mx = a.map(lambda x: x[1]).max()
        a = a.map(lambda x: (x[0],float(x[1])/float(mx))).sortByKey()

        h = reverse_links.join(a).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])).reduceByKey(add)
        mx = h.map(lambda x:x[1]).max()
        h = h.map(lambda x: (x[0],float(x[1])/float(mx))).sortByKey()

    output_filename = sys.argv[3]
    if not os.path.exists(output_filename):
        os.makedirs(output_filename)
    fp = open(output_filename+"/authority.txt","w+")
    a = a.sortByKey().collect()
    h = h.sortByKey().collect()
    for i in a:
        fp.write(str(i[0])+","+str("%.5f" % i[1])+"\n")
    fp.close()
    fp = open(output_filename+"/hub.txt","w+")
    for i in h:
        fp.write(str(i[0])+","+str("%.5f" % i[1])+"\n")
    fp.close()
    spark.stop()

#spark-2.2.1-bin-hadoop2.7/bin/spark-submit test.py Assignment5\(1\)/data/Wiki-Vote.txt 5 op
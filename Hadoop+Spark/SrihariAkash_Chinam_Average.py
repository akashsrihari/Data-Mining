from pyspark import SparkContext
from operator import add
import sys
import re,os

filename = sys.argv[1]
outputdir = sys.argv[2]
sc = SparkContext(appName="Average")
lines=sc.textFile(filename)
op = lines.flatMap(lambda x:x.split("\n")).map(lambda x:x.split(",")).filter(lambda x:len(x[3])>0).map(lambda x:(re.sub(r"[\\\^\[\],\.!?\"/:;@_(){}$&]"," ",x[3]).lower().strip().encode('utf-8'),1)).map(lambda x: (re.sub(r"[-']","",x[0]),1)).map(lambda x:(re.sub(r" +"," ",x[0]),x[1])).filter(lambda x:len(x[0])>0).reduceByKey(add)
op1 = lines.flatMap(lambda x:x.split("\n")).map(lambda x:x.split(",")).filter(lambda x:len(x[3])>0 and x[18].isnumeric()).map(lambda x:(re.sub(r"[\\\^\[\],\.!?\"/:;@_(){}$&]"," ",x[3]).lower().strip().encode('utf-8'),int(x[18]))).map(lambda x: (re.sub(r"[-']","",x[0]),x[1])).map(lambda x:(re.sub(r" +"," ",x[0]),x[1])).filter(lambda x:len(x[0])>0).reduceByKey(add)
op2=op.join(op1).map(lambda x:(x[0],x[1][0],float(x[1][1])/x[1][0])).sortByKey().collect()
opdir = outputdir+"/SrihariAkash_Chinam_task2.txt"

if not os.path.exists(outputdir):
    os.makedirs(outputdir)

with open(opdir,"w+") as fp:
    for i in op2:
        fp.write(i[0]+"\t"+str(i[1])+"\t"+str(i[2])+"\n")
fp.close()

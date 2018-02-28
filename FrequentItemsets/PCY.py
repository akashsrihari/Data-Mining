import sys
import os

intput_filename = sys.argv[1]
a = int(sys.argv[2])
b = int(sys.argv[3])
N = int(sys.argv[4])
s = float(sys.argv[5])
output_filename = sys.argv[6]

f = open(intput_filename)
baskets = f.readlines()

all_items = []

for i in range(len(baskets)):
	baskets[i] = baskets[i].strip().split(",")
	all_items.extend(baskets[i])

all_unique_items = map(lambda x:int(x),list(set(all_items)))
all_unique_items.sort()
counts_table = {}

for i in all_unique_items:
	counts_table[i] = all_items.count(str(i))

del all_items

frequent_singletons = {}
for i in counts_table:
	if counts_table[i] > s:
		frequent_singletons[i] = counts_table[i]

all_pairs = []
for i in all_unique_items:
	for j in all_unique_items:
		if i < j:
			all_pairs.append((int(i),int(j)))

hash_table = {}
for i in range(N):
	hash_table[i]=0

for i in baskets:
	for j in all_pairs:
		if str(j[0]) in i and str(j[1]) in i:
			hash_table[(j[0]*a+j[1]*b)%N]+=1

bitmap = ""
counter = 0
for i in range(N):
	if hash_table[i] >= s:
		bitmap += '1'
		counter+=1
	else:
		bitmap += '0'

del hash_table

candidate_counter = {}
for i in all_pairs:
	if i[0] in frequent_singletons and i[1] in frequent_singletons:
		if bitmap[(i[0]*a+i[1]*b)%N] == '1':
			candidate_counter[i] = 0

for i in baskets:
	for j in candidate_counter:
		if str(j[0]) in i and str(j[1]) in i:
			candidate_counter[j] += 1

frequent_pairs = []
false_candidates = []
for i in candidate_counter:
	if candidate_counter[i] < s:
		false_candidates.append(i)
	else:
		frequent_pairs.append(i)

print "False Positive Rate - ",round(float(counter)/float(N),3)

if not os.path.exists(output_filename):
    os.makedirs(output_filename)

f = open(output_filename+"/frequentset.txt","w+")
for i in frequent_singletons:
	f.write(str(i)+"\n")
frequent_pairs.sort()
for i in frequent_pairs:
	f.write("("+str(i[0])+","+str(i[1])+")"+"\n")
f.close()

f = open(output_filename+"/candidates.txt","w+")
for i in all_pairs:
	if i not in candidate_counter:
		f.write("("+str(i[0])+","+str(i[1])+")"+"\n")
f.close()
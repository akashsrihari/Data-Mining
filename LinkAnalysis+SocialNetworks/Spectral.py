import networkx as nx
import numpy as np
import sys

input_file = sys.argv[1]
k = int(sys.argv[2])
output_file = sys.argv[3]
fp = open(input_file,"r")
lines = fp.readlines()
fp.close()

for i in range(len(lines)):
	lines[i] = lines[i].strip().split(" ")

edges = np.array(lines).astype(int)

G = nx.Graph()
for i in edges:
	G.add_edge(i[0],i[1])

G = list(G.nodes())
G.sort()
graphs = [G]
graph_lengths = [len(G)]

while len(graph_lengths) < k:
	del_index = graph_lengths.index(max(graph_lengths))
	G = list(graphs[del_index])
	H = nx.Graph()
	for i in G:
		H.add_node(i)
	for i in edges:
		if i[0] in G and i[1] in G:
			H.add_edge(i[0],i[1])
	lap = nx.laplacian_matrix(H, nodelist = G).todense()
	val, vec = np.linalg.eigh(lap)
	dicto = dict(zip(val,vec.T))
	val = np.sort(val) 
	node_list = list(np.array(dicto[val[1]]))
	node_list.append(list(H.nodes()))
	liz1 = []
	liz2 = []
	for j in range(len(node_list[0])):
		if node_list[0][j]<0:
			liz1.append(node_list[1][j])
		else:
			liz2.append(node_list[1][j])
	liz1.sort()
	liz2.sort()
	del graphs[del_index], graph_lengths[del_index]
	graphs.extend([liz1,liz2])
	graph_lengths.extend([len(liz1),len(liz2)])

fp = open(output_file, "w")
for i in graphs:
	fp.write(str(i)[1:-1])
	fp.write("\n")
fp.close()
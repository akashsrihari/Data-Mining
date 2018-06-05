import numpy as np
import sys

sample_input = sys.argv[1]
full_input = sys.argv[2]
k = int(sys.argv[3])
n = int(sys.argv[4])
p = float(sys.argv[5])
output_file = sys.argv[6]

fp = open(sample_input,"r")
pts = fp.readlines()
fp.close()

for i in range(len(pts)):
	pts[i] = pts[i].strip().split(",")
	pts[i] = [float(j) for j in pts[i]]

dims = len(pts[0])

while len(pts)>k:
	dist = np.ones((len(pts),len(pts)))*sys.maxint	
	for i in range(len(pts)):
		for j in range(len(pts)):
			if i>=j:
				continue
			one = np.array(pts[i])
			if len(one.shape)==1:
				one = one.reshape(one.shape[0]/dims,dims)
			two = np.array(pts[j])
			if len(two.shape) == 1:
				two = two.reshape(two.shape[0]/dims,dims)
			temp_dist = []
			for m in range(one.shape[0]):
				for l in range(two.shape[0]):
					temp_dist.append(np.power(np.sum(np.power(np.subtract(one[m,:],two[l,:]),2)),0.5))
			dist[i,j] = min(temp_dist)

	min_index = np.unravel_index(np.argmin(dist, axis=None), dist.shape)
	x = np.array(pts[min_index[0]])
	if len(x.shape)==1:
		x = x.reshape(x.shape[0]/dims,dims)
	y = np.array(pts[min_index[1]])
	if len(y.shape)==1:
		y = y.reshape(y.shape[0]/dims,dims)
	z = np.concatenate((x,y),axis=0)
	pts[min_index[0]] = list(z)
	del pts[min_index[1]]

clusters = list(pts)
cluster_centers = np.zeros((k,dims))
for i in range(len(clusters)):
	cluster_centers[i] = np.mean(np.array(clusters[i]),axis=0)

cluster_representatives = []
for i in range(k):
	clust = []
	temp_cluster = np.array(clusters[i])
	clust.append(temp_cluster[np.lexsort((temp_cluster.T[::-1]))][0])
	while len(clust)<n:
		temp_dist = []
		temp_dist_arr = []
		one = np.array(clust)
		for l in range(temp_cluster.shape[0]):
			sm2 = []
			for m in range(one.shape[0]):
				sm2.append(np.power(np.sum(np.power(np.subtract(one[m,:],temp_cluster[l,:]),2)),0.5))
			temp_dist.append(sm2)
		temp_dist = np.amin(np.array(temp_dist),axis=1)
		sorted_temp_dist = np.sort(temp_dist)
		ind = np.where(temp_dist == sorted_temp_dist[-1])
		clust.append(temp_cluster[ind[0][0]])
	clust = np.array(clust)
	for j in range(clust.shape[0]):
		clust[j] += (cluster_centers[i]-clust[j])*p
	cluster_representatives.append(clust)
cluster_representatives = np.array(cluster_representatives)

print "\n"
for i in range(k):
	print "Representatives for cluster "+str(i)+"\n"
	print cluster_representatives[i]
	print "\n"

fp = open(full_input,"r")
pts = fp.readlines()
fp.close()

for i in range(len(pts)):
	pts[i] = pts[i].strip().split(",")
	pts[i] = [float(j) for j in pts[i]]

pts = np.array(pts)
fp = open(output_file,"w+")
for i in range(pts.shape[0]):
	dists = []
	for j in range(k):
		dist_all = np.power(np.sum(np.power(cluster_representatives[j] - pts[i],2),axis=1),0.5)
		dists.append(np.amin(dist_all))
	op = list(pts[i])
	op.append(np.argmin(dists))
	fp.write(",".join(str(j) for j in op))
	fp.write("\n")
fp.close()
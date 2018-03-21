import sys
import numpy as np

ip_filename = sys.argv[1]
n = int(sys.argv[2])
m = int(sys.argv[3])
f = int(sys.argv[4])
it = int(sys.argv[5])

f1 = open(ip_filename)
lines = f1.readlines()
UM = -1*np.ones((n,m))
for i in range(len(lines)):
	lines[i] = lines[i].strip().split(",")
del lines[0]
Y = np.array(lines)[:,0:3]
yaxis = np.unique(Y[:,1].astype(int))
dicto={}
for i in range(len(yaxis)):
	dicto[yaxis[i]] = i

for i in Y:
	UM[int(i[0])-1,dicto[int(i[1])]] = float(i[2])

UM[UM==-1] = np.nan
U = np.ones((n,f))
V = np.ones((f,m))

for i in range(it):
	for j in range(n):	
		for k in range(f):
			V_mod = np.array(V[k,:])
			V_mod[np.isnan(UM[j,:])] = np.nan
			x = float(np.nansum(V[k,:]*(UM[j,:]-np.dot(U[j,:],V)+U[j,k]*V[k,:])))
			y = float(np.nansum(np.power(V_mod,2)))
			U[j,k] = x/y

	for k in range(m):
		for j in range(f):
			U_mod = np.array(U[:,j])
			U_mod[np.isnan(UM[:,k])] = np.nan
			x = float(np.nansum(U[:,j]*(UM[:,k]-np.dot(U,V[:,k])+U[:,j]*V[j,k])))
			y = float(np.nansum(np.power(U_mod,2)))
			V[j,k] = x/y

	M1 = np.dot(U,V)
	diff = np.power(UM-M1,2)
	print "%.4f" % (np.power(np.nanmean(diff),0.5))
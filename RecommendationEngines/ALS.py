from __future__ import print_function

import sys

import numpy as np
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession

LAMBDA = 0.01   # regularization
np.random.seed(42)


def rmse(R, ms, us):
    diff = R - ms * us.T
    return np.sqrt(np.sum(np.power(diff, 2)) / (M * U))


def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]

    XtX = mat.T * mat
    Xty = mat.T * ratings[i, :].T

    for j in range(ff):
        XtX[j, j] += LAMBDA * uu

    return np.linalg.solve(XtX, Xty)


if __name__ == "__main__":

    """
    Usage: als [M] [U] [F] [iterations] [partitions]"
    """

    print("""WARN: This is a naive implementation of ALS and is given as an
      example. Please use pyspark.ml.recommendation.ALS for more
      conventional use.""", file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()

    sc = spark.sparkContext
    input_path = sys.argv[1] if len(sys.argv) > 1 else "ratings_task2.csv"
    M = int(sys.argv[2]) if len(sys.argv) > 2 else 671
    U = int(sys.argv[3]) if len(sys.argv) > 3 else 9066
    F = int(sys.argv[4]) if len(sys.argv) > 4 else 60
    ITERATIONS = int(sys.argv[5]) if len(sys.argv) > 5 else 10
    partitions = int(sys.argv[6]) if len(sys.argv) > 6 else 2
    output_path = sys.argv[7] if len(sys.argv) > 7 else "output.txt"    

    print("Running ALS with M=%d, U=%d, F=%d, iters=%d, partitions=%d\n" %
          (M, U, F, ITERATIONS, partitions))

    f1 = open(input_path)
    lines = f1.readlines()
    UM = -1*np.ones((M,U))
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
    
    R = np.matrix(UM)
    ms = matrix(np.ones((M,F)))
    us = matrix(np.ones((U,F)))

    Rb = sc.broadcast(R)
    msb = sc.broadcast(ms)
    usb = sc.broadcast(us)
    
    op = open(output_path,"w+")    

    for i in range(ITERATIONS):
        ms = sc.parallelize(range(M), partitions) \
               .map(lambda x: update(x, usb.value, Rb.value)) \
               .collect()
        # collect() returns a list, so array ends up being
        # a 3-d array, we take the first 2 dims for the matrix
        ms = matrix(np.array(ms)[:, :, 0])
        msb = sc.broadcast(ms)

        us = sc.parallelize(range(U), partitions) \
               .map(lambda x: update(x, msb.value, Rb.value.T)) \
               .collect()
        us = matrix(np.array(us)[:, :, 0])
        usb = sc.broadcast(us)

        error = rmse(R, ms, us)
        op.write("%.4f"%error)
        op.write("\n")
        print("Iteration %d:" % i)
        print("\nRMSE: %5.4f\n" % error)
    op.close()
    spark.stop()
Python version - 2.7

Spark version - 2.2.1




Files in this folder - 

SrihariAkash_Chinam_Average.py
SrihariAkash_Chinam_Average.java
SrihariAkash_Chinam_avg.jar
SrihariAkash_Chinam_task1.txt
SrihariAkash_Chinam_task2.txt





Executing Hadoop Mapreduce program:

bin/hadoop jar SrihariAkash_Chinam_avg.jar SrihariAkash_Chinam_Average input-dir output-dir


Note: File saved in output-dir as part-r-00000





Executing Spark program of python:

bin/spark-submit SrihariAkash_Chinam_Average.py menu.csv output-dir


Note : This will save output file as "SrihariAkash_Chinam_task2.txt" in output-dir
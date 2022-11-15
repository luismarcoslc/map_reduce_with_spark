from pyspark.sql import SparkSession
from pyspark.sql.dataframe import *
from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def main():

	# SPARK SETUP
	#conf = SparkConf().setAppName('retailer_program').setMaster("local[*]")
	#sc = SparkContext(conf=conf)
	spark = SparkSession.builder.master("local[*]") \
                    .appName('retailer_program') \
                    .getOrCreate()
	sc = spark.sparkContext

	# INPUT READING
	assert len(sys.argv) == 5, "Usage: python G006HW1.py <K> <H> <S> <file_name>"
	
	# Read number of partitions
	K = sys.argv[1]
	assert K.isdigit(), "K must be an integer"
	K = int(K)

	H = sys.argv[2]
	assert H.isdigit(), "H must be a number"
	H = int(H)

	S = sys.argv[3]
	assert isinstance(S, str), "S must be a string"
	S = str(S)

	data_path = sys.argv[4]
	assert os.path.isfile(data_path),"File or folder not found"

	#############  1 #######################

	# Read input file and subdivide it into K random partitions
	rawData = sc.textFile(data_path, minPartitions=K).cache()
	rawData.repartition(numPartitions=K)
	
	print("Number of rows = ",rawData.count())

	#############  2 #######################

	rawData = rawData.map(lambda s: s.split(","))

	if S == "all":
		rawData = rawData.filter(lambda x: int(x[3]) > 0)
	else:
		rawData = rawData.filter(lambda x: int(x[3]) > 0 and x[7] == S)

	productCustomer = rawData.map(lambda x: ((x[1], int(x[6])), 1)).reduceByKey(lambda x,y : x + y).map(lambda x: (x[0][0],x[0][1]))	 
	
	#Can also be done by converting to data frame and dropping duplicates
	#productCustomer = rawData.map(lambda x: (x[1], int(x[6])))
	#productCustomer = productCustomer.toDF().dropDuplicates().rdd
	
	print("Product-Customer Pairs = " , productCustomer.count())

	#############  3 #######################

	def partition_worker(partitionData):
		updated_data = []
		for x in partitionData:
			updated_data.append((x[0],1))
		return updated_data


	productPopularity1 = productCustomer.mapPartitions(partition_worker).groupByKey().mapValues(lambda vals: sum(vals))
	#print("productPopularity1: ")
	#for i in sorted(productPopularity1.collect()):
	#	print("Product: ", i[0], "Popularity: ", i[1], end = "; " )
	#print(end = "\n")


	# Alternative way
	#dict = productCustomer.sortByKey().countByKey()
	#print("productPopularity1: ")
	#for key, value in dict.items():
		#print("Product: ", key, "Popularity: ", value, end = "; " )

	#############  4 #######################

	productPopularity2 = productCustomer.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
	#print("productPopularity2: ")
	#for i in sorted(productPopularity2.collect()):
	#	print("Product: ", i[0], "Popularity: ", i[1], end = "; " )
	#print(end = "\n")

	#############  5 #######################

	if H > 0:

		top_popular = productPopularity1.takeOrdered(H, lambda x: (-x[1], x[0]))
		print(f"Top {H} Products and their Popularities: ")
		for i in top_popular:
			print("Product ", i[0], "Popularity ", i[1], end = "; " )
		print(end = "\n")


	# Alternative way
	#if H > 0:
	#	top_popular = productPopularity2.sortBy(lambda x: (-x[1], x[0])).take(H)
	#	print(f"Top {H} Products and their Popularities: ")
	#	for i in top_popular:
	#		print("Product ", i[0], "Popularity ", i[1], end = "; " )
	#	print(end = "\n")

	#############  6 #######################

	if H == 0:
		top_popular1 = productPopularity1.takeOrdered(productPopularity1.count(), lambda x: x[0])
		print("Products ordered in increasing lexicographic order of ProductID (productPopularity1) ", top_popular1)

		top_popular2 = productPopularity2.takeOrdered(productPopularity2.count(), lambda x: x[0])
		print("Products ordered in increasing lexicographic order of ProductID (productPopularity2)", top_popular2)


if __name__ == "__main__":
	main()


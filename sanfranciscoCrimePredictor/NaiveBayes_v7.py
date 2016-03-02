#pyspark --packages com.databricks:spark-csv_2.10:1.3.0
import numpy as np
import random
import decimal
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext
import collections
from collections import OrderedDict

#get spark context
sc = SparkContext(appName="APProject")
#get SQL context
sqlContext = SQLContext(sc)

#first argument is data file name
#dataFilePath="train.csv"
dataFilePath=sys.argv[1]
#second argument is pdDistrict (region) name
#pdDistrict='Central'
pdDistrict=sys.argv[2]
#third argument is day of week
day=sys.argv[3]
#day="Tuesday"
Alpha=sys.argv[4]
#Alpha=sys.argv[4]
Vocabulary= 10 + 7
#load csv data into data frame
trainDataFrame = sqlContext.load(source="com.databricks.spark.csv", header="true", path = dataFilePath)
trainDataFrame.registerTempTable("trainDataFrame")

#get crime category and count of all incidents
#Row(Category=u'ASSAULT', count=76876)
#n
CattegoriesAndCount=trainDataFrame.groupBy("Category").count()

# Calculate Probability of Category
Cnt= trainDataFrame.count()
CattegoriesAndCount2=trainDataFrame.groupBy("Category").count()
ProbCat=CattegoriesAndCount2.map(lambda x: (x.Category, x.count/float(Cnt)))

#get count of incidents where crime category and day of week are as per given
#Row(DayOfWeek=u'Saturday', PdDistrict=u'NORTHERN', Category=u'GAMBLING', cnt=1)

QueryCategoryAndCount=sqlContext.sql("SELECT  PdDistrict, Category, COUNT(*) AS cnt FROM trainDataFrame WHERE PdDistrict in ('"+str(pdDistrict)+"') GROUP BY Category,PdDistrict ORDER BY cnt")

QueryCategoryAndCount2=sqlContext.sql("SELECT DayOfWeek, Category, COUNT(*) AS cnt FROM trainDataFrame WHERE  DayOfWeek in('"+str(day)+"') GROUP BY Category, DayOfWeek ORDER BY cnt")

CattegoriesAndCount=CattegoriesAndCount.collect()
QueryCategoryAndCount=QueryCategoryAndCount.collect()
QueryCategoryAndCount2=QueryCategoryAndCount2.collect()

#create RDD of given query and count
QueryCategoryAndCountRDD=sc.parallelize(QueryCategoryAndCount)
QueryCategoryAndCountRDD2=sc.parallelize(QueryCategoryAndCount2)
#create map with category as key and count of incidents of that category
QueryCategoryAndCountMapRDD=QueryCategoryAndCountRDD.map(lambda x: (x.Category, x.cnt))
QueryCategoryAndCountMapRDD2=QueryCategoryAndCountRDD2.map(lambda x: (x.Category, x.cnt))

prob={}
#for each category
for rowN in CattegoriesAndCount:
	#init probability to 0
	prob[float(0)]=(rowN.Category)
	#lookup in the query RDD by category name to get the respective count
	nkArr=QueryCategoryAndCountMapRDD.lookup(rowN.Category)
	nkArr2=QueryCategoryAndCountMapRDD2.lookup(rowN.Category)
	nkArr3= ProbCat.lookup(rowN.Category)
	#if category not present in query result
	if(len(nkArr)==0):
		nk=0
	else:
		#get nk for District
		nk=float(QueryCategoryAndCountMapRDD.lookup(rowN.Category)[0]) + Alpha
	if(len(nkArr2)==0):
		nk2=0
	else:
		#get nk for Days
		nk2=float(QueryCategoryAndCountMapRDD2.lookup(rowN.Category)[0]) + Alpha

	if(len(nkArr3)==0):
		ProbabilityCategory=0
	else:
		ProbabilityCategory = float(ProbCat.lookup(rowN.Category)[0]) + (Alpha * Vocabulary)

	#calculate probability for given category
	x=float(nk/rowN.count)*float(nk2/rowN.count)*ProbabilityCategory
	prob[x]=(rowN.Category)
	#prob[float(nk/rowN.count)*float(nk2/rowN.count)]=(rowN.Category)

#sort probability
oderedProbs = collections.OrderedDict(sorted(prob.items()))
#display most likely crimes
crimes = list(oderedProbs.items())
print crimes[-1]
print crimes[-2]
print crimes[-3]

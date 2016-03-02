#pyspark --packages com.databricks:spark-csv_2.10:1.3.0
import numpy as np
import random
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext

#get spark context
sc = SparkContext(appName="APProject")
#get SQL context
sqlContext = SQLContext(sc)
#first argument is data file name
dataFilePath=sys.argv[1]
#dataFilePath="train.csv"

#create data frame from the given CSV file
trainDataFrame = sqlContext.load(source="com.databricks.spark.csv", header="true", path = dataFilePath)
trainDataFrame.registerTempTable("trainDataFrame")

#input: point and array of known centroids
#output: if given point is centroid returns centroid name else returns "Not Centroid"
def isCentroid(point, centroidArray):
	i=1
	for centroid in centroidArray:
		if(("%.6f" %point[0]=="%.6f" %centroid[1][0]) & ("%.6f" %point[1]=="%.6f" %centroid[1][1])):
			return "C"+str(i)
		i=i+1
	return "Not Centroid"
	
#input: point and array of known centroids
#output: if point is not centroid then calculate centroid and return the name else if point is centroid return its name 
def getCentroid(point, centroidArray):
	res=isCentroid(point, centroidArray)
	if(res=="Not Centroid"):			
		myCentroid=centroidArray[0]
		i=1
		min=1
		#loop thorough all known centroids
		for centroid in centroidArray:	
			#if the distance of any point is less than minimum update minimum
			if(distance(point,centroid[1])<=distance(point,myCentroid[1])):
				myCentroid=centroid
				min=i
				min
			i=i+1
		#return centroid name
		return "C"+str(min) 	
			
	else:
		return res
	
#calculate euclidean distance between point and a centroid
def distance(point, centroid):
    return np.sqrt((point[0] - centroid[0])**2 + (point[1] - centroid[1])**2)
	 
#fourth argument is crime category
category=sys.argv[4]
#category='GAMBLING'

#get longitudes and latitudes from the data frame
latitudes=sqlContext.sql("SELECT X FROM trainDataFrame WHERE Category in ('"+str(category)+"')").rdd.map(lambda r: float(r.X))
longitudes=sqlContext.sql("SELECT Y FROM trainDataFrame WHERE Category in ('"+str(category)+"')").rdd.map(lambda r: float(r.Y))

longitudesArray=np.array(longitudes.collect())[0:(len(longitudes.collect()))]
latitudesArray=np.array(latitudes.collect())[0:(len(longitudes.collect()))]

#points is too large
#Job aborted due to stage failure: Serialized task 15:0 was 21007939 bytes, which exceeds max allowed: spark.akka.frameSize (10485760 bytes) - reserved (204800 bytes). Consider increasing spark.akka.frameSize or using broadcast variables for large values.

#get random n centroids of the clusters to form centroids RDD
#second argument is k
n=int(sys.argv[2]) 
#n=3
clusterIndices=random.sample(range(0, ((len(longitudes.collect()))-1)), (n-1))
centroids=np.array([latitudesArray[0],longitudesArray[0]])
for index in clusterIndices:	
	centroids=np.vstack((centroids, [np.array([latitudesArray[index],longitudesArray[index]])]))

centroids = map(lambda (i,x): ("C"+str(i+1), x), enumerate(centroids))
centroids=sc.parallelize(centroids)
centroidArray=[]

#get all points in a RDD
points = map(lambda x,y:(x,y),latitudesArray,longitudesArray)
points=sc.parallelize(points)

#third argument is number of iterations
itertaions=int(sys.argv[3])
#itertaions=3
loopCount=0
while (loopCount<itertaions):	

	#('C3', (-122.41238895148561, 37.796053226084631))
	
	centroidArray=centroids.collect()
	#get clusters as per new centroids 	
	clusters=points.map(lambda point: (getCentroid(point, centroidArray), point))
	
	#calculate centroids for the new cluster	
	centroids=clusters.reduceByKey(lambda a,b: ((a[0]+b[0])/2, (a[1]+b[1])/2))
	
	#to debug the centroids formed
	#centroid1=centroids.filter(lambda a: "C1" in a).collect()[0][1]
	#centroid2=centroids.filter(lambda a: "C2" in a).collect()[0][1]		
	loopCount=loopCount+1

#form cluster as per latest centroids
centroidArray=centroids.collect()
clusters=points.map(lambda point: (getCentroid(point, centroidArray), point))
clustersRDD=sc.parallelize(clusters.collect())

#to check the clusters formed
i=1
listOfClustersCount=[]
while (i<=n):
	listOfClustersCount.append(clustersRDD.filter(lambda a: "C"+str(i) in a).count())
	i=i+1

i=1
while (i<=n):
	#clustersRDD.filter(lambda a: "C"+str(i) in a).collect()
	print "C"+str(i)+" "+str(listOfClustersCount[i-1])	
	#pointsInC2=clustersRDD.filter(lambda a: "C2" in a).collect()
	i=i+1

		
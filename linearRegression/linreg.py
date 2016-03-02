# linreg.py
#
# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# TODO: Write this.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile>
# Example usage: spark-submit linreg.py yxlin.csv
#
#

import sys
import numpy as np

from pyspark import SparkContext


if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    exit(-1)

  sc = SparkContext(appName="LinearRegression")

  # Input yx file has y_i as the first element of each line 
  # and the remaining elements constitute x_i
  yxinputFile = sc.textFile(sys.argv[1])
  yxlines = yxinputFile.map(lambda line: line.split(','))
  yxfirstline = yxlines.first()
  yxlength = len(yxfirstline)
  #print "yxlength: ", yxlength

  # dummy floating point array for beta to illustrate desired output format
  beta = np.zeros(yxlength, dtype=float)
  
  #Create RDDs 
  xRDD=yxlines.map(lambda x: (x[1:len(x)]), yxlines)
  xArr=np.array(xRDD.collect())
  #yArr=np.array(yRDD.collect(), dtype=float)

  #add ones for the bias
  xOnes=np.ones((len(xArr),len(xArr[0])+1), dtype=float)
  xOnes[:,1:len(xOnes)] = xArr
  xArr=xOnes
  xRDD=sc.parallelize(xArr)
  ones=np.ones((len(xArr),1), dtype=float)

  xyArr=np.array(yxlines.collect(), dtype=float)
  xyArr=np.insert(xyArr, [1],ones, axis=1)
  yxlines=sc.parallelize(xyArr)

  yRDD=yxlines.map(lambda y: (y[0]), yxlines)
	
  #map 1
  keyA=xRDD.map(lambda x: ("keyA",np.array(x, dtype=float).reshape(len(x),1)*np.transpose(np.array(x, dtype=float)).reshape(1,len(x))))
	
  #map2
  keyB=yxlines.map(lambda X: ("keyB",(np.array(X, dtype=float)[1:len(X)].reshape(len(X)-1,1)*np.array(X, dtype=float)[0].reshape(1,1))))
	
  #reduce1
  A = keyA.reduceByKey(lambda a, b: a + b).values()
	
  #reduce2
  B = keyB.reduceByKey(lambda a, b: a + b).values()
	
  A=(np.array(A.collect()))
  A=np.squeeze(A)
  B=np.array(B.collect())	
  Ainv=np.linalg.pinv(A)	
  beta=np.dot(Ainv,B)
  #print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  #
  # Add your code here to compute the array of 
  # linear regression coefficients beta.
  # You may also modify the above code.
  #

  # print the linear regression coefficients in desired output format
  print "beta: "
  for coeff in beta:
      print (coeff[0])[0]

  sc.stop()
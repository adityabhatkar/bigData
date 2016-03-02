====================================================================================================
Aditya Bhatkar 800887086
====================================================================================================


Execution Steps

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* WikiPageRanker.java -d build –Xlint
jar -cvf wikiPageRanker.jar -C build/ .
hadoop jar wikiPageRanker.jar  WikiPageRanker <inputPath> <outputPath>
====================================================================================================
====================================================================================================

Output folders

Output is generated inside provided  <outputPath>
For each job separate folder is created.
Only Job3 output is persisted, rest are deleted.
Final output can be found at <outputPath>/Job3
====================================================================================================
====================================================================================================

Assumptions 

Delimiter is chosen as ‘####’ to separate links from pages.
A distinguisher ‘$$$$’ is used to separate links representation from original input.
====================================================================================================
====================================================================================================







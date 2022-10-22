install spark
run the spark file with the command: ./spark-3.2.1-bin-hadoop3.2/bin/spark-submit ./Spark.py ./edgelist2.txt ./idslabels2.txt 30
where ./Spark.py ./edgelist2.txt ./idslabels2.txt 30 represent arg[0],arg[1],arg[2],arg[3]
you can have a different version of spark and hadoop so check you own version because this command is specific to my version.
All the file are in the same folder as the bin folder installation of pyspark.
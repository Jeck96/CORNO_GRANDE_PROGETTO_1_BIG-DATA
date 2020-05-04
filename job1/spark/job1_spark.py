#from pyspark.sql import SparkSession
import pyspark as ps

spark = ps.sql.SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
#df = spark.read.load("/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/test-progetto.csv",
                   #  format="csv", sep=",", inferSchema="true", header="true")

df=spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/test-progetto.csv',inferSchema="true", header="true")
#df.filter(df.date>'2008').show()

azioni = df.rdd
azioni = azioni.filter(lambda a: a['date']>'2008').map(lambda a: (a[0],a)).groupByKey()
azioni.foreach(lambda a: print (a))
#azioni.collect()
#azioni.saveAsTextFile("output.txt")
#output.saveAsTextFile("output.txt")

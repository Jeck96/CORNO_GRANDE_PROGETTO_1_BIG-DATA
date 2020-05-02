#from pyspark.sql import SparkSession
import pyspark as ps

spark = ps.sql.SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
df = spark.read.load("/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/test-progetto.csv",
                     format="csv", sep=",", inferSchema="true", header="true")
df.filter(lambda r_df: r_df['2007-02-20'].split('-')[0]>'2008')
df.show()
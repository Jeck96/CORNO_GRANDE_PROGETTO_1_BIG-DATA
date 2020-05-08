#from pyspark.sql import SparkSession
import pyspark as ps

spark = ps.sql.SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

df_azioni=spark.read.csv('/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/azioni_test.csv')
df_settori=spark.read.csv('/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/settori_test.csv')

azioni = df_azioni.rdd
settori = df_settori.rdd

print("\nazioni:\n")
azioni.foreach(lambda a: print (a))
("\nsettori:\n")
settori.foreach(lambda a: print (a))

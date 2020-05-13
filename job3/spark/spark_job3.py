from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark job 3 for Big Data project").config("spark.some.config.option", "some-value").getOrCreate()

df_azioni=spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/test-medio.csv',inferSchema="true", header="true")

df_aziende = spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/historical_stocks.csv',inferSchema="true", header="true")

#filtro direttamente il dataframe prima di fare il join e tolgo gli attributi che non mi servono
df_azioni = df_azioni.filter(df_azioni.date > '2016-01-01').join(df_aziende,on="ticker")\
                                                    .drop('open').drop('adj_close')\
                                                    .drop('low').drop('high').drop('volume')\
                                                    .drop('exchange').drop('sector').drop('industry')

rdd_base = df_azioni.rdd

print("\nresult finale:\n")
rdd_base.foreach(lambda a: print(a))
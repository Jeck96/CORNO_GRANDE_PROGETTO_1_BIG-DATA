#from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import pyspark as ps
from datetime import datetime

#spark = ps.sql.SparkSession.builder.appName("Python Spark job 1 for Big Data project").config("spark.some.config.option", "some-value").getOrCreate()

#df=spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/historical_stock_prices.csv',inferSchema="true", header="true")
#df=spark.read.csv('/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stock_prices.csv',inferSchema="true", header="true")
conf = SparkConf().setAppName("Job1 Spark")
sc = SparkContext(conf=conf)
azioni = sc.textFile("/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stock_prices_update.csv")
#df=spark.read.csv('/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/azioni_test.csv',inferSchema="true", header="true")

"""
un'azione è così definita:
        "ticker": azione[0],
        "open": azione[1],
        "close": azione[2],
        "adj_close": azione[3]
        "low": azione[4],
        "high": azione[5],
        "volume": azione[6],
        "data": azione[7],
"""
#azioni_df =spark.sparkContext.parallelize(df)
#azioni = azioni_df.rdd

#filtriamo le azioni, considerando solo quelle che hanno data maggiore del 2018


azioni = azioni.map(lambda f: f.split(','))
azioni = azioni.filter(lambda a: a[7]>='2008-01-01')
info_per_azione = azioni.map( lambda a: (a[0], [ float(a[2]), float(a[2]) ]) )
#prezzo_min_per_azione.foreach(lambda a: print(a)
def rid(a1,a2):

    prezzo_min=min(a1[0],a2[0])
    prezzo_max=max(a1[1],a2[1])
    return [prezzo_min,prezzo_max]

#info_per_azione.foreach(lambda a:print(a))
info_per_azione = info_per_azione.reduceByKey(rid)

info_per_azione.saveAsTextFile("results/prezzo_min_max.txt")

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
spark.close()

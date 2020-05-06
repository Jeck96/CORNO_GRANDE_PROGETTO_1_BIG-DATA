#from pyspark.sql import SparkSession
import pyspark as ps

spark = ps.sql.SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
#df = spark.read.load("/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/test-progetto.csv",
                   #  format="csv", sep=",", inferSchema="true", header="true")

df=spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/test-progetto.csv',inferSchema="true", header="true")
#df=spark.read.csv('/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/azioni_test.csv',inferSchema="true", header="true")
#df.filter(df.date>'2008').show()

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
azioni = df.rdd

"""
definiamo un map (K,V) tale che:
    -K è il simbolo dell'azione
    -V è una lista che contiene le info riguardanti l'azione ed in particolare:
        V[0] = prezzo di chiusura (azione[2])
        V[2] = data (azione[7])
"""
azioni = azioni.map(lambda line: ( line[0], [line[2],line[7]] ) )
print ("\nprima del reduceByKey:\n")
azioni.foreach(lambda a: print (a))


min_azioni=azioni.reduceByKey(min)
max_azioni = azioni.reduceByKey(max)

#ci salviamo in una lista l'RDD contenente i valori minimi per ciascun simbolo_azione
list_min_azioni=min_azioni.collect()
#ci salviamo in una lista l'RDD contente i valori massimi ci ciascun simbolo_azione
list_max_azioni = max_azioni.collect()



print ("\nDopo il reduceByKey:\n")
print("valori minimi:\n")
min_azioni.foreach(lambda a: print (a))


print("\nvalori massimi:\n")
max_azioni.foreach(lambda a: print (a))
#azioni.saveAsTextFile("output")

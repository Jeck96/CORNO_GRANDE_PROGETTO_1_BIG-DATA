#from pyspark.sql import SparkSession
import pyspark as ps

spark = ps.sql.SparkSession.builder.appName("Python Spark job 1 for Big Data project").config("spark.some.config.option", "some-value").getOrCreate()
#df = spark.read.load("/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/test-progetto.csv",
                   #  format="csv", sep=",", inferSchema="true", header="true")

df=spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/historical_stock_prices.csv',inferSchema="true", header="true")
#df=spark.read.csv('/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/azioni_test.csv',inferSchema="true", header="true")
#df=spark.read.csv('/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stock_prices.csv',inferSchema="true", header="true")

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
                K = "ticker"
                V[0] = "close"
                V[1] = "volume"
                V[2] = "date"
"""
azioni = azioni.filter(lambda a: a[7]>='2008-01-01').map(lambda line: ( line[0], [line[2],line[6],line[7]] ) )

"""print ("\nprima del reduceByKey:\n")
azioni.foreach(lambda a: print (a))
"""
"""
def funzione_reduce(a1,a2):
    prezzo_min=min(a1[0],a2[0])
    data_min = min(a1[1],a2[1])
    return [prezzo_min,data_min]

min_azioni=azioni.reduceByKey(min)
max_azioni = azioni.reduceByKey(max)
"""
def f_red_var_percent(a1,a2):
    if(a1[0]<a2[0]):
        data_iniziale = a1[0]
        close_iniziale = a1[1]
    else:
        data_iniziale = a2[0]
        close_iniziale = a2[1]
    if(a1[2]>a2[2]):
        data_finale = a1[2]
        close_finale = a1[3]
    else:
        data_finale = a2[2]
        close_finale = a2[3]
    return ([data_iniziale,close_iniziale,data_finale,close_finale])

var_perc = azioni.map( lambda a: (a[0],[a[1][2],a[1][0],a[1][2],a[1][0]]) ).reduceByKey(f_red_var_percent)
result_var_perc = var_perc.map(lambda a: (a[0],(100*(a[1][3]-a[1][1])/a[1][1])))
result_var_perc.saveAsTextFile("output")

list_var_perc = result_var_perc.collect()

for i in list_var_perc:
    print (i)
"""

for i in result_var_perc.collect():
    print (i)
"""
#calcolo volume medio
def f_red_avg_vol(a1,a2):
        return (a1[0]+a2[0],a1[1]+a2[1])
avg_vol = azioni.map(lambda a: (a[0],(a[1][1],1))).reduceByKey(f_red_avg_vol)
for t,v in avg_vol.collect():
        print(t,v[0]/v[1])
"""
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
"""
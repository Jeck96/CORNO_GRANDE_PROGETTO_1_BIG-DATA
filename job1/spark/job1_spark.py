#from pyspark.sql import SparkSession
import pyspark as ps
from datetime import datetime
from pyspark import SparkContext, SparkConf
spark = ps.sql.SparkSession.builder.appName("Python Spark job 1 for Big Data project").config("spark.some.config.option", "some-value").getOrCreate()

#df=spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/historical_stock_prices.csv',inferSchema="true", header="true")
df=spark.read.csv('/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stock_prices.csv',inferSchema="true", header="true")
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
#conf = SparkConf().setAppName("Job1 Spark")
#sc = SparkContext(conf=conf)
#azioni = sc.textFile("/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stock_prices_update.csv")


#azioni = azioni.map(lambda f: f.split(','))
#azioni = azioni.filter(lambda a: a[7]>='2008-01-01')
azioni = df.rdd
"""
definiamo un map (K,V) tale che:
                K = 'AHH'
                V: {
                    'prezzo_close_min' : 11.57
                    'prezzo_close_max' : 11.57
                    'somma_volume' : 1984
                    'data_min' : '2010-05-20'
                    'prezzo_iniziale': 11.57
                    'data_max' : '2010-05-20'
                    'prezzo_finale': 11.57
                    }
"""
#filtriamo le azioni, considerando solo quelle che hanno data maggiore del 2018
azioni = azioni.filter(lambda a: a[7]>='2008-01-01').map(lambda line: ( line[0], {'prezzo_close_min':float(line[2]),
                                                                                  'prezzo_close_max':float(line[2]),
                                                                                  'somma_volume':float(line[6]),
                                                                                  'data_min':line[7],
                                                                                  'prezzo_iniziale': float(line[2]),
                                                                                  'data_max':line[7],
                                                                                  'prezzo_finale':float(line[2])} ) )

def rid(a1,a2):
    a ={}
    a['prezzo_close_min']=min(a1['prezzo_close_min'],a2['prezzo_close_min'])
    a['prezzo_close_max'] = max(a1['prezzo_close_max'], a2['prezzo_close_max'])
    a['somma_volume'] = a1['somma_volume']+a2['somma_volume']

    if a1['data_min']<a2['data_min']:
        a['data_min']=a1['data_min']
        a['prezzo_iniziale']=a1['prezzo_iniziale']
    else:
        a['data_min'] = a2['data_min']
        a['prezzo_iniziale'] = a2['prezzo_iniziale']

    if a1['data_max']>a2['data_max']:
        a['data_max']=a1['data_max']
        a['prezzo_finale']=a1['prezzo_finale']
    else:
        a['data_max'] = a2['data_max']
        a['prezzo_finale'] = a2['prezzo_finale']

    return a

count_per_ticker = azioni.groupByKey().map(lambda a:(a[0],len(list(a[1]))))

result_per_ticker = azioni.reduceByKey(rid)

unione = result_per_ticker.join(count_per_ticker)

#print("\nunione:\n")
#unione.foreach(lambda a:print(a))

result_finale = unione.map(lambda a: (a[0],{
                                            'var_percentuale':(100*(a[1][0]['prezzo_finale']-a[1][0]['prezzo_iniziale'])/a[1][0]['prezzo_iniziale']),
                                            'volume_medio':a[1][0]['somma_volume']/a[1][1],
                                            'prezzo_minimo':a[1][0]['prezzo_close_min'],
                                            'prezzo_massimo':a[1][0]['prezzo_close_max']
                                            }))
print("qui")

result_finale = result_finale.sortBy((lambda row: float(row[1]['var_percentuale'])), ascending=False)
#risultato = result_finale.collect()
#print("\nresult:\n")
#result_finale.foreach(lambda a:print(a))
result_finale.saveAsTextFile("/home/giacomo/PycharmProjects/corno_grande_progetto1/job1/spark/results")

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)


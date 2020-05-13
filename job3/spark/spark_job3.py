from pyspark.sql import SparkSession
import costanct as C

spark = SparkSession.builder.appName("Python Spark job 3 for Big Data project").config("spark.some.config.option", "some-value").getOrCreate()

df_azioni=spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/test-medio.csv',inferSchema="true", header="true")

df_aziende = spark.read.csv('/home/adfr/Documenti/python-BigData/progetto1/csv_progetto/historical_stocks.csv',inferSchema="true", header="true")

#filtro direttamente il dataframe prima di fare il join e tolgo gli attributi che non mi servono
df_azioni = df_azioni.filter(df_azioni.date > '2016-01-01').join(df_aziende,on="ticker")\
                                                    .drop('open').drop('adj_close')\
                                                    .drop('low').drop('high').drop('volume')\
                                                    .drop('exchange').drop('sector').drop('industry')
def reduce_variazione_annua(v1,v2):
    res = {}
    if(v1['first_date']<v2['first_date']):
         res['first_date'] = v1['first_date']
         res['first_price'] = v1['first_price']
    else:
        res['first_date'] = v2['first_date']
        res['first_price'] = v2['first_price']
    
    if(v1['last_date']>v2['last_date']):
         res['last_date'] = v1['last_date']
         res['last_price'] = v1['last_price']
    else:
        res['last_date'] = v2['last_date']
        res['last_price'] = v2['last_price']
    return res

rdd_base = df_azioni.rdd
for anno in C.ANNI:
    rdd_variazione = rdd_base.filter(lambda row: row['date'][0:4]==str(anno)).map(lambda row: (row['name'],{\
        "first_date":row['date'],"first_price":row['close'],"last_date":row['date'],"last_price":row['close']}))
    rdd_variazione = rdd_variazione.reduceByKey(reduce_variazione_annua)
    rdd_variazione = rdd_variazione.map(lambda c: (c[0],100*(c[1]['first_price']-c[1]['last_price'])/c[1]['first_price']))
    rdd_variazione.foreach(lambda a: print(a[0],a[1],"%"))
"""
print("\nresult finale:\n")
rdd_base.foreach(lambda a: print(a))
"""
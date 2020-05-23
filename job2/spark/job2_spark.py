#from pyspark.sql import SparkSession
#from datetime import datetime
from pyspark import SparkContext

sc = SparkContext(appName = "job2_Spark")

#now = datetime.now()
#current_time = now.strftime("%H:%M:%S")
#print("Current Time =", current_time)

#spark = ps.sql.SparkSession.builder.appName("Python Spark SQL basic example").\
#                                    config("spark.some.config.option", "some-value").getOrCreate()

#importiamo il dataset sulle azioni
#df_azioni=spark.read.csv('/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stock_prices.csv',
#                         inferSchema="true", header="true")
azioni = sc.textFile("/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/file_grandi/historical_stock_prices_update_02.csv")
#importiamo il dataset sui settori
settori = sc.textFile("/home/giacomo/apache-hive-3.1.2-bin/data/BIG_DATA_PROGETTO-1/historical_stocks_update_per_hive.csv")
#df_settori=spark.read.csv('/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/historical_stocks.csv',
#                          inferSchema="true", header="true",sep=",")

#trasformiamo i dataframe df_azioni e df_settori in rdd
#azioni = df_azioni.rdd
#settori = df_settori.rdd

#stampe di test
#print("\nazioni:\n")
#azioni.foreach(lambda a: print (a))

#print("\nsettori:\n")
#settori.foreach(lambda a: print (a))
azioni = azioni.map(lambda a: a.split(','))
settori = settori.map(lambda s: s.split(';'))
"""
azioni_modificato è un RDD così costruito: 
    -Da azioni applichiamo un filter per rimuovere le azioni più vecchie del 2008
    -Applichiamo un map per creare una coppia (K,V), in particolare:
        -K è la coppia (ticker,anno) (rispettivamente a[0] e a[7])
        -V è un dizionario in cui mettiamo 
            -data_iniziale          (si intende la data minima nell'anno per il ticker)
            -prezzo_iniziale        (prezzo di chiusura associato alla data iniziale)
            -data_finale            (si intende la data massima nell'anno per il ticker)
            -prezzo_finale          (prezzo di chiusura associato alla data finale)
            -volume                 (somma dei volumi di tutte le azioni del ticker nell'anno)
            -var_giornaliera        (somma dei prezzi chiusura di tutte le zioni del ticker nell'anno)
            
            Il dizionario costruito in questo modo è particolarmente adatto all'operazione di riduzione(reduceByKey)
            che viene fatta in seguito sulla chiave in modo da ottenere poi le informazioni per ciascuna chiave 
            (ticker,anno)         
"""
azioni_modificato = azioni.filter(lambda a: a[7] >= '2008-01-01').\
                           map(lambda a:( (a[0] , a[7].split('-')[0]) ,
                                          {'data_iniziale':a[7],'prezzo_iniziale':float(a[2]),
                                           'data_finale':a[7],'prezzo_finale':float(a[2]),
                                           'volume':float(a[6]), 'var_giornaliera':float(a[2])} ))
"""
count_per_azione_anno serve per contare le occorrenze ci ciascuna azione raggruppate per chiave (ticker,anno) 
in modo tale che poi per conoscere la media del 'volume' e della 'var_giornaliera',
dividiamo per il numero di occorrenza la la somma dei volumi e dei prezzi di chiusura.
"""
count_per_azione_anno = azioni_modificato.groupByKey().map(lambda a: (a[0], len(list(a[1]))))
"""
riduzione(a1,a2) serve per il reduceByKey che si applica all'RDD azioni_modificato ed in particolare,
prende in input (V,V) dove V è un valore della coppia (K,V) definita sopra:
    -calcola il minimo tra due 'data_iniziale'
    -memorizza 'prezzo_iniziale' associato alla data minima
    -calcola il massimo tra due 'data_finale'
    -memorizza 'prezzo_finale' associato alla data massima 
    -somma il volume e la variazione giornaliera
ATTENZIONE questi valori sono calcolati a due a due, ovvero la funzione viene richiamata con due elementi per volta,
quindi applica correttamente la riduzione d'interesse SOLO SE richiamata all'intero di reduceByKey
RICORDANDO che per essere applicata questa funzione deve essere della forma (V,V) --> V
"""
def riduzione(a1,a2):
    a = {}
    if(a1.get('data_iniziale')<a2.get('data_iniziale')):
        a['data_iniziale'] = a1.get('data_iniziale')
        a['prezzo_iniziale']=a1.get('prezzo_iniziale')
    else:
        a['data_iniziale'] = a2.get('data_iniziale')
        a['prezzo_iniziale'] = a2.get('prezzo_iniziale')

    if(a1.get('data_finale')>a2.get('data_finale')):
        a['data_finale'] = a1.get('data_finale')
        a['prezzo_finale'] = a1.get('prezzo_finale')
    else:
        a['data_finale'] = a2.get('data_finale')
        a['prezzo_finale'] = a2.get('prezzo_finale')
    a['volume']=a1.get('volume')+a2.get('volume')
    a['var_giornaliera']=float(a1.get('var_giornaliera')+a2.get('var_giornaliera'))
    return a

azioni_modificato = azioni_modificato.reduceByKey(riduzione)

"""
qui facciamo il join tra i due RDD, azioni_modificato e count_per_azione_anno, per poter calcolare
il volume medio e la var_giornaliera media dato che sono necessari il numero di occorrenze per ciascuna chiave
"""
unione = azioni_modificato.join(count_per_azione_anno)

#stampa di test
#print("\nstampa unione:\n")
#unione.foreach(lambda a: print(a))

"""
qui si definisce un primo risultato ovvero per ciascun ticker per ciascun anno:
    -variazione annuale 
    -somma volume               (somma dei volumi assocati al ticker nell'anno)
    -somma prezzi di chiusura   (somma dei prezzi di chiusura associati al ticker nell'anno)
"""
def rid_per_aa(a):
    a_finale=(a[0],{'var_annuale':(100*(a[1][0].get('prezzo_finale')-a[1][0].get('prezzo_iniziale'))/a[1][0].get('prezzo_iniziale')),
                     'somma_volume':a[1][0].get('volume'),
                    'somma_prezzo_close':a[1][0].get('var_giornaliera'),
                    'count':a[1][1]})
    return a_finale

result = unione.map(rid_per_aa)
"""
result sarà fatto in questo modo: ( ('AHH',2010),{
                                        'var_annuale': 0.34,
                                        'somma_volume': 1190,
                                        'somma_prezzo_close': 265.32,
                                        'count': 30
                                        })
"""
#stampa di test
#print("\nresult:")
#result.foreach(lambda a: print(a))
"""
Adesso per includere le aziende e quindi i settori operiamo un map sull'RDD result così da avere come chiave
solo il ticker per poter fare il join con l'RDD settori_modificato. 
Successivamente  eseguiremo un altro map che avrà come chiave (anno,settore),
così da avere i risultati per ciascun settore
"""
result = result.map(lambda a:(a[0][0],[ a[0][1],a[1] ]))

"""
questa operazione è necessaria per non avere duplicati che poi fanno sballare i conti.
ES: potrebbe essere che ci siano ticker che compaiono più volte con lo stesso settore.
Applicando una riduzione sulla chiave (ticker,settore) eliminiamo attraverso res_set_mod(v1,v2) i duplicati
"""
def red_set_mod(v1,v2):
    return v1
settori_modificato = settori.map(lambda s:((s[0],s[3]),s[3])).reduceByKey(red_set_mod)

#stampa di test
#print("\nsettori_modificato:")
#settori_modificato.foreach(lambda a: print(a))
"""
dopo aver eliminato i duplicati, impostiamo come chiave solo il ticker 
in modo da poter fare il join con result (nel join gli RDD devono avere stessa chiave)
"""
settori_modificato = settori_modificato.map(lambda s:(s[0][0],s[1]))

#stampa di test
#print("\nsettori_modificato:")
#settori_modificato.foreach(lambda a: print(a))

result_per_settore = result.join(settori_modificato)

#stampa di test
#print("\nresult_per_settore:")
#result_per_settore.foreach(lambda a: print(a))
"""
In questo map definiamo la nuova chiave che sarà (settore,anno) e come valore avremo
    -var_annuale
    -somma_volume
    -somma_prezzo_close
    -count
"""
result_per_settore = result_per_settore.map(lambda az_set: ( (az_set[1][1],az_set[1][0][0]),
                                                              az_set[1][0][1]))
#stampa di test,
#print("\nresult per settore: \n")
#result_per_settore.foreach(lambda a: print(a))
"""
questo count ci servirà per eseguire il conteggio di tutte le tuple che hanno la stessa chiave,
per poi calcolare: variazione_annuale media per ogni anno
"""
count_per_settore = result_per_settore.groupByKey().map(lambda a: (a[0],len(list(a[1]))))

def red_per_settore(d1,d2):
    d = {
    'var_annuale':d1['var_annuale']+d2['var_annuale'],
    'somma_volume': d1['somma_volume']+d2['somma_volume'],
    'somma_prezzo_close':d1['somma_prezzo_close'] + d2['somma_prezzo_close'],
    'count': d1['count']+d2['count']
    }
    return d
result_per_settore  = result_per_settore.reduceByKey(red_per_settore)

result_per_settore =result_per_settore.join(count_per_settore)

#stampa di test
#print("\nresult per settore:")
#result_per_settore.foreach(lambda a: print(a))
"""
#In quest'ultima operazione viene calcolato il risultato finale, ovvero per ciascun settore:
    -variazione annuale media
    -volume medio
    -variazione giornaliera
"""
result_finale = result_per_settore.map(lambda a:(a[0],{'var_annuale_media':a[1][0]['var_annuale']/a[1][1],
                                                  'volume_medio':a[1][0]['somma_volume']/a[1][0]['count'],
                                                  'var_giornaliera':a[1][0]['somma_prezzo_close']/a[1][0]['count']
                                                  }))
#print("\nresult finale:\n")
#result_finale.foreach(lambda a: print(a))
result_finale.saveAsTextFile('results')
sc.stop()

#now = datetime.now()
#current_time = now.strftime("%H:%M:%S")
#print("Current Time =", current_time)
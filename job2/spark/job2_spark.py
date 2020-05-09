#from pyspark.sql import SparkSession
import pyspark as ps

spark = ps.sql.SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

#importiamo il dataset sulle azioni
df_azioni=spark.read.csv('/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/azioni_test.csv',inferSchema="true", header="true")
#importiamo il dataset sui settori
df_settori=spark.read.csv('/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/settori_test.csv', sep=";")

#trasformiamo i dataframe df_azioni e df_settori in rdd
azioni = df_azioni.rdd
settori = df_settori.rdd

#stampe di test
print("\nazioni:\n")
azioni.foreach(lambda a: print (a))

print("\nsettori:\n")
settori.foreach(lambda a: print (a))

"""
azioni_modificato è un RDD così costruito: 
    -Da azioni applichiamo un filter per rimuove le azioni più vecchie del 2018
    -Applichiamo un map per creare una coppia chiave valore, in particolare:
        -K è la coppia simbolo azione(a[0] e anno a[7])(preso dalla data lasciando solo l'anno)
        -V è un dizionario in cui mettiamo 
                -data_iniziale
                -prezzo_iniziale 
                -data_finale
                -prezzo_finale
                -volume 
                -var_giornaliera
            Il dizionario costruito in questo modo è particolarmente adatto all'operazione di raggruppamento
            che viene fatta in seguito sulla chiave in modo da ottenere poi per ciascun simobolo azione e anno 
            la data_iniziale (data minima nell'anno), la data_finale (data massima nell'anno) i prezzi di chiusura
            rispettivamente corrispondenti alla data iniziale e finale la somma del volume per quel simbolo azione
            in quell'anno (utile per poi fare la media) e la somma di tutte le variazioni giornaliere in 
            quell'anno (utile per poi fare la media)        
"""
azioni_modificato = azioni.filter(lambda a: a[7] >= '2008-01-01').\
    map(lambda a:((a[0] , a[7].split('-')[0]) ,
                  {'data_iniziale':a[7],'prezzo_iniziale':a[2],'data_finale':a[7],'prezzo_finale':a[2],
                   'volume':float(a[6]), 'var_giornaliera':a[2]-a[1]} ))

#questo rdd serve per contare le occorrenze ci ciascua azione raggruppate per chiave
count_per_chiave = azioni_modificato.groupByKey().map(lambda a: (a[0], len(list(a[1]))))

"""
questa funzione serve per il reduceByKey che si applica all'RDD azioni_modificato ed in particolare
calcola il minimo tra due date somma il volume e la variazione giornaliera, al fine di ottenere poi 
i risultati corretti per ciascuna chiave. ATTENZIONE questi valori sono calcolati a due a due, ovvero
la funzione viene richiamata con due elementi per volta, quindi applica correttamente la riduzione d'interesse
solo se richiamata all'intero di reduceByKey
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
    a['var_giornaliera']=a1.get('var_giornaliera')+a1.get('var_giornaliera')
    return a
azioni_modificato = azioni_modificato.reduceByKey(riduzione)
"""
qui facciamo il join tra i due RDD per poter calcolare il volume medio e la var_giornaliera media
dato che sono necessari il numero di occorrenze per ciascuna chiave
"""
unione = azioni_modificato.join(count_per_chiave)

#stampa di test
"""
print("\nstampa unione:\n")
unione.foreach(lambda a: print(a))
"""
"""
qui si definisce un primo risultato ovvero per ciascun simobolo_azione in ciascun anno:
    -variazione annuale 
    -volume medio
    -variazione media giornaliera
"""
def map_finale(a):
    a_finale=(a[0], [( 100*(a[1][0].get('prezzo_finale')-a[1][0].get('prezzo_iniziale'))/a[1][0].get('prezzo_iniziale')),
                     a[1][0].get('volume')/a[1][1],a[1][0].get('var_giornaliera')/a[1][1]])
    return a_finale
result = unione.map(map_finale)

print("\nresult finale:\n")
result.foreach(lambda a: print(a))

result.saveAsTextFile("results")
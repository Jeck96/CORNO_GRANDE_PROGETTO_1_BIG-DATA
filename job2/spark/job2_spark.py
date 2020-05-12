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
    -Da azioni applichiamo un filter per rimuove le azioni più vecchie del 2008
    -Applichiamo un map per creare una coppia chiave valore, in particolare:
        -K è la coppia simbolo azione e anno(rispettivamente a[0] e a[7])( anno,preso dalla data lasciando solo l'anno)
        -V è un dizionario in cui mettiamo 
                -data_iniziale
                -prezzo_iniziale (prezzo di chiusura)
                -data_finale
                -prezzo_finale (prezzo di chiusura)
                -volume             (si intende la somma dei volumi per chiave (simbolo_azione,anno)
                -var_giornaliera    (si intende la somma dei prezzi chiusura (simbolo_azione,anno)
            
            Il dizionario costruito in questo modo è particolarmente adatto all'operazione di riduzione(reduceByKey)
            che viene fatta in seguito sulla chiave in modo da ottenere poi per ciascuna chiave (simobolo_azione,anno) 
            la data_iniziale (data minima nell'anno), la data_finale (data massima nell'anno) i prezzi di chiusura
            rispettivamente corrispondenti alla data iniziale e finale, la somma del volume per quel simbolo azione
            in quell'anno (utile per poi fare la media) e la somma di tutte le variazioni giornaliere in 
            quell'anno (utile per poi fare la media)        
"""
azioni_modificato = azioni.filter(lambda a: a[7] >= '2008-01-01').\
    map(lambda a:((a[0] , a[7].split('-')[0]) ,
                  {'data_iniziale':a[7],'prezzo_iniziale':a[2],'data_finale':a[7],'prezzo_finale':a[2],
                   'volume':float(a[6]), 'var_giornaliera':a[2]} ))

"""
questo rdd serve per contare le occorrenze ci ciascuna azione raggruppate per chiave (simbolo_azione, anno) in modo tale
che poi per conoscere la media del 'volume' e della 'var_giornaliera', dividiamo per il numero di occorrenza la loro somma.
"""
count_per_azione_anno = azioni_modificato.groupByKey().map(lambda a: (a[0], len(list(a[1]))))

"""
questa funzione serve per il reduceByKey che si applica all'RDD azioni_modificato ed in particolare,
prende in input (V,V) dove V è un valore della coppia (k,v) definita sopra:
    -calcola il minimo tra due 'data_iniziale'
    -memorizza 'prezzo_iniziale' associato alla data minima
    -calcola il massimo tra due 'data_finale'
    -memorizza 'prezzo_finale' associato alla data massima 
    -somma il volume e la variazione giornaliera
ATTENZIONE questi valori sono calcolati a due a due, ovvero la funzione viene richiamata con due elementi per volta,
quindi applica correttamente la riduzione d'interessesolo se richiamata all'intero di reduceByKey
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
    a['var_giornaliera']=a1.get('var_giornaliera')+a1.get('var_giornaliera')
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
qui si definisce un primo risultato ovvero per ciascun simobolo_azione in ciascun anno:
    -variazione annuale 
    -volume medio
    -variazione media giornaliera
"""
def rid_per_aa(a):
    a_finale=(a[0], [( 100*(a[1][0].get('prezzo_finale')-a[1][0].get('prezzo_iniziale'))/a[1][0].get('prezzo_iniziale')),
                     a[1][0].get('volume')/a[1][1],a[1][0].get('var_giornaliera')/a[1][1]])
    return a_finale

"""
result sarà fatto in questo modo: ((ticker,anno),[variazione_annuale,volume_medio,var_giornaliera_media]
"""
result = unione.map(rid_per_aa)

"""
Adesso per includere le aziende e quindi i settori operiamo un map sull'RDD result così da avere come chiave
solo il ticker per poter fare il join con l'RDD settori_modificato. Successivamente  eseguiremo un altro map che avrà come
chiave anno e settore, così da avere i risultati per ciascun settore
"""
result = result.map(lambda a:(a[0][0],[a[0][1],a[1][0],a[1][1],a[1][2]]))

settori_modificato = settori.map(lambda s:(s[0],s[3]))

result_per_settore = result.join(settori_modificato)
"""
In questo map definiamo la nuova chiave che sarà (simbolo_azione,settore,anno) e come valore avremo
    -variazione_annuale, volume medio e variazione media giornaliera( quest ultime calcolate per simbolo_azione e anno)
"""
result_per_settore = result_per_settore.map(lambda az_set: ( (az_set[0],az_set[1][1],az_set[1][0][0]),
                                                        [ az_set[1][0][1], az_set[1][0][2], az_set[1][0][3]]))
#stampa di test
print("\nresult per settore:\n")
result_per_settore.foreach(lambda a: print(a))

"""
questo count ci servirà per eseguire il conteggio di tutte le tuple che hanno la stessa chiave per poi calcolare
variazione_annuale medie  volume medio e variazione_giornaliera media per ciascun settore per ogni anno
"""
count_per_settore = result_per_settore.groupByKey().map(lambda a: (a[0],len(list(a[1]))))


result_per_settore  = result_per_settore.reduceByKey(lambda v1,v2: [v1[0]+v2[0],v1[1]+v2[1],v1[2]+v2[2]])

result_finale = result_per_settore.join(count_per_settore)

"""
In quest'ultima operazione viene calcolato il risultato finale andando a dividere le somme eseguite nell'operazione
di cui sopra, per il numero di occorrenze di ogni chiave e quindi, per avere una media 
"""
result_finale = result_finale.map(lambda a:(a[0],[
                                                a[1][0][0]/a[1][1],
                                                a[1][0][1]/a[1][1],
                                                a[1][0][2]/a[1][1]
                                                ]))
print("\nresult finale:\n")
result_finale.foreach(lambda a: print(a))
#result_finale.saveAsTextFile('results')
import sys
import pandas as pd

settori = pd.read_csv("/home/giacomo/hadoop-3.2.1/DATI_AGGIUNTIVI/BIG_DATA_PROGETTO-1/settori_test_1.csv", sep=';')
settori = settori.values
#for s in settori:
 #   print(s)
#for s in settori:
 #   print(f'\nticker: {s[0]},settore: {s[3]}\n')
"""
simboli_per_settore sarà un dizionario che conterrà come chiavi i settori e come elementi array di stringhe che 
rappresentano i ticker associati al settore
"""
ticker_per_settore = {}
for settore in settori:
    if ticker_per_settore.get(settore[3]):
        ticker_per_settore[settore[3]].append(settore[0])
    else:
        ticker_per_settore[settore[3]]=[settore[0]]

arr = []
k = ('AHH',2010)
print(k)
arr.append({k:{'ticker':['AHH', 'PHIPP', 'SRCE'],'altro':[]}})
k = ('PIHPP',2013)
print(k)
arr.append({k:{'ticker':['AHH']},'altro':[]})
print (arr)
for aa in arr:
    k1 = list(aa.keys())[0]
    print(k1[0])
    for k in ticker_per_settore:
        print (k1[0])
        if k1[0] in ticker_per_settore.get(k,[]):
            print(f'\ntickers:{ticker_per_settore.get(k,[])}\n')
            print(f'{k1[0]} è presente')
        else:
            print(f'\ntickers:{ticker_per_settore.get(k, [])}\n')
            print(f'{k1[0]} è assente')
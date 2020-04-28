#!/usr/bin/python3
"""mapper.py"""

import sys
import json

def toJson(azione):
    dic = {   "ticker" : azione[0],
     "open" : azione[1], 
     "close" : azione[2],
     "low" : azione[4],
     "high" : azione[5],
     "volume" : azione[6],
    "date" : azione[7],
    }
    return json.dumps(dic)


for line in sys.stdin:
    azione = line.split(',')
    #l'ultimo campo di azione è la data e contiene anche \n di fine riga
    #RICORDARSI DI ELIMINARLO
    anno_azione = azione[7].split('-')[0]
    if int(anno_azione)>=2008:
        #print(azione[0],azione)
        print('%s\t%s' % (azione[0], toJson(azione)))


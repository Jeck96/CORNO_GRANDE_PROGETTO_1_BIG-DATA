#!/usr/bin/python3
"""mapper.py"""

import sys

for line in sys.stdin:
    azione = line.split(',')
    #l'ultimo campo di azione è la data e contiene anche \n di fine riga
    #RICORDARSI DI ELIMINARLO
    anno_azione = azione[7].split('-')[0]
    if int(anno_azione)>=2008:
        #print(azione[0],azione)
        print('%s\t%s' % (azione[0], azione))


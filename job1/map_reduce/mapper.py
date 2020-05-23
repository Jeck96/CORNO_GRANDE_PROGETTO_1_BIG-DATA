#!/usr/bin/python3
"""mapper.py"""

import sys
import json

def toJson(azione):
    dic = {   "ticker" : azione[0],
     "close" : azione[2],
     "volume" : azione[6],
      "date" : azione[7],
    }
    return json.dumps(dic)

for line in sys.stdin:
    azione = line.split(',')
    anno_azione = azione[7].split('-')[0]
    if int(anno_azione)>=2008:
        print('%s,%s' % (azione[0], toJson(azione)))


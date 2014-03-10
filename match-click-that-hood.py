#!/usr/bin/python

import sys
import json
import os
from collections import defaultdict

def isNeighborhoodFile(data):
  return data['annotation'] == ''

basedir = sys.argv[1]
for f in os.listdir(basedir):
  if f.endswith('metadata.json'):
    fullpath = os.path.join(basedir, f)
    data = defaultdict(str, json.loads(open(fullpath).read()))
    if data['dataTitle'] != 'Zillow' and isNeighborhoodFile(data):
      city = '%s %s' % (data['locationName'], data['stateName'])
      print './shape-gn-matchr.py --allowed_gn_classes=P --create=True --create_code=PPLX --create_parent="%(city)s" --create_output_file=hoods.tsv %(extra_args)s %(filename)s' % {
        'city': city,
        'extra_args': ' '.join(sys.argv[2:]),
        'filename': fullpath
      }


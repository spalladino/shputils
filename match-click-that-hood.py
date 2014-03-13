#!/usr/bin/python

import sys
import json
import os
from collections import defaultdict

def isNeighborhoodFile(data):
  return data['annotation'] == ''

basedir = sys.argv[1]
id_start = 1
for f in os.listdir(basedir):
  if f.endswith('metadata.json'):
    fullpath = os.path.join(basedir, f)
    data = defaultdict(str, json.loads(open(fullpath).read()))
    if data['dataTitle'] != 'Zillow' and isNeighborhoodFile(data):
      id_start += 200
      city = '%s %s' % (data['locationName'], data['stateName'])
      output_file = os.path.join(basedir, 'gn-' + f.replace('.metadata.json', '.geojson'))

      print 'rm "%s"' % (output_file)
      print './shape-gn-matchr.py --allowed_gn_classes=P,L,T --shp_name_keys=name --annotate --annotate_code=PPLX --annotate_parent="%(city)s" %(extra_args)s "%(filename)s" "%(output)s"' % {
        'city': city.encode('utf-8'),
        'extra_args': ' '.join(sys.argv[2:]).encode('utf-8'),
        'filename': fullpath.replace('.metadata.json'.encode('utf-8'), '.geojson'),
        'output': output_file.encode('utf-8')
      }


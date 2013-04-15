#!/usr/bin/python

import logging
import sys

from shapely.geometry import mapping, shape
from shapely.geometry import Point

from fiona import collection

import itertools

import psycopg2
import psycopg2.extras


# these should all be command-line opts, I am feeling lazy
conn = psycopg2.connect("dbname='geonames' user='blackmad' host='localhost' password='xxx'")
shp_name_cols = ['qs_name', 'qs_name_al']
allowed_gn_classes = ['P']
allowed_gn_codes = []
inputFile = sys.argv[1] 
outputFile = sys.argv[2] 
# set to 0 or None to take all
maxFeaturesToProcess = 0

def take(n, iterable):
  return itertools.islice(iterable, n)

def levenshtein(a,b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a,b = b,a
        n,m = m,n
        
    current = range(n+1)
    for i in range(1,m+1):
        previous, current = current, [i]+[0]*n
        for j in range(1,n+1):
            add, delete = previous[j]+1, current[j-1]+1
            change = previous[j-1]
            if a[j-1] != b[i-1]:
                change = change + 1
            current[j] = min(add, delete, change)
    return current[n]


logging.basicConfig(stream=sys.stderr, level=logging.INFO)

logger = logging.getLogger('gn_matcher')

formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('debug.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

failureh = logging.FileHandler('failure.log')
failureh.setLevel(logging.DEBUG)

matchLogger = logging.getLogger('match')
ambiguousLogger = logging.getLogger('ambiguous')
failureLogger = logging.getLogger('failure')
failureLogger.addHandler(failureh)
loggers = [matchLogger, ambiguousLogger, failureLogger]
for l in loggers:
  l.addHandler(fh)

def does_feature_match(f, gn_candidate):
  point = Point(gn_candidate['latitude'], gn_candidate['longitude'])
  candidate_names = filter(None, [gn_candidate['name'], gn_candidate['asciiname']] + (gn_candidate['alternatenames'] or '').split(','))
  feature_names = filter(None, [f['properties'][col] for col in shp_name_cols])
  # for each input name in the shape, see if we have a match in a geoname feature
  for f_name in feature_names:
    for gn_name in candidate_names:
      gn_name = gn_name.decode('utf-8')
      distance = levenshtein(f_name, gn_name)
      logger.debug(u'%s vs %s -- distance %d' % (f_name, gn_name, distance))
      if distance == 0 or ((distance*1.0) / len(f_name) < 0.14):
        if (get_geoname_fclass(gn_candidate) in allowed_gn_classes) or (get_geoname_fcode(gn_candidate) in allowed_gn_codes):
          matchLogger.debug(u'%s vs %s -- distance %d -- GOOD ENOUGH' % (f_name, gn_name, distance))
          return True
        else:
          matchLogger.debug(u'%s vs %s -- distance %d -- good enough, BUT class/code did not match' % (f_name, geoname_debug_str(gn_candidate), distance))

  return False

def get_feature_name(f):
  feature_names = filter(None, [f['properties'][col] for col in shp_name_cols])
  return feature_names[0]

def get_geoname_name(gn):
  return gn['name'].decode('utf-8')

def get_geoname_id(gn):
  return str(gn['geonameid'])

def get_geoname_fclass(gn):
  return gn['fclass'].decode('utf-8')

def get_geoname_fcode(gn):
  return gn['fcode'].decode('utf-8')

def geoname_debug_str(gn):
  return u"%s %s %s %s" % (get_geoname_name(gn), get_geoname_id(gn), gn['fclass'].decode('utf-8'), gn['fcode'].decode('utf-8'))

def get_feature_debug(f):
  return u"%s (%s)" % (get_feature_name(f), shape(f['geometry']).centroid)

def main():
  input = collection(inputFile, "r")

  newSchema = input.schema.copy()
  newSchema['geonameid'] = 'str:1000'
  output = collection(
    outputFile, 'w', 'ESRI Shapefile', newSchema, crs=input.crs, encoding='utf-8')

  inputIter = input
  if maxFeaturesToProcess:
    inputIter = take(maxFeaturesToProcess, input)
  for f in inputIter:
    # Make a shapely object from the dict.
    geom = shape(f['geometry'])
    if not geom.is_valid:
      # Use the 0-buffer polygon cleaning trick
      clean = geom.buffer(0.0)
      geom = clean
    if geom.is_valid:
      cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
      cur.execute("""select * FROM geoname WHERE the_geom && ST_SetSRID(ST_MakeBox2D(ST_MakePoint(%s, %s), ST_MakePoint(%s, %s)), 4326)""", geom.bounds)
      matches = []
      for gn_candidate in cur.fetchall():
        if does_feature_match(f, gn_candidate):
          matches.append(gn_candidate)
      if len(matches) == 0:
        f['geonameid'] = None
        failureLogger.info(u'found 0 match for %s' % (get_feature_debug(f)))
      elif len(matches) == 1:
        m = matches[0]
        matchLogger.info(u'found 1 match for %s:' % (get_feature_debug(f)))
        matchLogger.info('\t' + geoname_debug_str(m))
        f['geonameid'] = ','.join([get_geoname_id(m) for m in matches])
      elif len(matches) > 1:
        ambiguousLogger.info(u'found multiple matches for %s:' % (get_feature_debug(f)))
        for m in matches:
          ambiguousLogger.info('\t' + geoname_debug_str(m))
        f['geonameid'] = ','.join([get_geoname_id(m) for m in matches])

main()

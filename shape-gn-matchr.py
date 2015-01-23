#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import logging
import sys
import json

from shapely.geometry import mapping, shape
from shapely.geometry import Point, Polygon

import fiona

import itertools
import traceback

import psycopg2
import psycopg2.extras

import re
import unicodedata

from collections import defaultdict
from optparse import OptionParser

import urllib, urllib2
import json

from shapely import speedups
if speedups.available:
  speedups.enable()

from unicodecsv import UnicodeWriter

parser = OptionParser(usage="""%prog [options]""")

parser.add_option('--allowed_gn_classes', dest='allowed_gn_classes', default='P', help='comma separated list of allowed geonames feature classes')
parser.add_option('--allowed_gn_codes', dest='allowed_gn_codes', default='', help='comma separated list of allowed geonames feature codes')
parser.add_option('--fallback_allowed_gn_classes', dest='fallback_allowed_gn_classes', default='', help='comma separated list of fallback_allowed geonames feature classes')
parser.add_option('--fallback_allowed_gn_codes', dest='fallback_allowed_gn_codes', default='ADM4', help='comma separated list of fallback_allowed geonames feature codes')

parser.add_option('--shp_name_keys', dest='name_keys', default='qs_loc,qs_loc_alt', help='comma separated list of keys in shapefile to use for feature name')
parser.add_option('--shp_cc_key', dest='cc_key', default=None, help='shapefile column to find countrycode')
parser.add_option('--radius', dest='radius', default=None, type='float', help='optional radius in meters to blow out point features')

parser.add_option('--dbname', dest='dbname', default='geonames', help='postgres dbname')
parser.add_option('--dbuser', dest='dbuser', default='postgres', help='postgres user')
parser.add_option('--dbpass', dest='dbpass', default='xxx', help='postgres password')
parser.add_option('--dbhost', dest='dbhost', default='localhost', help='postgres host')
parser.add_option('--dbtable', dest='dbtable', default='geoname', help='postgres table')
parser.add_option('--db_geom_col', dest='db_geom_col', default='the_geom', help='postgres column with geo index')
parser.add_option('--gn_output_col', dest='geonameid_output_column', default='geonameid', help='column to output geonameid match to')
parser.add_option('--skip_existing_matches', action="store_true", dest='skip_existing_matches', default=True, help='skip features that already have a value in gn_output_col')
parser.add_option('--noskip_existing_matches', action="store_false", dest='skip_existing_matches', default=True, help='skip features that already have a value in gn_output_col')


(options, args) = parser.parse_args()

# these should all be command-line opts, I am feeling lazy
conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (
  options.dbname, options.dbuser, options.dbhost, options.dbpass))
shp_name_cols = options.name_keys.split(',')
shp_cc_col = options.cc_key

allowed_gn_classes = options.allowed_gn_classes.split(',')
allowed_gn_codes = options.allowed_gn_codes.split(',')
fallback_allowed_gn_classes = options.fallback_allowed_gn_classes.split(',')
fallback_allowed_gn_codes = options.fallback_allowed_gn_codes.split(',')

buffer_expand = 0.1

inputFile = args[0]
outputFile = args[1]
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


format = '%(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, filename='debug.log', format=format)

logger = logging.getLogger('gn_matcher')

matchLogger = logging.getLogger('match')
ambiguousLogger = logging.getLogger('ambiguous')
failureLogger = logging.getLogger('failure')

def remove_diacritics(char):
    '''
    Return the base character of char, by "removing" any
    diacritics like accents or curls and strokes and the like.
    '''
    desc = unicodedata.name(unicode(char))
    cutoff = desc.find(' WITH ')
    if cutoff != -1:
        desc = desc[:cutoff]
    return unicodedata.lookup(desc)

def remove_accents(input_str):
    try:
      if type(input_str) == str:
        input_str = input_str.decode('utf-8')
    except:
      print 'failed to convert to unicode'
      print input_str
      print traceback.print_exc()

    nkfd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])

def hacks(n):
  # fix "Vaivanos,Los"
  n = re.sub(r'(.*)(\w),(\w+)', r'\3 \1\2', n)
  n = n.lower()
  # fix german name weirndesses
  n = n.replace(u'(rhld.)', u'')
  n = n.replace(u'([A-Z])', u'')
  n = n.replace(u', stadt', u'')
  n = n.replace(u'am main', u'')
  n = n.replace(u'st.', u'saint')
  n = n.replace(u'ste.', u'sainte')
  n = n.replace(u'ß', u'ss')
  n = n.replace(u'es ', u'')
  n = n.replace(u' County', u'')
  return n

def geonames_annotate_from_db(f, m):
  centroid = shape(f['geometry']).centroid
  feature_dict = {
    'lat': get_geoname_lat(m) or centroid.y,
    'lng': get_geoname_lng(m) or centroid.x,
    'fclass': get_geoname_fclass(m),
    'fcode': get_geoname_fcode(m),
    'countryCode': get_geoname_cc(m),
    'adminCode1': get_geoname_admin(m,1),
    'adminCode2': get_geoname_admin(m,2),
    'adminCode3': get_geoname_admin(m,3),
    'adminCode4': get_geoname_admin(m,4)
  }

  f['properties'] = dict(f['properties'].items() + feature_dict.items())

def get_feature_names(f):
  return filter(None, [f['properties'][col] for col in shp_name_cols])

def get_feature_names_for_matching(f):
  feature_names = get_feature_names(f)
  feature_names = [re.sub(' \(.*\)', '', n) for n in feature_names]
  feature_names = [remove_accents(n).lower() for n in feature_names]

  # hack specific to some input data that looks like "Navoculas,Los"
  feature_names = [hacks(n) for n in feature_names]

  return feature_names

def get_geoname_names_for_matching(gn_candidate):
  feature_names = filter(None, [gn_candidate['name'], gn_candidate['asciiname']] + (gn_candidate['alternatenames'] or '').split(','))
  feature_names = [n.decode('utf-8') for n in feature_names]
  if gn_candidate['country'] == 'DE':
    feature_names = [re.sub(' \(.*\)', '', n) for n in feature_names]

  # hack specific to some input data that looks like "Navoculas,Los"
  feature_names = [hacks(n) for n in feature_names]

  feature_names = [remove_accents(n).lower() for n in feature_names]
  return feature_names

def does_feature_match(f, gn_candidate):
  point = Point(gn_candidate['latitude'], gn_candidate['longitude'])
  candidate_names = get_geoname_names_for_matching(gn_candidate)
  feature_names = get_feature_names_for_matching(f)

  # for each input name in the shape, see if we have a match in a geoname feature
  for f_name in feature_names:
    for gn_name in candidate_names:
      distance = levenshtein(f_name, gn_name)
      logger.debug(u'%s vs %s -- distance %d' % (f_name, gn_name, distance))
      if distance <= 1 or ((distance*1.0) / len(f_name) < 0.20):
        matchLogger.debug(u'%s vs %s -- distance %d -- GOOD ENOUGH' % (f_name, gn_name, distance))
        return (True, distance)
      else:
        matchLogger.debug(u'%s vs %s -- distance %d -- NO' % (f_name, geoname_debug_str(gn_candidate), distance))

  return (False, -1)

def get_feature_name(f):
  feature_names = filter(None, [f['properties'][col] for col in shp_name_cols])
  if feature_names:
    return feature_names[0]
  else:
    return None

def get_geoname_name(gn):
  if not gn: return None
  return gn['name'].decode('utf-8')

def get_geoname_id(gn):
  if not gn: return None
  return str(gn['geonameid'])

def get_geoname_fclass(gn):
  if not gn: return None
  return (gn['fclass'] or '').decode('utf-8')

def get_geoname_fcode(gn):
  if not gn: return None
  return (gn['fcode'] or '').decode('utf-8')

def get_geoname_lat(gn):
  if not gn: return None
  return float(gn['latitude'])

def get_geoname_lng(gn):
  if not gn: return None
  return float(gn['longitude'])

def get_geoname_cc(gn):
  if not gn: return None
  return (gn['country'] or '').decode('utf-8')

def get_geoname_admin(gn, index):
  if not gn: return None
  return (gn['admin' + str(index)] or '').decode('utf-8')

def geoname_debug_str(gn):
  return u"%s %s %s %s" % (get_geoname_name(gn), get_geoname_id(gn), get_geoname_fclass(gn), get_geoname_fcode(gn))

def get_feature_cc(f):
  if shp_cc_col:
    return f['properties'][shp_cc_col]
  else:
    return 'XX'

def get_feature_debug(f):
  res =  u"%s %s " % (get_feature_name(f), get_feature_cc(f))
  try:
    res += (u"%s" % (shape(f['geometry']).bounds,))
  except:
    import traceback
    traceback.print_exc()
    res += (u" null geom? %s" % f['geometry'])
  return res


def bbox_polygon(bbox):
  """
  annotate Polygon that covers the given bbox.
  """
  if (len(bbox) == 4):
    return Polygon((
      (bbox[0], bbox[1]),
      (bbox[2], bbox[1]),
      (bbox[2], bbox[3]),
      (bbox[0], bbox[3]),
    ))
  else:
    None

def main():
  input = fiona.open(inputFile, "r")
  newSchema = input.schema.copy()
  newSchema['properties'] = dict(newSchema['properties'].items() + {
      options.geonameid_output_column: 'str:1000',
      'lat': 'float',
      'lng': 'float',
      'fclass': 'str:1',
      'fcode': 'str:4',
      'countryCode': 'str:2',
      'adminCode1': 'str:10',
      'adminCode2': 'str:10',
      'adminCode3': 'str:10',
      'adminCode4': 'str:10'
    }.items())

  outputFormat = 'ESRI Shapefile'
  if 'json' in outputFile:
    outputFormat = 'GeoJSON'
  output = fiona.open(
    outputFile, 'w', outputFormat, newSchema, crs=input.crs, encoding='utf-8')
  inputIter = input

  if maxFeaturesToProcess:
    inputIter = take(maxFeaturesToProcess, inputIter)
  if maxFeaturesToProcess:
    num_elems = maxFeaturesToProcess
  else:
    num_elems = len(inputIter)
  num_matched = 0
  num_failed = 0
  num_annotated = 0
  num_ambiguous = 0
  num_fallback = 0
  num_zero_candidates = 0
  num_prior_match = 0
  num_no_name = 0
  num_bad_geom = 0

  def print_status(num):
    sys.stderr.write('finished %d of %d (prior_matches %s success %s (fallback: %s), ambiguous: %s, annotated %s, failed %s (no-name %s, bad-geom: %s, zero-candidates: %s))\n' % (num, num_elems, num_prior_match, num_matched, num_fallback, num_ambiguous, num_annotated, num_failed, num_no_name, num_bad_geom, num_zero_candidates))

  seen = 0
  for i,f in enumerate(inputIter):
    seen = i + 1
    if (i % 1000) == 0:
      print_status(i)

    if (options.geonameid_output_column in f['properties']
        and f['properties'][options.geonameid_output_column]
        and options.skip_existing_matches):
      num_prior_match += 1
      pass
    elif not get_feature_name(f):
      num_no_name += 1
    else:
      # Make a shapely object from the dict.
      try:
        geom_type = f["geometry"]["type"]
        geom = shape(f['geometry'])
      except:
        print 'failed to parse geometry: %s' % f['geometry']
        num_bad_geom += 1
        geom = None

      if geom and not geom.is_valid:
        # Use the 0-buffer polygon cleaning trick
        clean = geom.buffer(0.0)
        geom = clean
      if (geom is None) or (f is None):
        print 'skipping %s due to missing_geom -- you could fix this with a radius if you wanted' % get_feature_debug(f)
      if f["geometry"]["type"] not in ('Polygon', 'MultiPolygon') and not options.radius:
        print 'skipping %s due to it not being a polygon -- you could fix this with a radius if you wanted' % get_feature_debug(f)
        num_bad_geom += 1
      elif geom.is_valid:
        if f["geometry"]["type"] == 'Point':
          geom = geom.buffer(0.00001 * options.radius)

        matches = defaultdict(list)
        failures = []

        poly_bounds = bbox_polygon(geom.bounds)

        if not poly_bounds:
          print "couldn't build poly bounds for %s %s" % (geom, geom.bounds)
          failureLogger.error(u'could not build bounds for  %s %s' % (get_feature_debug(f), geom.bounds))
          rows = []
        else:
          expanded_bounds = poly_bounds.buffer(buffer_expand)

          cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
          cur.execute("""select * FROM """ + options.dbtable + """ WHERE """ + options.db_geom_col + """ && ST_SetSRID(ST_MakeBox2D(ST_MakePoint(%s, %s), ST_MakePoint(%s, %s)), 4326) AND (fclass IN %s OR fcode IN %s)""",
            (
              expanded_bounds.bounds[0],
              expanded_bounds.bounds[1],
              expanded_bounds.bounds[2],
              expanded_bounds.bounds[3],
              tuple(allowed_gn_classes + fallback_allowed_gn_classes),
              tuple(allowed_gn_codes + fallback_allowed_gn_codes)
            )
          )

          matchLogger.debug('expanded bounds: %s' % expanded_bounds)

          rowcount = cur.rowcount
          if rowcount > 2000:
            print u'oversized result set: %s rows for %s' % (cur.rowcount, get_feature_debug(f))
            failureLogger.error(u'oversized result set: %s rows for %s' % (cur.rowcount, get_feature_debug(f)))
          if rowcount > 10000:
            print 'giving up on this'
            rows = []
          else:
            rows = cur.fetchall()

        if len(rows) == 0:
          failureLogger.error(u'found 0 candidates for %s %s' % (get_feature_debug(f), geom.bounds))
          num_zero_candidates += 1
        if len(rows) > 2000:
          print u'oversized result set: %s rows for %s' % (len(rows), get_feature_debug(f))
          failureLogger.error(u'oversized result set: %s rows for %s' % (len(rows), get_feature_debug(f)))
          if len(rows) > 10000:
            print "giving up on this one"
            failureLogger.error(u'giving up on: %s rows for %s' % (len(rows), get_feature_debug(f)))
            rows = []
        for gn_candidate in rows:
          (doesMatch, distance) = does_feature_match(f, gn_candidate)
          if doesMatch:
            matches[distance].append(gn_candidate)
          else:
            failures.append(gn_candidate)

        best_matches = []
        if len(matches):
          matchLogger.debug('had matches of distances: %s' % matches.keys())
          best_matches = matches[min(matches.keys())]

        final_matches = filter(lambda gn_candidate: (get_geoname_fclass(gn_candidate) in allowed_gn_classes) or (get_geoname_fcode(gn_candidate) in allowed_gn_codes), best_matches)
        fallback_matches = filter(lambda gn_candidate: (get_geoname_fclass(gn_candidate) in fallback_allowed_gn_classes) or (get_geoname_fcode(gn_candidate) in fallback_allowed_gn_codes), best_matches)
        if len(final_matches) == 0 and len(fallback_matches) > 0:
          matchLogger.debug('0 preferred type matches, falling back to fallback match types')
          final_matches = fallback_matches
          num_fallback += 1

        priorMatch = None
        if (options.geonameid_output_column in f['properties'] and f['properties'][options.geonameid_output_column]):
          priorMatch = str(f['properties'][options.geonameid_output_column]).replace('.0', '')

        if len(final_matches) == 0:
          f['properties'][options.geonameid_output_column] = None
          geonames_annotate_from_db(f, None)
          failureLogger.error(u'found 0 matches for %s' % (get_feature_debug(f)))
          for m in rows:
            failureLogger.error('\t' + geoname_debug_str(m))
          num_failed += 1

        elif len(final_matches) == 1:
          m = final_matches[0]
          matchLogger.debug(u'found 1 match for %s:' % (get_feature_debug(f)))
          matchLogger.debug('\t' + geoname_debug_str(m))
          f['properties'][options.geonameid_output_column] = get_geoname_id(m)
          geonames_annotate_from_db(f, m)
          num_matched += 1

        elif len(final_matches) > 1:
          ambiguousLogger.error(u'found multiple final_matches for %s:' % (get_feature_debug(f)))
          for m in final_matches:
            ambiguousLogger.error('\t' + geoname_debug_str(m))
          f['properties'][options.geonameid_output_column] = ','.join([get_geoname_id(m) for m in final_matches])
          geonames_annotate_from_db(f, None)
          num_ambiguous += 1

      try:
        output.write(f)
      except:
        import traceback
        traceback.print_exc()
        print 'soldiering on'
  print_status(seen)
  output.close()

main()

#!/usr/bin/python

import fiona
import urllib
import urllib2
import sys
from geocoder import Geocoder

import math

def distance(origin, destination):
  lat1, lon1 = origin
  lat2, lon2 = destination
  radius = 6371 # km

  dlat = math.radians(lat2-lat1)
  dlon = math.radians(lon2-lon1)
  a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
    * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
  c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
  d = radius * c

  return d

source = fiona.open(sys.argv[1])
output_schema = source.schema.copy()
output_schema['properties']['db_geonameid'] = 'str'
output_schema['properties']['db_lat'] = 'float'
output_schema['properties']['db_lng'] = 'float'
output_schema['properties']['db_distance'] = 'float'

output = fiona.open('fixed-' + sys.argv[1], 'w', 
    crs=source.crs,
    driver=source.driver,
    schema=output_schema)

geocoder = Geocoder('demo.twofishes.net')
for f in source:
  query = u"%s %s %s" % (
    f['properties']['NAME'],
    f['properties']['ADM1NAME'],
    f['properties']['ADM0NAME']
  )
  print query
  ll = (f['properties']['LATITUDE'], f['properties']['LONGITUDE'])
  llStr = '%s,%s' % ll

  hint = {
    'woeHint': 'TOWN',
    'll': llStr
  }

  try:
    geocode = geocoder.geocode(query, hint)

    if not geocode:
      query = u"%s %s" % (
        f['properties']['ADM1NAME'],
        f['properties']['ADM0NAME']
      )
      geocode = geocoder.geocode(query, hint)
  except:
    next

  f['properties']['db_geonameid'] = ''
  f['properties']['db_lat'] = ''
  f['properties']['db_lng'] = ''
  f['properties']['db_distance'] = 0.0
 
  if geocode and not geocode.what():
    current_geonameid = str(f['properties']['GEONAMEID']).split('.')[0]
    if geocode.geonameid() != current_geonameid:
      d = distance((geocode.lat(), geocode.lng()), ll)
      if d < 5:
        print 'differ within 5km: us %s vs ne %s' % (geocode.geonameid(), current_geonameid)
      else:
        print 'differ %s > 5km TOO FAR: us %s vs ne %s (us %s vs ne %s)' % (d, geocode.geonameid(), current_geonameid, (geocode.lat(), geocode.lng()), ll)
      f['properties']['db_geonameid'] = geocode.geonameid()
      f['properties']['db_lat'] = geocode.lat()
      f['properties']['db_lng'] = geocode.lng()
      f['properties']['db_distance'] = d
  else:
    print u'no geocode for %s' % query

  output.write(f)

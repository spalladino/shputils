#!/usr/bin/python

from osgeo import ogr
from shapely.wkb import loads
from collections import defaultdict
from shapely.geometry import mapping, shape
from shapely.ops import cascaded_union
from fiona import collection
import shapely.speedups
import json
import sys
from optparse import OptionParser
from merge_utils import *

parser = OptionParser()
parser.add_option('-a', '--all', dest='all', action="store_true", default=False,
                  help='dissolve all shapes into one')
parser.add_option('-i', '--input', dest='input',
                  help='shapefile to read', metavar='FILE')
parser.add_option('-o', '--output', dest='output',
                  help='shapefile to write', metavar='FILE')
parser.add_option('-f', '--fields', dest='fields', metavar='f1,f2,f3',
  help='comma separated list of field names in the shapefile to group by and write out')
parser.add_option('-c', '--collectors', dest='collectors', action="append", default=[],
  metavar='inputKey:op:outputKey',
  help='arbitrarily collect fields across group by. op is one of %s' % (','.join(groupByOperations.keys())))

(options, args) = parser.parse_args()

matchingFields = []

def checkArg(opt, message):
  if not opt:
    print "Missing %s" % message
    parser.print_usage()
    sys.exit(1)

checkArg(options.input, 'input')
checkArg(options.output, 'output')

# we build the key as the string representation of the json representation of the
# dict of keys that we grouped by (and intend to save) dictionaries aren't hashable,
# and this was an easy way to keep the full dict next to the geometries
def buildKeyFromFeature(feature):
  values = {}
  if options.all:
    return '{}'

  for field in matchingFields:
    value = feature.GetField(field)
    if not value:
      raise Exception('missing field %s on feature %s' % (field, feature))
    else:
      values[field] = value

  return json.dumps(values)

def processInput():
  global matchingFields
  geometryBuckets = defaultdict(list) 
  inputCRS = None

  with collection(options.input, 'r') as input:
    if not options.fields and not options.all:
      print "no matching fields specified, and --all not specified, please specify some with -f"
      sys.exit(1)
    if options.fields:
      matchingFields = [getActualProperty(input, f) for f in options.fields.split(',')]
    originalSchema = input.schema.copy()
    print "original schema"
    print '  %s' % originalSchema
    newSchema = filterFionaSchema(input, matchingFields)
    newSchema['geometry'] = 'MultiPolygon' 
    inputCRS = input.crs
    collectors = Collectors(input, options.collectors)
    collectors.addToFionaSchema(newSchema)
    print 'grouping by: %s' % matchingFields

  print "modified schema:"
  print '  %s' % newSchema

  ds = ogr.Open(options.input)
  layer = ds.GetLayer(0)
  print 'examining %s, with %d features' % (options.input, layer.GetFeatureCount())
  featuresSeen = 0

  def printFeature(f):
    featureDefinition = layer.GetLayerDefn()
    fieldIndices = xrange(featureDefinition.GetFieldCount())
    for fieldIndex in fieldIndices:
      fieldDefinition = featureDefinition.GetFieldDefn(fieldIndex)
      print "\t%s:%s = %s" % (
        fieldDefinition.GetName(), fieldDefinition.GetType(), f.GetField(fieldIndex))

  # using raw shapely here because fiona barfs on invalid geoms in the shapefile
  while True:
    f = layer.GetNextFeature()
    if f is None: break
    g = f.geometry()
    featuresSeen += 1
    if g is not None:
      if not loads(g.ExportToWkb()).is_valid:
        print 'SKIPPING invalid geometry for:'
        printFeature(f)
        print g
      else:
        groupKey = buildKeyFromFeature(f)
        collectors.recordMatch(groupKey, f)
        geometryBuckets[groupKey].append(loads(g.ExportToWkb()))

  print 'saw %d features, made %d dissolved features' % (featuresSeen, len(geometryBuckets))

  with collection(
      options.output, 'w', 'ESRI Shapefile', newSchema, crs=inputCRS) as output:
    for key, value in geometryBuckets.items():
      merged = cascaded_union(value)
      properties = json.loads(key)
      collectors.outputMatchesToDict(key, properties)
      output.write({
        'properties': properties,
        'geometry': mapping(merged)
      })

processInput()

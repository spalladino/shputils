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

parser = OptionParser(usage="""%prog [options]

DISSOLVE SHAPES

Takes an Esri Shapefile and dissolves (unions) geometries sharing the same values in 
user-specified fields. Mutlipart polygons are built from the matching features. Useful
for rebuilding exploided polygons or dissolving children into a parent shape.""")

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
parser.add_option('-s', '--skip_failures', dest='skip_failures', action="store_true", default=False,
                  help='skip union failures')

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
    value = feature['properties'][field]
    if not value:
      values[field] = None
    else:
      values[field] = value

  return json.dumps(values)

def processInput():
  global matchingFields
  geometryBuckets = defaultdict(list) 
  inputCRS = None

  ds = ogr.Open(options.input)
  layer = ds.GetLayer(0)
  featureDefinition = layer.GetLayerDefn()

  input = collection(options.input, 'r')
  if not options.fields and not options.all:
    print "no matching fields specified, and --all not specified, please specify some with -f"
    sys.exit(1)
  if options.fields:
    matchingFields = [getActualProperty(layer, f) for f in options.fields.split(',')]
  originalSchema = input.schema.copy()

  print "original schema"
  print '  %s' % originalSchema
  origFieldNames = originalSchema["properties"].keys()
  newSchema = filterSchemaDict(originalSchema, matchingFields)
  newSchema['geometry'] = 'MultiPolygon' 
  inputCRS = input.crs
  
  
  clist = list(options.collectors)
  for i, c in enumerate(clist):
    if c[0] == '*':
      # remove special keyword filter from list
      del clist[i]
      # push all fields into list
      # TODO throw error if more than 2 elements in : separated string (c)
      for field in origFieldNames:
        # formated like: origfield:operation:outfield_operation
        x = field + ":" + c[2:] + ":" + c[2:].upper() + "_" + field
        print x
        clist.append(x)
              
  print 'collectors: %s' % clist
  
  collectors = Collectors(input, clist)
  collectors.addToFionaSchema(newSchema)
  print 'grouping by: %s' % matchingFields

  print "modified schema:"
  print '  %s' % newSchema

  print 'examining %s, with %d features' % (options.input, len(input))
  featuresSeen = 0

  def printFeature(f):
    fieldIndices = xrange(featureDefinition.GetFieldCount())
    for fieldIndex in fieldIndices:
      fieldDefinition = featureDefinition.GetFieldDefn(fieldIndex)
      print "\t%s:%s = %s" % (
        fieldDefinition.GetName(), fieldDefinition.GetType(), f.GetField(fieldIndex))

  for f in input:
    if f is None: break
    g = f['geometry']
    featuresSeen += 1
    if g is not None:
      geom = shape(g).buffer(0)
      if not geom.is_valid:
        print 'SKIPPING invalid geometry for:'
        printFeature(f)
        print g
      else:
        groupKey = buildKeyFromFeature(f)
        collectors.recordMatch(groupKey, f)
        geometryBuckets[groupKey].append(geom)
 
  print 'saw %d features, made %d dissolved features' % (featuresSeen, len(geometryBuckets))

  with collection(
      options.output, 'w', 'ESRI Shapefile', newSchema, crs=inputCRS, encoding='utf-8') as output:
    output.encoding = 'utf-8'
    for key, value in geometryBuckets.items():
      try:
        merged = cascaded_union(value)
      except Exception as inst:
        try:
          print "coluldn't create a cascaded union for %s, trying a linear union" % key
          merged = value[0]
          for v in value[1:]:
            merged = merged.union(v)
        except Exception as inst2:
          import traceback
          if options.skip_failures:
            print inst2
            print "coluldn't create a merged union for %s" % key
            print "continuing on"
          else:
            print "coluldn't create a merged union for %s" % key
            raise inst2

      properties = json.loads(key)
      #TODO: multiple collectors
      #TODO: * collectors
      collectors.outputMatchesToDict(key, properties)
      output.write({
        'properties': properties,
        'geometry': mapping(merged)
      })

processInput()

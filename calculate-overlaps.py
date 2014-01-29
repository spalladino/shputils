#!/usr/bin/python

from rtree import index 
import sys
import fiona
from shapely import geometry 
import os
from shapely import speedups

speedups.enable()

ID_COLUMN = 'NID'

rtree_file = sys.argv[1] + '.rtree'
features = fiona.open(sys.argv[1])
num_features = len(features)
if not os.path.exists(rtree_file + '.idx'):
  idx = index.Rtree(rtree_file)
  for i, feature in enumerate(features):
    if i % 1000 == 0:
      sys.stderr.write('finished %d of %d indexing\n' % (i, num_features))
    shape = geometry.shape(feature['geometry'])
    idx.insert(i, shape.bounds, obj=feature)
else:
  idx = index.Rtree(rtree_file)

output = open('overlaps.txt', 'w')

def getIdFromFeature(feature):
  return feature['properties'][ID_COLUMN]

def add_relation(feature1, feature2, relationship, percentage = 1.0):
  id1 = getIdFromFeature(feature1)
  id2 = getIdFromFeature(feature2)
  output.write(u'%d %d %s %s\n' % (id1, id2, relationship, percentage))

for i, feature in enumerate(features):
  if i % 1000 == 0:
    print 'finished %d of %d intersect checks' % (i, num_features)
  shape = geometry.shape(feature['geometry'])
  intersections = [i for i in idx.intersection(shape.bounds, objects='raw')]
  #sys.stderr.write(u'found %d overlaps with %s\n' % (
  #  len(intersections), feature['properties']['NEIGHBORHD']))
  for ifeature in intersections:
    ishape = geometry.shape(ifeature['geometry'])
    if shape.contains(ishape):
      add_relation(feature, ifeature, 'CONTAINS')
    elif ishape.contains(shape):
      add_relation(feature, ifeature, 'CONTAINED_BY')
    elif ishape.intersects(shape):
      intersectionArea = ishape.intersection(shape).area
      overlapRatio = intersectionArea / shape.area
      if ishape.touches(ishape) or overlapRatio < 0.00001:
        intersectionLine = ishape.intersection(shape)
        add_relation(feature, ifeature, 'TOUCHES', intersectionLine.length / shape.boundary.length)
      else:
        add_relation(feature, ifeature, 'OVERLAPS', overlapRatio)

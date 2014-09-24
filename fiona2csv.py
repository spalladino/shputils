#!/usr/bin/python

from shapely.geometry import shape
import fiona
import sys
import csv
from shapely import affinity

from optparse import OptionParser
import csv, codecs, cStringIO

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


parser = OptionParser()
parser.add_option("--invert", dest="invert", action="store_true",
                  help="invert coordinates")
parser.add_option("-i", "--input", dest="input")
parser.add_option("-o", "--output", dest="output")
(options, args) = parser.parse_args()

print options.output
output = open(options.output, 'w')
writer = UnicodeWriter(output)

def fix_coordinates(coords):
  if options.invert:
    if type(coords[0][0][0]) == list:
      return [ [ [ (ring[1], ring[0]) for ring in rings ] for rings in shape ] for shape in coords ]
    else:
      return [ [ (ring[1], ring[0]) for ring in rings ] for rings in coords ]
  else:
    return coords

numseen = 0
for index, f in enumerate(fiona.open(options.input)):
  numseen += 1
  attrs = [ f['properties'][attr] for attr in sorted(f['properties']) ]
  geometry = f['geometry']
  geometry['coordinates'] = fix_coordinates(geometry['coordinates']) 
  
  geom = shape(geometry)
  writer.writerow(attrs + [str(geom),])

output.close()
print 'done with %s' % numseen

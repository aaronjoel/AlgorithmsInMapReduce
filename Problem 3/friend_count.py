import sys
import MapReduce

mr = MapReduce.MapReduce()

def mapper(relation):
    # key: document identifier
    # value: document contents
    key = relation[0]
    value = relation[1]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

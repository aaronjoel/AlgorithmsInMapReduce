import sys
import MapReduce

mr = MapReduce.MapReduce()

def mapper(relation):
    # key: relation
    # value: 1
    key = tuple(sorted((relation[0], relation[1])))
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    if total == 1:
        mr.emit((key[0], key[1]))
        mr.emit((key[1], key[0]))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

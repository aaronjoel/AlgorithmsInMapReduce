import sys
import MapReduce

mr = MapReduce.MapReduce()

def mapper(record):
    for k in range(5):
        if record[0] == 'a':
            key = (record[1], k)
            value = (record[0], record[2], record[3])
        elif record[0] == 'b':
            key = (k, record[2])
            value = (record[0], record[1], record[3])
        mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    a = {}
    b = {}
    for v in list_of_values:
        if v[0] == 'a':
            a[v[1]] = v[2]
        elif v[0] == 'b':
            b[v[1]] = v[2]
    value = 0
    for k in a:
        if k in b:
            value += a[k] * b[k]
    if value != 0:
        mr.emit((key[0], key[1], value))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

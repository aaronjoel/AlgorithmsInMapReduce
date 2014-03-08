import sys
import MapReduce

mr = MapReduce.MapReduce()

def mapper(record):
    # key: sequence id
    # value: nucleotides
    key = record[1][:-10]
    value = record[0]
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: trimmed nucleotide strings
    # value: list of sequence id
    mr.emit(key)

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

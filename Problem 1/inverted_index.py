import sys
import MapReduce

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, key)

def reducer(key, list_of_docid):
    # key: word
    # list_of_docid: list of occurrence counts
    docid_list = []
    for docid in list_of_docid:
        if docid not in docid_list:
            docid_list.append(docid)
    mr.emit((key, docid_list))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

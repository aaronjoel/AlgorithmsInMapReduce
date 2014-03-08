import sys
import MapReduce

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    identifier = record[1]
    mr.emit_intermediate(identifier, record)

def reducer(identifier, records):
    # identifier: JOIN to be performed on
    # records: list of records having same identifier
    list_records = []
    for record in records:
        if record[0] == "order":
            order_record = record
        else:
            list_records.append(record)
    for record in list_records:
        record = order_record + record
        mr.emit(record)

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)

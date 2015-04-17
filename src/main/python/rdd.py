import csv
import StringIO
import collections


def load_record(line, fieldCollection):
    """Parse a CSV line."""
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, fieldnames=fieldCollection)
    return reader.next()


def load_csv(sc, csv_file, fieldCollection):
    """Load a CSV as an RDD"""
    return sc.textFile(csv_file).map(lambda line:
                                     load_record(line, fieldCollection))


def writeRecords(fields, records):
    """Write out CSV lines"""
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]


def field_rdd(table_rdd, key, fields):
    if len(fields) == 1:
        return table_rdd.map(lambda entry:
                             (int(entry[key]), float(entry[fields[0]])))
    else:
        return table_rdd.map(lambda entry:
                             (int(entry[key]), [float(entry[field])
                                                for field in fields]))


def join_rdds(rdds):
    joined = rdds[0]
    for i in range(1, len(rdds)):
        joined = joined.join(rdds[i])

    return joined.map(lambda (key, rdd_fields): (key, flatten(rdd_fields)))


def flatten(l):
    """
    Flatten an irregular list of lists
    http://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists-in-python
    """
    for el in l:
        if ((isinstance(el, collections.Iterable) and not
             isinstance(el, basestring))):
            for sub in flatten(el):
                yield sub
        else:
            yield el

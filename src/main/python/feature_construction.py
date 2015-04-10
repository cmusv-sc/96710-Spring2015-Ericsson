import csv
import StringIO
import argparse
import collections
from pyspark import SparkConf, SparkContext

CSV_FIELDS = ('start time',
              'end time',
              'job ID',
              'task index',
              'machine ID',
              'CPU rate',
              'canonical memory usage',
              'assigned memory usage',
              'unmapped page cache',
              'total page cache',
              'maximum memory usage',
              'disk I/O time',
              'local disk space usage',
              'maximum CPU rate',
              'maximum disk IO time',
              'cycles per instruction',
              'memory accesses per instruction',
              'sample portion',
              'aggregation type',
              'sampled CPU usage')

PRIMARY_ID = 'job ID'
AVG_FIELDS = ('CPU rate',
              'maximum memory usage',
              'disk I/O time',
              'total page cache')


def load_record(line):
    """Parse a CSV line."""
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, fieldnames=CSV_FIELDS)
    return reader.next()


def writeRecords(fields, records):
    """Write out CSV lines"""
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]


def load_csv(sc, csv_file):
    """Load a CSV as an RDD"""
    return sc.textFile(csv_file).map(load_record)


def average_rdd(rdd, key):
    """Create an RDD with the avgerage of a given """
    taskmem = rdd.map(lambda entry: (int(entry[PRIMARY_ID]),
                                     float(entry[key])))
    sum_count = taskmem.combineByKey((lambda mem: (mem, 1)),
                                     (lambda a, b: (a[0] + b, a[1] + 1)),
                                     (lambda a, b: (a[0] + b[0], a[1] + b[1])))
    avg = sum_count.map(lambda (task_id, (sum_, count)):
                        (task_id, sum_ / count))
    return avg


def flatten(l):
    """
    Flatten an irregular list of lists
    http://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists-in-python
    """
    for el in l:
        if (isinstance(el, collections.Iterable)
                and not isinstance(el, basestring)):
            for sub in flatten(el):
                yield sub
        else:
            yield el


def parse_args():
    """Get arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='input file')
    parser.add_argument('output_file', help='output file')
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    # Configure and initialize Spark
    conf = (SparkConf().setAppName('Ericsson').
            # Allow spark to overwrite output files
            set("spark.hadoop.validateOutputSpecs", "false"))
    sc = SparkContext(conf=conf)

    # Get arguments
    args = parse_args()
    input_file = args.input_file
    output_file = args.output_file

    # Load the CSV into an (id, field_dictionary) RDD
    input_rdd = load_csv(sc, input_file)

    # Generate RDDs that average fields
    avg_rdds = [average_rdd(input_rdd, field) for field in AVG_FIELDS]

    # Join features together
    joined = avg_rdds[0]
    for i in range(1, len(avg_rdds)):
        joined = joined.join(avg_rdds[i])

    joined = joined.map(
        lambda (task_id, rdd_fields): (task_id, flatten(rdd_fields)))

    # Transform the RDD into a dictionary
    output_fields = [PRIMARY_ID] + ['avg ' + f for f in AVG_FIELDS]
    output_rdd = joined.map(
        lambda (task_id, rdd_fields):
            dict(zip(output_fields, [task_id] + list(rdd_fields))))

    # Write to CSV
    output_rdd.mapPartitions(lambda records: writeRecords(
        output_fields, records)).saveAsTextFile(output_file)

    print "Processed {0}'s: {1}".format(PRIMARY_ID, output_rdd.count())
    print output_rdd.first()

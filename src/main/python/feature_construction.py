import csv
import StringIO
import argparse
import collections
from pyspark import SparkConf, SparkContext

JOB_EVENTS_FIELDS = ('time',
                     'missing info',
                     'job ID',
                     'event type',
                     'user',
                     'scheduling class',
                     'job name',
                     'logical job name')

TASK_USAGE_FIELDS = ('start time',
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

JOB_EVENTS_RESULT_FIELDS = 'event type'

TASK_USAGE_RESULT_FIELDS = ('CPU rate',
                  'canonical memory usage',
                  'assigned memory usage',
                  'unmapped page cache',
                  'total page cache',
                  'maximum memory usage',
                  'disk I/O time',
                  'local disk space usage',
                  'maximum CPU rate',
                  'maximum disk IO time')


def load_record(line, fieldCollection):
    """Parse a CSV line."""
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, fieldnames=fieldCollection)
    return reader.next()


def writeRecords(fields, records):
    """Write out CSV lines"""
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]


def load_csv(sc, csv_file, fieldCollection):
    """Load a CSV as an RDD"""
    return sc.textFile(csv_file).map(lambda line: load_record(line, fieldCollection))


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

def min_rdd(rdd, key):
	"""Create an RDD with the minimum of a given """
	taskmem = rdd.map(lambda entry: (int(entry[PRIMARY_ID]), 
									 float(entry[key])))
	min_num = taskmem.combineByKey((lambda mem: mem), min, min)
	min_result = min_num.map(lambda (task_id, min_rec): (task_id, min_rec))
	return min_result

def max_rdd(rdd, key):
    """Create an RDD with the maximum of a given """
    taskmem = rdd.map(lambda entry: (int(entry[PRIMARY_ID]),
                                     float(entry[key])))
    max_num = taskmem.combineByKey((lambda mem: mem), max, max)
    max_result = max_num.map(lambda (task_id, max_rec): (task_id, max_rec))
    return max_result

def job_fails_helper(event_type):
    fails = 'NO_FAIL'
    if event_type == 3:
        fails = 'FAIL'
    return fails

def job_fails(rdd, key):
    """Create an RDD shows status of current job """
    job_event_mem = rdd.map(lambda entry: (int(entry[PRIMARY_ID]),
                                     job_fails_helper(int(entry[key]))))
    return job_event_mem

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

    # Load the job_events CSV into an (id, field_dictionary) RDD
    job_event_rdd = load_csv(sc, input_file + "/job_events/sampleTest.csv.gz", JOB_EVENTS_FIELDS)

    # Generate RDDs that average fields
    jobFail_rdds = [job_fails(job_event_rdd, JOB_EVENTS_RESULT_FIELDS)]

    # Load the task_usage CSV into an (id, field_dictionary) RDD
    task_usage_rdd = load_csv(sc, input_file + "/task_usage/sampleTest.csv.gz", TASK_USAGE_FIELDS)

    # Generate RDDs that average fields
    avg_rdds = [average_rdd(task_usage_rdd, field) for field in TASK_USAGE_RESULT_FIELDS]
	
    # Generate RDDs that maximum fields
    max_rdds = [max_rdd(task_usage_rdd, field) for field in TASK_USAGE_RESULT_FIELDS]

    # Generate RDDs that minimum fields
    min_rdds = [min_rdd(task_usage_rdd, field) for field in TASK_USAGE_RESULT_FIELDS]

    # Join features together
    joined = jobFail_rdds[0]
    for i in range(0, len(avg_rdds)):
        joined = joined.join(avg_rdds[i])
		
    for i in range(0, len(max_rdds)):
        joined = joined.join(max_rdds[i])

    for i in range(0, len(min_rdds)):
        joined = joined.join(min_rdds[i])

    joined = joined.map(
        lambda (task_id, rdd_fields): (task_id, flatten(rdd_fields)))

    # Transform the RDD into a dictionary
    output_fields = [PRIMARY_ID] + ['Job fails'] + ['avg ' + f for f in TASK_USAGE_RESULT_FIELDS] + ['max ' + f for f in TASK_USAGE_RESULT_FIELDS] \
                    + ['min ' + f for f in TASK_USAGE_RESULT_FIELDS]
    output_rdd = joined.map(
        lambda (task_id, rdd_fields):
            dict(zip(output_fields, [task_id] + list(rdd_fields))))

    # Write to CSV
    output_rdd.mapPartitions(lambda records: writeRecords(
        output_fields, records)).saveAsTextFile(output_file)

    print "Processed {0}'s: {1}".format(PRIMARY_ID, output_rdd.count())
    print output_rdd.first()

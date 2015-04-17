def average_rdd(field_rdd):
    sum_count = field_rdd.combineByKey(
        (lambda val: (val, 1)),
        (lambda a, b: (a[0] + b, a[1] + 1)),
        (lambda a, b: (a[0] + b[0], a[1] + b[1])))
    avg = sum_count.map(lambda (key, (sum_, count)):
                        (key, sum_ / count))
    return avg


def min_rdd(field_rdd):
    min_num = field_rdd.combineByKey((lambda mem: mem), min, min)
    min_result = min_num.map(lambda (task_id, min_rec): (task_id, min_rec))
    return min_result


def max_rdd(field_rdd):
    max_num = field_rdd.combineByKey((lambda mem: mem), max, max)
    max_result = max_num.map(lambda (task_id, max_rec): (task_id, max_rec))
    return max_result


def job_status_rdd(field_rdd):
    """Create an RDD shows status of current job """
    job_event_rdd = field_rdd.map(lambda entry: (
        entry[0], 'FAIL' if entry[1] == 3 else 'NO_FAIL'))
    return job_event_rdd

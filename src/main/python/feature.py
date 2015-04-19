import operator


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


def combined_feature_helper(field_rdd, op):
    """Create an RDD to reduce list of parameters with operation """
    combined_feature_rdd = field_rdd.map(lambda entry: (
        entry[0], 0 if op == operator.div and 0 in entry[1] else reduce(op, entry[1])))
    return average_rdd(combined_feature_rdd)


def add_rdd(field_rdd):
    return combined_feature_helper(field_rdd, operator.add)


def subtract_rdd(field_rdd):
    return combined_feature_helper(field_rdd, operator.sub)


def multiply_rdd(field_rdd):
    return combined_feature_helper(field_rdd, operator.mul)


def divide_rdd(field_rdd):
    return combined_feature_helper(field_rdd, operator.div)
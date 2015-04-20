#!/usr/bin/env python

import argparse
import csv
import logging
import os
from collections import defaultdict
from ConfigParser import ConfigParser, NoOptionError
from pyspark import SparkConf, SparkContext

import rdd
import feature


def main():
    # Get arguments and configuration
    args = parse_args()
    config = parse_config(args.config)
    logging.getLogger().setLevel(logging.INFO)

    # Load the tables, features, and schema
    tables = get_tables(config)
    features = get_features(config)
    schema_path = args.schema or os.path.join(args.data_dir,
                                              config.get('main', 'schema'))
    schema = parse_schema(schema_path)

    sc = initialize_spark()

    # Generate RDDs for each table
    table_rdds = dict(zip(tables.iterkeys(), [rdd.load_csv(
        sc, os.path.join(args.data_dir, table, csv_file), schema[table])
        for table, csv_file in tables.iteritems()]))

    # Generate the RDDs of all features
    feature_rdds = []
    feature_names = []
    feature_key = config.get('main', 'key')
    for feature_name, feature_info in features.iteritems():
        # Generate an RDD with all necessary fields for the feature
        field_rdd = rdd.field_rdd(table_rdds[feature_info['table']],
                                  feature_key,
                                  feature_info['fields'])

        # Generate an RDD from the specified functions
        for function in feature_info['functions']:
            function_name = '{0}_rdd'.format(function)
            feature_rdds.append(getattr(feature, function_name)(field_rdd))
            feature_names.append(feature_name if
                                 len(feature_info['functions']) == 1
                                 else '{0} {1}'.format(function, feature_name))

    # Join the RDDs on their key
    joined_rdds = rdd.join_rdds(feature_rdds)

    # Write to CSV
    output_fields = [feature_key] + feature_names
    output_rdd = joined_rdds.map(lambda (key, feature_values): dict(
        zip(output_fields, [key] + list(feature_values))))
    output_rdd.mapPartitions(lambda records: rdd.writeRecords(
        output_fields, records)).saveAsTextFile(args.output_file)

    # Print header
    for field in output_fields:
        print field

    logging.info("Processed {0} {1}(s)".format(output_rdd.count(),
                                               feature_key))
    logging.info(output_rdd.first())


def parse_args():
    """Get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('data_dir', help='Data directory')
    parser.add_argument('output_file', help='Output file')
    parser.add_argument('--config', help='Config file',
                        default='../resources/config/feature_construction.ini')
    parser.add_argument('--schema', help='Schema file')
    args = parser.parse_args()
    return args


def parse_config(config_filepath):
    """Get configuration."""
    config = ConfigParser()
    config.read(config_filepath)
    return config


def parse_schema(schema_filepath):
    """Parse a schema CSV file and return a dict of tables and fields."""
    with open(schema_filepath, 'r') as schema_file:
        schema_reader = csv.DictReader(schema_file)

        schema = defaultdict(list)
        for row in schema_reader:
            file_pattern = row['file pattern'].split('/')[0]
            schema[file_pattern].append(row['content'])

    return dict(schema)


def get_tables(config):
    """Get a dict of { table name: file pattern }."""
    table_list = config.get('main', 'tables').split(',')
    tables = dict(zip(table_list, [config.get('table_files', files)
                                   for files in table_list]))
    return tables


def get_features(config):
    """Get a dict of features with their corresponding info."""
    feature_list = config.get('main', 'features').split(',')
    feature_properties = ['table', 'fields', 'functions']
    features = defaultdict(dict)

    for feature_name in feature_list:
        for prop in feature_properties:
            try:
                value = config.get(feature_name, prop)
            except NoOptionError as ex:
                # Allow the feature name to be the default 'fields' value
                if prop == 'fields':
                    value = feature_name
                else:
                    raise ex
            value = value.split(',') if prop.endswith('s') else value
            features[feature_name][prop] = value

    return dict(features)


def initialize_spark():
    """Configure and initialize Spark."""
    conf = (SparkConf().setAppName('Ericsson').
            # Allow spark to overwrite output files
            set("spark.hadoop.validateOutputSpecs", "false"))
    return SparkContext(conf=conf)

if __name__ == '__main__':
    main()

#!/usr/bin/env python

import argparse
import logging


def main():
    logging.getLogger().setLevel(logging.INFO)
    args = parse_args()
    clean_files(args.files)


def clean_values(value):
    """Format floats to not include 'f' when printed"""
    try:
        val = format(float(value), '.5f')
    except:
        val = value
    return val


def clean_files(files):
    """Clean csv data files"""
    for f in files:
        data = []

        with open(f, 'r') as fi:
            lines = fi.readlines()
            for l in lines:
                fields = l.split(',')
                data.append(map(clean_values, fields))

        with open(f, 'w') as fi:
            for d in data:
                fi.write(','.join(d) + '\n')

        logging.info('Cleaned {0}'.format(f))


def parse_args():
    """Get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs='*', help='Input file pattern')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()

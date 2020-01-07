#!/usr/bin/python3

"""aggregate multiple rows together

USAGE

aggregator.py [-t time_column] [-a aggregator] 
              [-s aggregate_specification] [-u unsorted_keys]
              [input_file] [output_file]

The script aggregates all the incoming data for a given set of keys.
By default we assume that this is the output of the FeatureCounter
from the gawseed modules and the aggregator algorithm is set to
gawseed.analysis.aggregator.FastAggregator.  Use the -a, -s and -u
switches to change that algorithm and parameters to the
gawseed.analysis.aggregator.Aggregator if another type of input data
is used.

XXX: gawseed.analysis.aggregator.Aggregator is incomplete

EXAMPLE

featureCounter.py [args] | aggregrator.py > results

"""

import argparse
import sys
import importlib
import inspect

# ick, remote during packaging:
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..', 'modules'))

# place holders for objects to load later
Fsdb = None
FastAggregator = None

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-t", "--time-column", default="timestamp", type=str,
                        help="The name of the time column to use")

    parser.add_argument("-s", "--specification", nargs="*", default=["gawseed.analysis.aggregator.summer"],
                        help="Specification to specify special aggregator operations")

    parser.add_argument("-y", "--yaml-specification", type=argparse.FileType('r'),
                        help="YAML file to use for loading processing specifications")

    parser.add_argument("-Z", "--use-zip", nargs="*", type=str,
                        help="Use zipimporter to load modules this zip file rather than the native imports")

    parser.add_argument("input_file", type=argparse.FileType('r'),
                        nargs='?', default=sys.stdin,
                        help="File to read")

    parser.add_argument("output_file", type=argparse.FileType('w'),
                        nargs='?', default=sys.stdout,
                        help="File to read")

    args = parser.parse_args()
    return args

def load_modules(args):
    global Fsdb
    global FastAggregator
    if args.use_zip:
        import zipimport
        importer = zipimport.zipimporter(args.use_zip[0])
        fsdb_module = importer.load_module("fsdb")
        gawseed = importer.load_module("gawseed") # everything below fails on some machines without this
        aggregator_module = importer.load_module("gawseed/analysis/aggregator")

        Fsdb = fsdb_module.Fsdb
        FastAggregator = aggregator_module.FastAggregator
    else:
        from pyfsdb import Fsdb
        from gawseed.analysis.aggregator import FastAggregator

def main():
    args = parse_args()
    load_modules(args)

    f = Fsdb(file_handle = args.input_file, out_file_handle = args.output_file, pass_comments='e')

    time_col_num = f.get_column_number(args.time_column)

    # create the feature counter instance
    ag = FastAggregator(aggregators = args.specification,
                        yaml_specification = args.yaml_specification)

    for output_row in ag.process(f):
        f.append(output_row)

if __name__ == "__main__":
    main()


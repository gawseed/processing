#!/usr/bin/python3

"""aggregate, filter and count features in FSDB data

USAGE

featureCounter.py [-t time_column] [-b bin_size] -s SPEC \
                  [input_file] [output_file]

Where SPEC is a comma separated list of colon separated data to
analyze.  Each colon separated portion should be in the form
OUTPUT_NAME:ALGORITHM:[COLUMN_NAME:[VALUE_FUNCTION:VALUE_ARGS]].  If
COLUMN_NAME is not specified then it will use the OUTPUT_NAME.  The
ALGORITHM should be a python function from a module to import
(e.g. gawseed.algorithm.generic.identity) that knows how to
process/filter the column into a key/subkey pair.  If VALUE_FUNCTION
is specified then it will point to another module/function to import
to compute a value with using comma separated VALUE_ARGS as arguments
to it.

The output will consist of five columns:

    timebin: the time stamp binned into specified [tbd] time windows
    index:   the OUTPUT_NAME from the SPEC
    key:     the first output from the ALGORITHM results
    subkey:  the second output from the ALGORITHM results
    value:   the count of the key/subkey's for that index in that timebin 

EXAMPLE

Count all the individual 'name' columns seen in a dataset:

    -s name_count:gawseed.algorithm.generic.identity:name

Count all the prefixes for a give DNS domain (www.example.com has a
prefix of "www" and a domain of "example.com").  The key in this is
the domain and the subkey is the prefix:

    -s prefix_count:gawseed.algorithm.dns.PSL_prefix:name

Silly example to show it works on generic files:

    (echo "#fsdb -F C: login pwd uid gid name dir shell" ; 
    cat /etc/passwd ) |
    python3 featureCounter.py \\
    -s numshells:gawseed.algorithm.generic.identity:shell \\
    gid:gawseed.algorithm.generic.identity:gid \\
    -b 100 -t uid

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
featureCounter = None

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-t", "--time-column", default="time", type=str,
                        help="The name of the time column to use")

    parser.add_argument("-b", "--bin-size", type=int,
                        help="Time bin size to use (seconds)")

    parser.add_argument("-s", "--specification", nargs="+",
                        help="Specification to process in the form colname:gawseed.algorithm.X[:outputname],...")

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
    global FeatureCounter
    if args.use_zip:
        import zipimport
        importer = zipimport.zipimporter(args.use_zip[0])
        fsdb_module = importer.load_module("fsdb")
        gawseed = importer.load_module("gawseed") # everything below fails on some machines without this
        gawseed = importer.load_module("gawseed/support/functionLoader") # everything below fails on some machines without this
        featureCounter = importer.load_module("gawseed/analysis/featureCounter")

        Fsdb = fsdb_module.Fsdb
        FeatureCounter = featureCounter.FeatureCounter
    else:
        from pyfsdb import Fsdb
        from gawseed.analysis.featureCounter import FeatureCounter

def main():
    args = parse_args()
    load_modules(args)

    specs=args.specification

    f = Fsdb(file_handle = args.input_file, out_file_handle = args.output_file, pass_comments='e')

    time_col_num = f.get_column_number(args.time_column)
    bin_size = args.bin_size

    # break apart the SPECs and load the needed modules/functions
    mapping_functions = None

    if not args.yaml_specification:
        mapping_functions = {}
        for spec in specs:
            parts = spec.split(":")

            if len(parts) < 2:
                sys.stderr.write("Bad specification: %s\n" % (spec))
                sys.stderr.write(" breakdown: %s\n" % (parts))
                sys.stderr.write("all specs: " + str(specs) + "\n")
                exit(1)

            outputname = parts[0]

            if len(parts) <= 2: # duplicate column name
                parts.append(outputname)

            function_name = parts[1]
            column_names = parts[2].split(",")

            spec = { 'function':  function_name,
                     'arguments': column_names }

            if len(parts) >= 4:
                spec['value'] = parts[3]

            if len(parts) == 5:
                spec['value_arguments'] = list(map(int, parts[4].split(",")))

            mapping_functions[outputname] = spec
    
            #print(spec)

    # create the feature counter instance
    fc = FeatureCounter(time_col_num, mapping_functions, bin_size = bin_size,
                        yaml_specification = args.yaml_specification)

    # XXX: don't assume all arguments are column names
    mapping_functions = fc.mapping_functions()
    for item in mapping_functions:
        mapping_functions[item]['arguments'] = fc.convert_argument_specifiers(mapping_functions[item]['arguments'], f)
        if 'value_arguments' in mapping_functions[item]:
            mapping_functions[item]['value_arguments'] = fc.convert_argument_specifiers(mapping_functions[item]['value_arguments'], f)
            

    if type(fc.time_column) == type("str"):
        # need to convert it to the column number from the yaml spec
        fc.time_column = f.get_column_number(fc.time_column)

    # declare the output names for fsdb
    f.out_column_names = ['timestamp','index','key','subkey','value']

    for output_row in fc.process(f, incremental = True):
        f.append(output_row)

if __name__ == "__main__":
    main()


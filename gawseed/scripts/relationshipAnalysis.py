#!/usr/bin/python3

"""analyze relationships between data

USAGE

relationshipAnalysis.py [-t time_column] -c output_column_defs
              [input_file] [output_file]

The script performs mathematical functions to analyze relationship in
data between multiple columns in gawseed aggregated output format.

EXAMPLE

featureCounter.py -y analysis.yml | aggregrator.py -y analysis.yml |
relationshipAnalysis -y analysis.yml

"""

import argparse
import sys
import importlib
import inspect

# ick, remote during packaging:
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..', 'modules'))

Fsdb = None
RelationshipAnalysis = None

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-t", "--time-column", default="timestamp", type=str,
                        help="The name of the time column to use")

    parser.add_argument("-c", "--output-columns", type=str, nargs='+',
                        help="List of output column specifications to use")

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

    # generate the column structure
    if args.output_columns:
        columns_info = {}
        for column_info_spec in args.output_columns:
            column_info = {}
            
            column_split = column_info_spec.index(":")
            column_name = column_info_spec[:column_split]
            columns_info[column_name] = column_info
            column_function_info = column_info_spec[column_split+1:]

            column_info['function'] = 'foo'

            column_split = column_function_info.index(":")
            column_function = column_function_info[:column_split]
            column_function_pieces = column_function_info[1+column_split:].split(",")

            column_info['function'] = column_function
            column_info['arguments'] = column_function_pieces

            args.columns_info = columns_info
    else:
        args.columns_info = None    

    return args

def load_modules(args):
    global Fsdb
    global RelationshipAnalysis
    if args.use_zip:
        import zipimport
        importer = zipimport.zipimporter(args.use_zip[0])
        fsdb_module = importer.load_module("fsdb")
        gawseed = importer.load_module("gawseed") # everything below fails on some machines without this
        relationshipAnalysis_module = importer.load_module("gawseed/support")
        relationshipAnalysis_module = importer.load_module("gawseed/analysis/relationshipAnalysis")

        Fsdb = fsdb_module.Fsdb
        RelationshipAnalysis = relationshipAnalysis_module.RelationshipAnalysis
    else:
        from pyfsdb import Fsdb
        from gawseed.analysis.relationshipAnalysis import RelationshipAnalysis

def main():
    args = parse_args()
    load_modules(args)

    # create the relationship analysis instance
    ra = RelationshipAnalysis(args.columns_info, import_from_zip=args.use_zip,
                              yaml_specification = args.yaml_specification)

    f = Fsdb(file_handle = args.input_file, out_file_handle = args.output_file, pass_comments='e')

    out_column_names = ['timestamp', 'key', 'subkey']
    out_column_names.extend(ra.column_names())
        
    f.out_column_names = out_column_names

    #time_col_num = f.get_column_number(args.time_column)
    #print("#" + str(f.column_names))
    

    for output_row in ra.process(f):
        f.append(output_row)

if __name__ == "__main__":
    main()


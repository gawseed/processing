#!/usr/bin/python3

"""Analyzes features for given key/subkeys in FSDB formatted data.

The featureCounter script examines time-series datasets for features,
by extracting out keys and (optionally) subkeys from FSDB formatted
datasets.  The YAML specification passed in is expected to be marked
with:

    - a 'binSize' integer field that indicates the number of seconds
      all time values should be "binned" into.  This is done via
      bin(t) = t - t % binSize

    - A timeColumn string to indicate the column name where the time
      stamp for the row is stored.

    - a 'featureCounter:' dictionary section, containing an 'output'
      dictionary specification, which in turn should contain an array
      of dictionary objects containing at least a 'function' and
      optional 'arguments', 'value', 'combine' and 'combine_arguments' entries.

"""

import gawseed.analysis
from gawseed.algorithm.generic import one, identity, combine_summer
from gawseed.support.functionLoader import load_function

class FeatureCounter(gawseed.analysis.Analysis):
    """Counts features found in data columns into key/subkey pairs.

       See the scripts/general/featureCounter.py documentation for
       further information.
    """
    _mapping_functions = None

    def __init__(self, time_column, mapping_functions = None, bin_size = None, import_from_zip = None, yaml_specification = None, filter_functions = None):
        super().__init__(time_column, bin_size, yaml_specification)
        self._import_from_zip = import_from_zip

        if mapping_functions is not None:
            self._mapping_functions = mapping_functions
        else:
            if yaml_specification == None:
                raise ValueError("FeatureCounter requires either mapping_function or yaml_specification to be passed")
            if 'featureCounter' not in self._specification:
                raise ValueError("featureCounter token missing from YAML specification")
            if 'outputs' not in self._specification['featureCounter']:
                raise ValueError("featureCounter.outputs tokens missing from YAML specification")

            self._mapping_functions = self._specification['featureCounter']['outputs']

        if filter_functions is not None:
            self._filter_functions = filter_functions
        else:
            if self._specification and 'featureCounter' in self._specification and 'filters' in self._specification['featureCounter']:
                self._filter_functions = self._specification['featureCounter']['filters']
            else:
                self._filter_functions = []

        # prepopulate mapping function empty arguments if needed
        for fn in self._mapping_functions:
            # assume default of identity mapping
            if 'function' not in self._mapping_functions[fn]:
                self._mapping_functions[fn]['function'] = identity
            if 'arguments' not in self._mapping_functions[fn]:
                self._mapping_functions[fn]['arguments'] = []

            # load the function if it's a string specification
            if type(self._mapping_functions[fn]['function']) == type("str"):
                self._mapping_functions[fn]['function'] = load_function(self._mapping_functions[fn]['function'], import_from_zip=self._import_from_zip)

            # default for value extraction is just the number 1 for counting purposes
            if 'value' not in self._mapping_functions[fn]:
                self._mapping_functions[fn]['value'] = one
            elif type(self._mapping_functions[fn]['value']) == type("str"):
                self._mapping_functions[fn]['value'] = load_function(self._mapping_functions[fn]['value'], import_from_zip=self._import_from_zip)


            # default for combining is to take the sum
            if 'combine' not in self._mapping_functions[fn]:
                self._mapping_functions[fn]['combine'] = combine_summer
            elif type(self._mapping_functions[fn]['combine']) == type("str"):
                self._mapping_functions[fn]['combine'] = load_function(self._mapping_functions[fn]['combine'], import_from_zip=self._import_from_zip)
            if 'combine_arguments' not in self._mapping_functions[fn]:
                self._mapping_functions[fn]['combine_arguments'] = []

            # default to empty arguments
            if 'value_arguments' not in self._mapping_functions[fn]:
                self._mapping_functions[fn]['value_arguments'] = []

        # populate any missing arguments to filter functions
        for fn in self._filter_functions:
            print(fn)
            if 'function' not in fn:
                raise ValueError("filter_functions passed without a 'filter' keyword")

            # actually load it
            if type(fn['function']) == type("str"):
                fn['function'] = load_function(fn['function'], default_module='gawseed.algorithm.filter', import_from_zip=self._import_from_zip)

            # set the arguments to [] if none were passed
            if 'arguments' not in fn:
                fn['arguments'] = []

    def process(self, input_stream, incremental=False):
        prelim = {}
        oldtimebin = -1

        # now that we have input stream, process argument specifiers
        # XXX: this feels like the wrong place to do this
        for fn in self._filter_functions:
            for pos in range(0, len(fn['arguments'])):
                (column, col_number) = self.convert_argument_specifier(fn['arguments'][pos], input_stream)
                if col_number:
                    fn['arguments'][pos] = col_number
                elif column:
                    fn['arguments'][pos] = column
                # else leave as is

        # loop through all the rows and evaluate each of them
        for row in input_stream:

            # apply any filters; if any return false then exclude the data
            skip=False
            for fn in self._filter_functions:
                if not fn['function'](row, fn['arguments']):
                    skip = True
                    break
            if skip: # bad python style...  should use a function
                continue

            # get the timestamp column value, and bin it
            if self._time_column >= len(row)-1:
                continue
            timebin = self.timebin(row[self._time_column])

            # create the slice for this timebin
            if incremental and timebin != oldtimebin and oldtimebin != -1:
                # start yielding values immediately after finishing a timestamp
                for index in prelim[oldtimebin]:
                    for key in prelim[oldtimebin][index]:
                        for subkey in prelim[oldtimebin][index][key]:
                            yield([oldtimebin, index, key, subkey,
                                   prelim[oldtimebin][index][key][subkey]])

                # re-init the prelim data to drop the old data for mem savings
                prelim = { }

            oldtimebin = timebin

            if timebin not in prelim:

                prelim[timebin] = {}
                
                # create a hash for each index too
                for name in self._mapping_functions:
                    prelim[timebin][name] = {}

            # for each mapping function, 
            for index in self._mapping_functions:
                args = self._mapping_functions[index]['arguments']
                (key, subkey) = self._mapping_functions[index]['function'](row, args)
            
                if not key:
                    continue
                
                # this is faster than using a Counter from collections
                if key not in prelim[timebin][index]:
                    prelim[timebin][index][key] = {}

                
                value_args = self._mapping_functions[index]['value_arguments']
                value = self._mapping_functions[index]['value'](row, value_args)

                combine_fn = self._mapping_functions[index]['combine']
                combine_args = self._mapping_functions[index]['combine_arguments']
                current_val = None

                if subkey in prelim[timebin][index][key]:
                    current_val = prelim[timebin][index][key][subkey]

                prelim[timebin][index][key][subkey] = combine_fn(timebin, index,
                                                                 key, subkey,
                                                                 current_val, value)
                    
            oldtimebin = timebin

        # collect and report all the results
        for timebin in prelim:
            for index in prelim[timebin]:
                for key in prelim[timebin][index]:
                    for subkey in prelim[timebin][index][key]:
                        yield([timebin, index, key, subkey,
                               prelim[timebin][index][key][subkey]])

    
    def mapping_functions(self):
        return self._mapping_functions


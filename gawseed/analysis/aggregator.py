#!/usr/bin/python3

import gawseed.analysis
import pdb
from gawseed.support.functionLoader import load_function
from gawseed.algorithm.aggregator import summer

class FastAggregator(gawseed.analysis.Analysis):
    """Aggregates information together from multiple FeatureCounter outputs.

       See the scripts/general/aggregator.py documentation for
       further information.


       FastAggregator assumes keys are always sorted(time), index,
       key, subkey, value.  See the Aggregator class for a class that
       doesn't require this structure.
    """
    _results = {}
    _last_time = 0
    _aggregators = [ summer ]

    def __init__(self, aggregators = [summer], import_from_zip = None, yaml_specification = None):
        self._aggregators = []
        self._import_from_zip = import_from_zip
        super().__init__(None, None, yaml_specification)

        if self._specification is not None:
            if 'aggregator' not in self._specification:
                raise ValueError('yaml specification requires an aggregator token')
            if 'aggregators' not in self._specification['aggregator']:
                raise ValueError('aggregator specification requires an aggregators token of an array of functions')
            aggregators = self._specification['aggregator']['aggregators']

        for definition in aggregators:
            # load the function if it's a string specification
            
            if type(definition) == type("str"):
                function = definition
                parts = function.split(":")
                args = []

                # potentially parse function/args apart
                if len(parts) > 1:
                    function = parts[0]
                    args = parts[1].split(",")

                # load the string a as module.function
                fn = load_function(function, default_module = 'gawseed.algorithm.aggregator',
                                   import_from_zip=self._import_from_zip)

                self._aggregators.append({ 'function' : fn,
                                           'arguments': args})

            # loaded from yaml
            elif type(definition) == type({}): 
                arguments = []
                if 'arguments' in definition:
                    arguments = definition['arguments']

                fn = load_function(definition['function'],
                                   default_module = 'gawseed.algorithm.aggregator')
                self._aggregators.append({ 'function': fn,
                                           'arguments' : arguments})

            # assume its otherwise a python function object
            else:
                self._aggregators.append({ 'function': definition,
                                           'arguments' : []})

    def process(self, data_iterator):
        self._last_time = -1
        for row in data_iterator:
            if row[0] != self._last_time:
                if int(self._last_time) > int(row[0]):
                    raise ValueError("Time went backwards -- input data must be pre-sorted for time")

                # end of a timestream, so start releasing the current collected data
                for index in self._results:
                    for key in self._results[index]:
                        for subkey in self._results[index][key]:
                            yield [self._last_time, index, key, subkey, self._results[index][key][subkey]]

                self._results = {}
                self._last_time = row[0]

            index = row[1]
            key = row[2]
            subkey = row[3]
            value = float(row[4]) # always?
    
            for aggregator in self._aggregators:
                aggregator['function'](index, key, subkey, value, self._results, aggregator['arguments'])
                                   
        # release the final data
        for index in self._results:
            for key in self._results[index]:
                for subkey in self._results[index][key]:
                    yield [self._last_time, index, key, subkey, self._results[index][key][subkey]]

class Aggregator(gawseed.analysis.Analysis):
    """Currently broken -- do not use"""
    _sorted = []
    _sorted_len = 0
    _non_sorted = []
    _non_sorted_len = 0
    _value_columns = []
    _value_columns_len = 0

    _primary_indexes = []

    _current_depth = 0
    _results = {}

    def __init__(self, aggregator = summer,
                 sorted_fields = [0],
                 non_sorted_fields = [1, 2, 3],
                 value_columns = [4]):

        self._sorted = sorted_fields
        self._sorted_len = len(sorted_fields)

        self._non_sorted = non_sorted_fields
        self._non_sorted_len = len(non_sorted_fields)

        self._value_columns = value_columns
        self._value_columns_len = len(value_columns)

        self._primary_indexes = []
        for i in range(0, self._sorted_len):
            self._primary_indexes.append(None)

    @property
    def sorted_fields(self):
        return self._sorted

    @sorted_fields.setter
    def set_sorted_fields(self, newlist):
        self._sorted_len = len(newlist)
        self._sorted = newlist
        
        self._primary_indexes = []
        for i in range(0, self._sorted_len):
            self._primary_indexes.append(None)

    @property
    def non_sorted_fields(self):
        return self._non_sorted

    @non_sorted_fields.setter
    def set_non_sorted_fields(self, newlist):
        self._not_sorted_len = len(newlist)
        self._non_sorted = newlist

    @property
    def value_columns(self):
        return self._value_columns

    @value_columns.setter
    def set_value_columns(self, newlist):
        self._len_value_columns = len(newlist)
        self._value_columns = newlist

    def transform_results_recursive(self, results, keys = [], depth = 0):
        if depth == self._non_sorted_len - 1:
            # we're down to the bottom, report results
            #pdb.set_trace()
            for item in results:
                rowkeys=keys[:]
                rowkeys.append(item)
                for value in results[item]:
                    rowkeys.append(value)
                yield rowkeys
        else:
            for item in results:
                subkeys = keys[:]
                subkeys.append(item)
                yield from self.transform_results_recursive(results[item], subkeys, depth+1)            

    def transform_results_broken(self, results):
        if not results:
            results = self._results

        # our primary keys become the first columns,
        # with the secondary keys in depth
        current_indexes = []
        for i in range(0, self._sorted_len):
            current_indexes.append(self._primary_indexes[i])

        # a stack of keys we'll build as we descend into the tree
        ckeys = []

        # a reference to the current point in the tree
        tree_ref = results
        tree_refs = [results]

        for i in range(0, self._non_sorted_len + 1):
            tree_refs.append(tree_ref)
            for key in tree_ref:
                pass

        result_rows = []
        resultref = self._results 
        for i in range(0, self._non_sorted_len):
            if row[i] not in resultref:
                resultref[row[i]] = {}

            current_row = current_indexes[:] # this is faster than .copy()

    # for each incoming row we:
    #    - test to see if the sorted indexes are still the same; if
    #      not, then we yield the results so far
    #    - set the saved indexes to the existing ones, and continue
    #      collecting the sub-data in the other indexes.
    def process(self, data_iterator):
        for row in data_iterator:
            for i in range(0, self._sorted_len):
                if row[i] != self._primary_indexes[i]:
                    yield from self.transform_results_recursive(self._results, self._primary_indexes)

                    # now reset the results:
                    self._results = {}

                    # and change the primary indexes
                    for j in range(0, self._sorted_len):
                        self._primary_indexes[j] = row[self._sorted[j]]

            # at this point, all sorted keys match so we need to
            # memorize secondary (non-sorted) indexes
            #pdb.set_trace()
            resultref = self._results
            for i in range(0, self._non_sorted_len - 1):
                if row[self._non_sorted[i]] not in resultref:
                    resultref[row[self._non_sorted[i]]] = {}
                resultref = resultref[row[self._non_sorted[i]]]


            # resultref now is the depth-most pointer to where to store value data
            #pdb.set_trace()
            last_key = self._non_sorted[self._non_sorted_len - 1]
            if row[last_key] not in resultref:
                resultref[row[last_key]] = []
                resultref = resultref[row[last_key]]
                for i in range(0, self._value_columns_len):
                    resultref.append(float(row[self._value_columns[i]]))
            else:
                resultref = resultref[row[last_key]]
                for i in range(0, self._value_columns_len):
                    resultref[i] += float(row[self._value_columns[i]])
                
                    
        yield from self.transform_results_recursive(self._results, self._primary_indexes)

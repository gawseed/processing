#!/usr/bin/python3

import gawseed.analysis
from gawseed.algorithm.generic import one, identity
from gawseed.support.functionLoader import load_function

class RelationshipAnalysis(gawseed.analysis.Analysis):
    """Performs final math analysis and transforms row data into columns.

       See the scripts/general/relationshipAnalysis.py documentation for
       further information.
    """
    def __init__(self, output_columns=None, import_from_zip=None, yaml_specification=None):
        super().__init__(None, None, yaml_specification = yaml_specification)
        self._output_columns = output_columns
        self._import_from_zip = import_from_zip

        if not output_columns:
            if yaml_specification == None:
                raise ValueError("RelationshipAnalysis requires either output_columns or yaml_specification to be passed")
            if 'relationshipAnalysis' not in self._specification:
                raise ValueError("relationshipAnalysis token missing from YAML specification")
            
            if 'outputs' not in self._specification['relationshipAnalysis']:
                raise ValueError("relationshipAnalysis.outputs token missing from YAML specification")

            self._output_columns = self._specification['relationshipAnalysis']['outputs']

        for output_column in self._output_columns:
            if 'arguments' not in self._output_columns[output_column]:
                self._output_columns[output_column]['arguments'] = []

        self.load_functions()

    def load_functions(self):
        for output_column in self._output_columns:
            if type(self._output_columns[output_column]['function']) == type("str"):
                self._output_columns[output_column]['function'] = load_function(self._output_columns[output_column]['function'], default_module = 'gawseed.algorithm.relationship', import_from_zip=self._import_from_zip)

    def collect_data(self, data_iterator):
        current_index = None
        current_data = {}
        for row in data_iterator:
            timeval = row[0]
            index = row[1]
            key = row[2]
            subkey = row[3]
            value = row[4]

            if current_index and timeval != current_index:  # XXX: specify time col
                yield { 'time_index': current_index,
                        'data': current_data }
                current_data = {}
            current_index = timeval

            if key not in current_data:
                current_data[key] = {}

            if subkey not in current_data[key]:
                current_data[key][subkey] = {}
                
            current_data[key][subkey][index] = value # XXX: this overwrites; should we check and error?

        yield { 'time_index': current_index,
                'data': current_data }

    def process(self, data_iterator):
        for bundle in self.collect_data(data_iterator):
            # bundle will be {time_index: foo, data: { key: { subkey: { index: VAL }}}} structures
            timeindex = bundle['time_index']
            data = bundle['data']
            for key in data:
                for subkey in data[key]:
                    result_row = [timeindex, key, subkey]
                    have_data = False
                    for column in self._output_columns:
                        column_def = self._output_columns[column]
                        calculated = column_def['function'](bundle, key, subkey, data[key][subkey],
                                                            column_def['arguments'])
                        if calculated is not None:
                            have_data = True
                        result_row.append(calculated)

                    if have_data:
                        yield result_row

    def column_names(self):
        return list(self._output_columns.keys())

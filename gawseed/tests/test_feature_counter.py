#!/usr/bin/python3

import unittest

class featureCounterTests(unittest.TestCase):

    def test_init_featureCounter(self):
        import gawseed.analysis.featureCounter

        fc = gawseed.analysis.featureCounter.FeatureCounter(0, {})
        self.assertTrue(fc)

    def test_identity_counter(self):
        import gawseed.analysis.featureCounter
        from gawseed.algorithm.generic import identity

        input_data = [
            [10.5, 'valueA', 40],
            [13,   'valueB', 4200],
            [59,   'valueA', 2],
            [62,   'valueA', 100],
            [62,   'valueC', 200],
            [115,  'valueA',  50],
            [181,  'valueA', 1]
        ]

        expected_output = [
            [0,   'output', 'valueA', '', 2],
            [0,   'output', 'valueB', '', 1],
            [60,  'output', 'valueA', '', 2],
            [60,  'output', 'valueC', '', 1],
            [180, 'output', 'valueA', '', 1]
        ]

        fc = gawseed.analysis.featureCounter.FeatureCounter(0, { 'output':
                                                                 { 'function': identity, 'arguments': [1] } } )

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process returns correct data')

    def test_identity_adder(self):
        import gawseed.analysis.featureCounter
        from gawseed.algorithm.generic import identity, column_value

        input_data = [
            [10.5, 'valueA', 40],
            [13,   'valueB', 4200],
            [59,   'valueA', 2],
            [62,   'valueA', 100],
            [62,   'valueC', 200],
            [115,  'valueA',  50],
            [181,  'valueA', 1]
        ]

        expected_output = [
            [0,   'output', 'valueA', '', 42],
            [0,   'output', 'valueB', '', 4200],
            [60,  'output', 'valueA', '', 150],
            [60,  'output', 'valueC', '', 200],
            [180, 'output', 'valueA', '', 1]
        ]

        # process everything in bulk
        fc = gawseed.analysis.featureCounter.FeatureCounter(0, { 'output':
                                                                 { 'function': identity,
                                                                   'arguments': [1],
                                                                   'value': column_value,
                                                                   'value_arguments': [2]} } )

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process returns correct data')

        # try running in incremental mode
        fc = gawseed.analysis.featureCounter.FeatureCounter(0, { 'output':
                                                                 { 'function': identity,
                                                                   'arguments': [1],
                                                                   'value': column_value,
                                                                   'value_arguments': [2]} })

        results = []
        for out_row in fc.process(input_stream = input_data, incremental=True):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process returns correct data')

    def test_domain_counter(self):
        import gawseed.analysis.featureCounter
        from gawseed.algorithm.dns import PSL_domain

        input_data = [
            [10.5, 'www.example.com', 40],
            [13,   'www.example.co.uk', 4200],
            [59,   'www.example.com', 2],
            [62,   'www.example.com', 100],
            [62,   'www.example.tr', 200],
            [115,  'www.example.com',  50],
            [181,  'www.example.com', 1]
        ]

        expected_output = [
            [0,   'output', 'example.com', '', 2],
            [0,   'output', 'example.co.uk', '', 1],
            [60,  'output', 'example.com', '', 2],
            [60,  'output', 'example.tr', '', 1],
            [180, 'output', 'example.com', '', 1]
        ]

        fc = gawseed.analysis.featureCounter.FeatureCounter(0, { 'output':
                                                                 { 'function': PSL_domain, 'arguments': [1] } } )

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process(PSL_domain) returns correct data')

    def test_domain_counter_by_text_function(self):
        import gawseed.analysis.featureCounter

        input_data = [
            [10.5, 'www.example.com', 40],
            [13,   'www.example.co.uk', 4200],
            [59,   'www.example.com', 2],
            [62,   'www.example.com', 100],
            [62,   'www.example.tr', 200],
            [115,  'www.example.com',  50],
            [181,  'www.example.com', 1]
        ]

        expected_output = [
            [0,   'output', 'example.com', '', 2],
            [0,   'output', 'example.co.uk', '', 1],
            [60,  'output', 'example.com', '', 2],
            [60,  'output', 'example.tr', '', 1],
            [180, 'output', 'example.com', '', 1]
        ]

        fc = gawseed.analysis.featureCounter.FeatureCounter(0, { 'output':
                                                                 { 'function': 'gawseed.algorithm.dns.PSL_domain',
                                                                   'arguments': [1] } } )

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process("PSL_domain") (by name) returns correct data')

    def test_domain_registration_counter(self):
        import gawseed.analysis.featureCounter
        from gawseed.algorithm.dns import PSL_registration

        input_data = [
            [10.5, 'www.example.com', 40],
            [13,   'www.example.co.uk', 4200],
            [59,   'www.google.com', 2],
            [62,   'www.example.com', 100],
            [62,   'www.example.tr', 200],
            [115,  'foo.bar.baz.com',  50],
            [181,  'www.example.com', 1]
        ]

        expected_output = [
            [0,   'output', 'com', '', 2],
            [0,   'output', 'co.uk', '', 1],
            [60,  'output', 'com', '', 2],
            [60,  'output', 'tr', '', 1],
            [180, 'output', 'com', '', 1]
        ]

        fc = gawseed.analysis.featureCounter.FeatureCounter(0, { 'output':
                                                                 { 'function': PSL_registration, 'arguments': [1] } } )

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process(PSL_registration) returns correct data')

    def test_domain_prefix_counter(self):
        import gawseed.analysis.featureCounter
        from gawseed.algorithm.dns import PSL_prefix

        input_data = [
            [10.5, 'www.example.com', 40],
            [13,   'www.example.co.uk', 4200],
            [59,   'img.google.com', 2],
            [59,   'www.example.com', 2],
            [62,   'www.example.com', 100],
            [62,   'www.example.tr', 200],
            [115,  'foo.bar.baz.com',  50],
            [181,  'www.example.com', 1]
        ]

        expected_output = [
            [0,   'output', 'example.com',   'www', 2],
            [0,   'output', 'example.co.uk', 'www', 1],
            [0,   'output', 'google.com',    'img', 1],
            [60,  'output', 'example.com',   'www', 1],
            [60,  'output', 'example.tr',    'www', 1],
            [60,  'output', 'baz.com',       'foo.bar', 1],
            [180, 'output', 'example.com',   'www', 1]
        ]

        fc = gawseed.analysis.featureCounter.FeatureCounter(0, { 'output':
                                                                 { 'function': PSL_prefix, 'arguments': [1] } } )

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process(PSL_registration) returns correct data')

    def test_domain_multiple_counters(self):
        import gawseed.analysis.featureCounter
        from gawseed.algorithm.dns import PSL_prefix, PSL_registration

        input_data = [
            [10.5, 'www.example.com', 40],
            [13,   'www.example.co.uk', 4200],
            [59,   'img.google.com', 2],
            [62,   'www.example.com', 100],
            [62,   'www.example.tr', 200],
            [115,  'foo.bar.baz.com',  50],
            [181,  'www.example.com', 1]
        ]

        expected_output = [
            [0,   'pslpre', 'example.com',   'www', 1],
            [0,   'pslpre', 'example.co.uk', 'www', 1],
            [0,   'pslpre', 'google.com',    'img', 1],
            [0,   'pslreg', 'com', '', 2],
            [0,   'pslreg', 'co.uk', '', 1],
            [60,  'pslpre', 'example.com',   'www', 1],
            [60,  'pslpre', 'example.tr',    'www', 1],
            [60,  'pslpre', 'baz.com',       'foo.bar', 1],
            [60,  'pslreg', 'com', '', 2],
            [60,  'pslreg', 'tr', '', 1],
            [180, 'pslpre', 'example.com',   'www', 1],
            [180, 'pslreg', 'com', '', 1]
        ]

        fc = gawseed.analysis.featureCounter.FeatureCounter(0,
                                                            {'pslpre':
                                                             { 'function': PSL_prefix, 'arguments': [1] },
                                                             'pslreg':
                                                             { 'function': PSL_registration, 'arguments': [1] } })

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process(PSL_registration) returns correct data')

    def test_domain_multiple_counters_incremental(self):
        import gawseed.analysis.featureCounter
        from gawseed.algorithm.dns import PSL_prefix, PSL_registration

        input_data = [
            [10.5, 'www.example.com', 40],
            [13,   'www.example.co.uk', 4200],
            [59,   'img.google.com', 2],
            [62,   'www.example.com', 100],
            [62,   'www.example.tr', 200],
            [115,  'foo.bar.baz.com',  50],
            [181,  'www.example.com', 1]
        ]

        expected_output = [
            [0,   'pslpre', 'example.com',   'www', 1],
            [0,   'pslpre', 'example.co.uk', 'www', 1],
            [0,   'pslpre', 'google.com',    'img', 1],
            [0,   'pslreg', 'com', '', 2],
            [0,   'pslreg', 'co.uk', '', 1],
            [60,  'pslpre', 'example.com',   'www', 1],
            [60,  'pslpre', 'example.tr',    'www', 1],
            [60,  'pslpre', 'baz.com',       'foo.bar', 1],
            [60,  'pslreg', 'com', '', 2],
            [60,  'pslreg', 'tr', '', 1],
            [180, 'pslpre', 'example.com',   'www', 1],
            [180, 'pslreg', 'com', '', 1]
        ]

        fc = gawseed.analysis.featureCounter.FeatureCounter(0,
                                                            {'pslpre':
                                                             { 'function': PSL_prefix, 'arguments': [1] },
                                                             'pslreg':
                                                             { 'function': PSL_registration, 'arguments': [1] } })

        results = []
        self.maxDiff = None
        for out_row in fc.process(input_stream = input_data, incremental=True):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process(PSL_registration) returns correct data')

    def test_different_column_value_extractor_spec(self):
        from gawseed.analysis.featureCounter import FeatureCounter

        input_data = [
            [0,   'valueA', 42],
            [0,   'valueB', 4200],
            [0,   'valueA', 52],
            [60,  'valueA', 150],
            [60,  'valueC', 200],
            [180, 'valueA', 1]
        ]

        expected_output = [
            [0,   'output', 'valueA', '', 94],
            [0,   'output', 'valueB', '', 4200],
            [60,  'output', 'valueA', '', 150],
            [60,  'output', 'valueC', '', 200],
            [180, 'output', 'valueA', '', 1]
        ]

        fc = FeatureCounter(0, { 'output':
                                 { 'function': 'gawseed.algorithm.generic.identity',
                                   'arguments': [1],
                                   'value': 'gawseed.algorithm.generic.column_value',
                                   'value_arguments': [2]} } )

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results, 'FeatureCounter.process("PSL_domain") (by name) returns correct data')

    def test_yaml_spec(self):
        from gawseed.analysis.featureCounter import FeatureCounter

        input_data = [
            [0,   'valueA', 42],
            [0,   'valueB', 4200],
            [0,   'valueA', 52],
            [60,  'valueA', 150],
            [60,  'valueC', 200],
            [180, 'valueA', 1]
        ]

        expected_output = [
            [0,   'output', 'valueA', '', 94],
            [0,   'output', 'valueB', '', 4200],
            [60,  'output', 'valueA', '', 150],
            [60,  'output', 'valueC', '', 200],
            [180, 'output', 'valueA', '', 1]
        ]

        yaml_stream = """
---

timeColumn: 0
featureCounter:
  outputs:
    output:
      function: gawseed.algorithm.generic.identity
      arguments: 
        - 1
      value: gawseed.algorithm.generic.column_value
      value_arguments: [2]
"""
        fc = FeatureCounter(yaml_specification = yaml_stream, time_column = 0)

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results,
                         'FeatureCounter.process("PSL_domain") (by name) returns correct data')
        
    def test_feature_string_identity(self):
        from gawseed.analysis.featureCounter import FeatureCounter

        input_data = [
            [0,   'valueA', 42],
            [0,   'valueB', 4200],
            [60,  'valueA', 150],
        ]

        expected_output = [
            [0,   'output', 'idstring', '', 2],
            [60,   'output', 'idstring', '', 1],
        ]

        yaml_stream = """
---

timeColumn: 0
featureCounter:
  outputs:
    output:
      function: string
      arguments: 
        - idstring
"""
        fc = FeatureCounter(yaml_specification = yaml_stream, time_column = 0)

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results,
                         'FeatureCounter.process("PSL_domain") (by name) returns correct data')
        

    def test_feature_filter_excludesrcipexprs(self):
        from gawseed.algorithm.filter import exclude_exprs
        
        input_data = [
            [10.5, 'valueA', 40],
            [13,   'valueB', 4200],
            [59,   'valueA', 2],
            [62,   'valueA', 100],
            [62,   'valueC', 200],
            [115,  'valueA',  50],
            [181,  'valueA', 1]
        ]

        expected_output = [
            [13,   'valueB', 4200],
            [62,   'valueC', 200],
        ]

        output_data = []
        for row in input_data:
            if exclude_exprs(row, [1, 'valueA']):
                output_data.append(row)
        self.assertEqual(expected_output, output_data,
                         "Filtering of valueB from input data failed")

    def test_feature_filter_include_exprs(self):
        from gawseed.algorithm.filter import include_exprs
        
        input_data = [
            [10.5, 'valueA', 40],
            [13,   'valueB', 4200],
            [59,   'valueA', 2],
            [62,   'valueA', 100],
            [62,   'valueC', 200],
            [115,  'valueA',  50],
            [181,  'valueA', 1]
        ]

        expected_output = [
            [10.5, 'valueA', 40],
            [59,   'valueA', 2],
            [62,   'valueA', 100],
            [115,  'valueA',  50],
            [181,  'valueA', 1]
        ]

        output_data = []
        for row in input_data:
            if include_exprs(row, [1, 'valueA']):
                output_data.append(row)
        self.assertEqual(expected_output, output_data,
                         "Filtering of valueB from input data failed")

    def test_feature_filtered_string_identity(self):
        from gawseed.analysis.featureCounter import FeatureCounter

        input_data = [
            [0,   'valueA', 42],
            [0,   'valueB', 4200],
            [60,  'valueA', 150],
            [60,  'valueA', 151],
            [60,  'valueB', 10],
            [60,  'valueC', 20],
            [60,  'isvalueD', 10]
        ]

        expected_output = [
            [0,   'output', 'valueB', '', 1],
            [60,   'output', 'valueB', '', 1],
            [60,   'output', 'valueC', '', 1],
        ]

        yaml_stream = """
---

timeColumn: 0
featureCounter:
  filters:
    - function: exclude_exprs
      arguments:
        - int(1)
        - valueA
        - val.*D
  outputs:
    output:
      function: identity
      arguments: 
        - 1
"""
        fc = FeatureCounter(yaml_specification = yaml_stream, time_column = 0)

        results = []
        for out_row in fc.process(input_stream = input_data):
            results.append(out_row)

        self.assertEqual(expected_output, results,
                         'FeatureCounter.process("PSL_domain") (by name) returns correct data')
        


if __name__ == '__main__':
    unittest.main()
    

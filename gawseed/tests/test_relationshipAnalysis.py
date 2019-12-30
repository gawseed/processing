#!/usr/bin/python3

import unittest

from gawseed.algorithm.generic import fraction
from gawseed.analysis.relationshipAnalysis import RelationshipAnalysis

class relationshipAnalysisTests(unittest.TestCase):

    def test_init_relationshipAnalysis(self):
        import gawseed.analysis.relationshipAnalysis

        ra = RelationshipAnalysis(
                                  output_columns = 
                                      { 'col1': { 'function': fraction,
                                                  'arguments': [ 1, 2 ] } }
                                  )
        self.assertTrue(ra)

    def test_collect_data(self):
        import gawseed.analysis.relationshipAnalysis

        ra = RelationshipAnalysis(
                                  output_columns = 
                                      { 'col1': { 'function': fraction,
                                                  'arguments': [ 1, 2 ] } }
                                  )
        self.assertTrue(ra)
        
        input_data = [
            ['60',  'data1', 'k1', 's1', 10.0],
            ['60',  'data1', 'k2', 's2', 10.0],
            ['60',  'data2', 'k1', 's1', 10.0],
            ['60',  'data2', 'k2', 's2', 20.0],

            ['120', 'data1', 'k1', 's1', 10.0],
            ['120', 'data1', 'k2', 's2', 20.0],
            ['120', 'data2', 'k1', 's1', 10.0],
            ['120', 'data2', 'k2', 's2', 10.0],
        ]

        expected_results = [
            { 'time_index': '60',
              'data': { 'k1': { 's1': { 'data1': 10.0,
                                        'data2': 10.0}},
                        'k2': { 's2': { 'data1': 10.0,
                                        'data2': 20.0,}}},
            },
            
            { 'time_index': '120',
              'data': { 'k1': { 's1': { 'data1': 10.0,
                                        'data2': 10.0}},
                        'k2': { 's2': { 'data1': 20.0,
                                        'data2': 10.0,}}},
            }
        ]

        current_row = 0
        for collected in ra.collect_data(input_data):
            self.assertEqual(collected, expected_results[current_row],
                             ("collected rows %d was as expected" % (current_row)))
            current_row += 1

    def test_simple_relation(self):
        import gawseed.analysis.relationshipAnalysis
        import gawseed.algorithm.relationship

        ra = RelationshipAnalysis(
                                  output_columns = 
                                  { 'col1': { 'function': gawseed.algorithm.relationship.fraction,
                                              'arguments': [ 'data1', 'data2' ] } }
                                  )
        self.assertTrue(ra)

        input_data = [
            ['60',  'data1', 'k1', 's1', 10.0],
            ['60',  'data1', 'k2', 's2', 10.0],
            ['60',  'data2', 'k1', 's1', 10.0],
            ['60',  'data2', 'k2', 's2', 20.0],

            ['120', 'data1', 'k1', 's1', 10.0],
            ['120', 'data1', 'k2', 's2', 20.0],
            ['120', 'data2', 'k1', 's1', 10.0],
            ['120', 'data2', 'k2', 's2', 10.0],
        ]

        expected_results = [
            # T, Key, subkey, data1/data2
            ['60',  'k1', 's1', 1.0],
            ['60',  'k2', 's2', 0.5],
            ['120', 'k1', 's1', 1.0],
            ['120', 'k2', 's2', 2.0],
        ]

        results = []
        for output in ra.process(input_data):
            results.append(output)

        self.assertEqual(results, expected_results, "expected results from relationshipAnalysis")

    def test_double_relation(self):
        import gawseed.analysis.relationshipAnalysis
        import gawseed.algorithm.relationship

        ra = RelationshipAnalysis(
                                  output_columns = 
                                  { 'col1': { 'function': gawseed.algorithm.relationship.fraction,
                                              'arguments': [ 'data1', 'data2' ] },
                                    'col2': { 'function': gawseed.algorithm.relationship.fraction,
                                              'arguments': [ 'data2', 'data1' ] } }
                                  )
        self.assertTrue(ra)

        input_data = [
            ['60',  'data1', 'k1', 's1', 10.0],
            ['60',  'data1', 'k2', 's2', 10.0],
            ['60',  'data2', 'k1', 's1', 10.0],
            ['60',  'data2', 'k2', 's2', 20.0],

            ['120', 'data1', 'k1', 's1', 10.0],
            ['120', 'data1', 'k2', 's2', 20.0],
            ['120', 'data2', 'k1', 's1', 10.0],
            ['120', 'data2', 'k2', 's2', 10.0],
        ]

        expected_results = [
            # T, Key, subkey, data1/data2
            ['60',  'k1', 's1', 1.0, 1.0],
            ['60',  'k2', 's2', 0.5, 2.0],
            ['120', 'k1', 's1', 1.0, 1.0],
            ['120', 'k2', 's2', 2.0, 0.5],
        ]

        results = []
        for output in ra.process(input_data):
            results.append(output)

        self.assertEqual(results, expected_results, "expected results from relationshipAnalysis")

    def test_missing_indexes(self):
        import gawseed.analysis.relationshipAnalysis
        import gawseed.algorithm.relationship
        import gawseed.algorithm.generic

        ra = RelationshipAnalysis(
                                  output_columns = 
                                  { 'col1': { 'function': gawseed.algorithm.relationship.fraction,
                                              'arguments': [ 'data1', 'data2' ] } ,
                                    'col2': { 'function': gawseed.algorithm.relationship.one,
                                              'arguments': ['foo']}}
                                  )
        self.assertTrue(ra)

        input_data = [
            ['60',  'data1', 'k1', 's1', 10.0],
            ['60',  'data1', 'k2', 's2', 10.0],
            ['60',  'data2', 'k1', 's1', 10.0],
            ['60',  'data2', 'k2', 's2', 10.0],

            # data2 missing for keys
            ['120', 'data1', 'k1', 's1', 10.0],
            ['120', 'data2', 'k2', 's2', 10.0],

        ]

        expected_results = [
            # T, Key, subkey, data1/data2
            ['60',  'k1', 's1', 1.0, 1.0],
            ['60',  'k2', 's2', 1.0, 1.0],
            ['120', 'k1', 's1', None, 1.0],
            ['120', 'k2', 's2', None, 1.0],
        ]

        results = []
        for output in ra.process(input_data):
            results.append(output)

        self.assertEqual(results, expected_results, "expected results from relationshipAnalysis")

    def test_load_by_name(self):
        import gawseed.analysis.relationshipAnalysis
        import gawseed.algorithm.relationship

        ra = RelationshipAnalysis(
                                  output_columns = 
                                  { 'col1': { 'function': 'gawseed.algorithm.relationship.fraction',
                                              'arguments': [ 'data1', 'data2' ] } }
                                  )
        self.assertTrue(ra)

        input_data = [
            ['60',  'data1', 'k1', 's1', 10.0],
            ['60',  'data1', 'k2', 's2', 10.0],
            ['60',  'data2', 'k1', 's1', 10.0],
            ['60',  'data2', 'k2', 's2', 20.0],

            ['120', 'data1', 'k1', 's1', 10.0],
            ['120', 'data1', 'k2', 's2', 20.0],
            ['120', 'data2', 'k1', 's1', 10.0],
            ['120', 'data2', 'k2', 's2', 10.0],
        ]

        expected_results = [
            # T, Key, subkey, data1/data2
            ['60',  'k1', 's1', 1.0],
            ['60',  'k2', 's2', 0.5],
            ['120', 'k1', 's1', 1.0],
            ['120', 'k2', 's2', 2.0],
        ]

        results = []
        for output in ra.process(input_data):
            results.append(output)

        self.assertEqual(results, expected_results, "expected results from loading by function name")

        
    def test_lookup(self):
        import gawseed.analysis.relationshipAnalysis
        import gawseed.algorithm.relationship

        ra = RelationshipAnalysis(
                                  output_columns = 
                                  { 'col1': { 'function': 'gawseed.algorithm.relationship.fraction',
                                              'arguments': [ 'data1', 'data2' ] } }
                                  )
        self.assertTrue(ra)

        input_data = [
            ['60',  'data1', 'k1', 's1', 10.0],
            ['60',  'data1', 'k2', 's2', 10.0],
            ['60',  'data2', 'k1', 's1', 10.0],
            ['60',  'data2', 'k2', 's2', 20.0],

            ['120', 'data1', 'k1', 's1', 10.0],
            ['120', 'data1', 'k2', 's2', 20.0],
            ['120', 'data2', 'k1', 's1', 10.0],
            ['120', 'data2', 'k2', 's2', 10.0],
        ]

        expected_results = [
            # T, Key, subkey, data1/data2
            ['60',  'k1', 's1', 1.0],
            ['60',  'k2', 's2', 0.5],
            ['120', 'k1', 's1', 1.0],
            ['120', 'k2', 's2', 2.0],
        ]

        results = []
        for output in ra.process(input_data):
            results.append(output)

        self.assertEqual(results, expected_results, "expected results from loading by function name")
        
    def test_relatioship_max(self):
        import gawseed.analysis.relationshipAnalysis
        import gawseed.algorithm.relationship

        ra = RelationshipAnalysis(
                                  output_columns = 
                                  { 'col1': { 'function': 'gawseed.algorithm.relationship.value_max',
                                              'arguments': [ 'data1' ] } }
                                  )
        self.assertTrue(ra)

        input_data = [
            ['60',  'data1', 'k1', 's1', 10.0],
            ['60',  'data1', 'k2', 's2', 10.0],

            ['120', 'data1', 'k1', 's1', 40.0],
            ['120', 'data1', 'k2', 's2', 5.0],

            ['180', 'data1', 'k1', 's1', 10.0],
            ['180', 'data1', 'k2', 's2', 10.0],
        ]

        expected_results = [
            # T, Key, subkey, data1/data2
            ['60',  'k1', 's1', 10.0],
            ['60',  'k2', 's2', 10.0],
            ['120', 'k1', 's1', 40.0],
            ['120', 'k2', 's2', 10.0],
            ['180', 'k1', 's1', 40.0],
            ['180', 'k2', 's2', 10.0],
        ]

        results = []
        for output in ra.process(input_data):
            results.append(output)

        self.assertEqual(results, expected_results, "expected results from loading by function name")


if __name__ == '__main__':
    unittest.main()
    

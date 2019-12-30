#!/usr/bin/python3

import unittest
import sys

class algorithmGenericTests(unittest.TestCase):

    def test_init_featureCounter(self):
        import gawseed.algorithm.generic
        self.assertTrue(True)

    def test_generic_identity(self):
        from gawseed.algorithm.generic import identity
        input_data = [
            ['1'],
            [1],
            ['foobar']
        ]

        expected_data = []
        output_data = []
        for item in input_data:
            expected_data.append((item[0], ''))
            output_data.append(identity(item, [0]))

        self.assertEqual(expected_data, output_data, 'identity should not change values')

    def test_double_identity(self):
        from gawseed.algorithm.generic import double_identity
        input_data = [
            ['1','2'],
            [1,2],
            ['foobar','baz']
        ]

        expected_data = []
        output_data = []
        for item in input_data:
            expected_data.append((item[0], item[1]))
            output_data.append(double_identity(item, [0,1]))

        self.assertEqual(expected_data, output_data, 'identity should not change values')

    def test_re_match_one(self):
        from gawseed.algorithm.generic import re_match_one
        input_data = [
            ['apple', 'bananana'],
            ['orange', 'pear'],
            ['strawberry', 'blueberry']
        ]
        re_column = 1
        re_argument = '(e.*r)'

        expected_data = [['',''], ['ear', ''], ['eberr', '']]
        output_data = []

        for item in input_data:
            output_data.append(re_match_one(item, [re_column, re_argument]))
        self.assertEqual(expected_data, output_data, 're matcher worked')

    def test_max_values(self):
        from gawseed.algorithm.generic import value_max

        input_data = [
            [0, 10, 5],
            [0, 5, 6],
            [10, 20, 4],
            [20, 30, 3]
        ]

        expected_data = [[10, 5],
                         [10, 6],
                         [20, 6],
                         [30, 6]]
        output_data = []

        for item in input_data:
            out = []
            for col in [1,2]:
                out.append(value_max(item, [col]))
            output_data.append(out)

        self.assertEqual(expected_data, output_data,
                         "max functions work for selecting max values over time")

    #
    # value function tests
    #
    def test_one(self):
        from gawseed.algorithm.generic import one
        self.assertEqual(one([]), 1, 'one([]) returns 1')
        self.assertEqual(one(range(1,9)), 1, 'one(range) returns 1')
        self.assertEqual(one([],[9]), 1, 'one([],[9]) returns 1')
        self.assertEqual(one([1],9), 1, 'one([1],9) returns 1')

    def test_column_value(self):
        from gawseed.algorithm.generic import column_value
        input_data = [
            ['1', 42],
            [1, 10],
            ['foobar', 5]
        ]

        expected_data = []
        output_data = []
        for item in input_data:
            expected_data.append(item[1])
            output_data.append(column_value(item, [1]))

        self.assertEqual(expected_data, output_data, 'column_value should return the column value')
        
    def test_fraction(self):
        from gawseed.algorithm.generic import fraction
        input_data = [
            [1, 1],
            [1, 2],
            [4, 1],
        ]

        expected_data = [1.0, 0.5, 4.0]
        output_data = []

        for item in input_data:
            output_data.append(fraction(item, [0, 1]))

        self.assertEqual(expected_data, output_data, 'fraction should divide data from multiple cols')
        
if __name__ == '__main__':
    unittest.main()
    

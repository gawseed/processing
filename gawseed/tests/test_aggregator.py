#!/usr/bin/python3

import unittest

class aggregatorTests(unittest.TestCase):

    def test_init_aggregator(self):
        import gawseed.analysis.aggregator

        ag = gawseed.analysis.aggregator.Aggregator([],[], [])
        self.assertTrue(ag)

        ag = gawseed.analysis.aggregator.Aggregator([],[])
        self.assertTrue(ag)

        ag = gawseed.analysis.aggregator.Aggregator([])
        self.assertTrue(ag)

        ag = gawseed.analysis.aggregator.Aggregator()
        self.assertTrue(ag)

    def test_def_sort_fields(self):
        from gawseed.analysis.aggregator import Aggregator

        ag = Aggregator()

        self.assertEqual(ag.sorted_fields,
                         [0], "Time is the only sorted field")
        self.assertEqual(ag._sorted_len, 1,
                         "Length of sorted keys is correct")
        self.assertEqual(ag.non_sorted_fields,
                         [1, 2, 3],
                         "The default non_sorted keys are correct")
        self.assertEqual(ag._non_sorted_len, 3,
                         "Length of non sorted keys is correct")

    def _do_aggregator_tests(self, ag, prefix,
                             data = [['60', 'i', 'k', 's', '32'],
                                     ['60', 'i', 'k2', 's', '10'],
                                     ['60', 'i', 'k', 's', '10'],
                                     ['120', 'i', 'k', 's', '100'],
                             ],
                             expected_results = [['60', 'i', 'k', 's', 42.0],
                                                 ['60', 'i', 'k2', 's', 10.0],
                                                 ['120', 'i', 'k', 's', 100.0]]):

        
        results = []
        for row in ag.process(data):
            results.append(row)

        self.assertEqual(expected_results, results, 
                         prefix + ": Output of aggregator.process() seems correct")

    def test_fast_aggregrate_data(self):
        from gawseed.analysis.aggregator import FastAggregator

        self._do_aggregator_tests(FastAggregator(), "fast")

    def test_aggregate_data(self):
        from gawseed.analysis.aggregator import Aggregator

        self._do_aggregator_tests(Aggregator(), "slow")

        ag = Aggregator(sorted_fields = [0, 1],
                        non_sorted_fields = [2,3,4,5],
                        value_columns = [6, 7])
        
        self._do_aggregator_tests(ag, "slow",
                                  data = [['60', '30', 'i', 'k', 's', 't', '32', '3.2'],
                                          ['60', '30', 'i', 'k2', 's', 't', '10', '1.0'],
                                          ['60', '30', 'i', 'k', 's', 't', '10', '1'],
                                          ['120', '30', 'i', 'k', 's', 't', '100', '10.0'],
                                  ],
                                  expected_results = [['60', '30', 'i', 'k', 's', 't', 42.0, 4.2],
                                                      ['60', '30', 'i', 'k2', 's', 't', 10.0, 1.0],
                                                      ['120', '30', 'i', 'k', 's', 't', 100.0, 10.0]])
                                  
    def test_aggregate_unique_counts(self):
        from gawseed.analysis.aggregator import FastAggregator
        from gawseed.algorithm.aggregator import sumAndCountUnique

        self._do_aggregator_tests(FastAggregator(aggregators = [sumAndCountUnique]), "fast-unique",
                                  data = [['60', 'i', 'k', 's', '32'],
                                          ['60', 'i', 'k2', 's', '10'],
                                          ['60', 'i', 'k', 's', '10'],
                                          ['60', 'i', 'k', 's2', '10'],
                                          ['120', 'i', 'k', 's', '100'],
                                  ],
                                  expected_results = [['60', 'i', 'k', 's', 42.0],
                                                      ['60', 'i', 'k', 's2', 10.0],
                                                      ['60', 'i', 'k2', 's', 10.0],
                                                      ['60', 'i_unique', 'k', 'unique', 2],
                                                      ['60', 'i_unique', 'k2', 'unique', 1],
                                                      ['120', 'i', 'k', 's', 100.0],
                                                      ['120', 'i_unique', 'k', 'unique', 1]
                                  ])

    def test_aggregate_unique_counts_with_label(self):
        from gawseed.analysis.aggregator import FastAggregator
        from gawseed.algorithm.aggregator import sumAndCountUnique

        self._do_aggregator_tests(FastAggregator(aggregators = ['gawseed.algorithm.aggregator.sumAndCountUnique:uniques']), "fast-unique",
                                  data = [['60', 'i', 'k', 's', '32'],
                                          ['60', 'i', 'k2', 's', '10'],
                                          ['60', 'i', 'k', 's', '10'],
                                          ['60', 'i', 'k', 's2', '10'],
                                          ['120', 'i', 'k', 's', '100'],
                                  ],
                                  expected_results = [['60', 'i', 'k', 's', 42.0],
                                                      ['60', 'i', 'k', 's2', 10.0],
                                                      ['60', 'i', 'k2', 's', 10.0],
                                                      ['60', 'uniques', 'k', 'unique', 2],
                                                      ['60', 'uniques', 'k2', 'unique', 1],
                                                      ['120', 'i', 'k', 's', 100.0],
                                                      ['120', 'uniques', 'k', 'unique', 1]
                                  ])

    def test_aggregate_unique_counts_with_label_yaml(self):
        from gawseed.analysis.aggregator import FastAggregator
        from gawseed.algorithm.aggregator import sumAndCountUnique

        yaml_specification = """
aggregator:
  aggregators:
    - function: gawseed.algorithm.aggregator.sumAndCountUnique
      arguments: ["uniques"]
"""

        self._do_aggregator_tests(FastAggregator(yaml_specification = yaml_specification), "fast-unique",
                                  data = [['60', 'i', 'k', 's', '32'],
                                          ['60', 'i', 'k2', 's', '10'],
                                          ['60', 'i', 'k', 's', '10'],
                                          ['60', 'i', 'k', 's2', '10'],
                                          ['120', 'i', 'k', 's', '100'],
                                  ],
                                  expected_results = [['60', 'i', 'k', 's', 42.0],
                                                      ['60', 'i', 'k', 's2', 10.0],
                                                      ['60', 'i', 'k2', 's', 10.0],
                                                      ['60', 'uniques', 'k', 'unique', 2],
                                                      ['60', 'uniques', 'k2', 'unique', 1],
                                                      ['120', 'i', 'k', 's', 100.0],
                                                      ['120', 'uniques', 'k', 'unique', 1]
                                  ])

    def test_aggregate_max_yaml(self):
        from gawseed.analysis.aggregator import FastAggregator
        from gawseed.algorithm.aggregator import sumAndCountUnique

        yaml_specification = """
aggregator:
  aggregators:
    - function: gawseed.algorithm.aggregator.value_max
      arguments: ["max"]
"""

        self._do_aggregator_tests(FastAggregator(yaml_specification = yaml_specification), "fast-unique",
                                  data = [['60', 'i', 'k', 's', '32'],
                                          ['60', 'i', 'k2', 's', '10'],
                                          ['60', 'i', 'k', 's', '10'],
                                          ['60', 'i', 'k', 's2', '10'],
                                          ['120', 'i', 'k', 's', '100'],
                                  ],
                                  expected_results = [['60', 'max', 'k', 's', 32.0],
                                                      ['60', 'max', 'k', 's2', 10.0],
                                                      ['60', 'max', 'k2', 's', 10.0],
                                                      ['120', 'max', 'k', 's', 100.0],
                                  ])

    def test_aggregate_unique_single(self):
        from gawseed.analysis.aggregator import FastAggregator
        from gawseed.algorithm.aggregator import sumAndCountUnique

        yaml_specification = """
aggregator:
  aggregators:
    - function: gawseed.algorithm.aggregator.summer
      arguments: []
    - function: gawseed.algorithm.aggregator.unique
      arguments: ['i2', 'un']

"""

        self._do_aggregator_tests(FastAggregator(yaml_specification = yaml_specification), "fast-unique",
                                  data = [['60', 'i', 'k', 's', '32'],
                                          ['60', 'i', 'k2', 's', '10'],
                                          ['60', 'i2', 'k', 's', '10'],
                                          ['60', 'i2', 'k', 's2', '10'],
                                  ],
                                  expected_results = [['60', 'i', 'k', 's', 32.0],
                                                      ['60', 'i', 'k2', 's', 10.0],
                                                      ['60', 'i2', 'k', 's', 10.0],
                                                      ['60', 'i2', 'k', 's2', 10.0],
                                                      ['60', 'un', 'k', 'un', 2],
                                                      
                                  ])


    def test_aggregate_unique_with_double_key(self):
        # tests an issue with state keeping in aggregrator.py
        from gawseed.analysis.aggregator import FastAggregator
        from gawseed.algorithm.aggregator import sumAndCountUnique

        yaml_specification = """
aggregator:
  aggregators:
    - function: gawseed.algorithm.aggregator.summer
      arguments: []
    - function: gawseed.algorithm.aggregator.unique
      arguments: ['i1', 'un1']
    - function: gawseed.algorithm.aggregator.unique
      arguments: ['i2', 'un2']

"""

        print("-----------------")
        data = [['60', 'i1', 'k', 's', '32'],
                ['60', 'i2', 'k', 's', '10'],
                ['120', 'i1', 'k2', 's', '10'],
                ['120', 'i2', 'k2', 's2', '20'],
        ]
        expected_results = [['60', 'i1', 'k', 's', 32.0],
                            ['60', 'un1', 'k', 'un1', 1],
                            ['60', 'i2', 'k', 's', 10.0],
                            ['60', 'un2', 'k', 'un2', 1],
                            ['120', 'i1', 'k2', 's', 10.0],
                            ['120', 'un1', 'k2', 'un1', 1],
                            ['120', 'i2', 'k2', 's2', 20.0],
                            ['120', 'un2', 'k2', 'un2', 1],
        ]

#        import pdb ; pdb.set_trace()
        self._do_aggregator_tests(FastAggregator(yaml_specification = yaml_specification), "fast-unique", data = data, expected_results = expected_results)

    def test_aggregate_string_splitter(self):
        from gawseed.analysis.aggregator import FastAggregator
        from gawseed.algorithm.aggregator import stringSplitter
        import collections
        
        strings = ['hello world, this is the earth calling',
                   'hello mars, earth is calling']
        data = []
        for item in strings:
            data.append(['60', 'i', item, '', 1.0])

        # calculate what we expect from the more complex system
        counts = collections.Counter()
        for row in data:
            words = row[2].split()
            for word in words:
                counts[word] += 1

        expected_results = []
        for key in counts:
            expected_results.append(['60', 'substrings', key,
                                     '', float(counts[key])])


        yaml_specification = """
aggregator:
  aggregators:
    - function: gawseed.algorithm.aggregator.stringSplitter
      arguments: ['substrings']

"""

        self._do_aggregator_tests(FastAggregator(yaml_specification = yaml_specification), "fast-unique",
                                  data, expected_results)


    def test_aggregate_just_unique_single(self):
        from gawseed.analysis.aggregator import FastAggregator
        from gawseed.algorithm.aggregator import sumAndCountUnique

        yaml_specification = """
aggregator:
  aggregators:
    - function: gawseed.algorithm.aggregator.unique
      arguments: ['i2', 'un']

"""

        self._do_aggregator_tests(FastAggregator(yaml_specification = yaml_specification), "fast-unique",
                                  data = [['60', 'i', 'k', 's', '32'],
                                          ['60', 'i', 'k2', 's', '10'],
                                          ['60', 'i2', 'k', 's', '10'],
                                          ['60', 'i2', 'k', 's2', '10'],
                                  ],
                                  expected_results = [['60', 'un', 'k', 'un', 2],
                                                      
                                  ])


    def test_load_function(self):
        from gawseed.analysis.aggregator import FastAggregator

        self._do_aggregator_tests(FastAggregator(aggregators = ['gawseed.analysis.aggregator.summer']), "loader")


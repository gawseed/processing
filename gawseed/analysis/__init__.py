#!/usr/bin/python3

import yaml

class Analysis(object):
    _bin_size = None
    _time_column = None
    _specification = None

    def __init__(self, time_column = None, bin_size = None, yaml_specification = None):
        self._time_column = time_column
        self._bin_size = bin_size

        if yaml_specification:
            try:
                self._specification = yaml.load(yaml_specification,
                                                Loader=yaml.FullLoader)
            except:
                self._specification = yaml.load(yaml_specification)
            if self._time_column is None and 'timeColumn' in self._specification:
                self._time_column = self._specification['timeColumn']
            if self._bin_size is None and 'binSize' in self._specification:
                self._bin_size = self._specification['binSize']

        # set the last resort bin_size
        if not self._bin_size:
            self._bin_size = 60

        # set the last resort bin_size
        if not self._time_column:
            self._time_column = 0

    def timebin(self, timeval):
        timeval = int(float(timeval))
        return timeval - (timeval % self._bin_size)

    def process(input_stream):
        pass

    @property
    def time_column(self):
        return self._time_column

    @time_column.setter
    def time_column(self, val):
        self._time_column = val
            
    def convert_argument_specifier(self, arg, fsdbh = None):
        if type(arg) == str:
            if arg[:7] == "column(":
                col_name = arg[7:-1]
                col_number = fsdbh.get_column_number(col_name)
                return (col_name, col_number)
            elif arg[:4] == "col(":
                col_name = arg[4:-1]
                col_number = fsdbh.get_column_number(col_name)
                return (col_name, col_number)
            elif arg[:4] == "int(":
                col_name = "int_" + arg[4:-1]
                col_number = int(arg[4:-1])
                return (col_name, col_number)
            elif arg[:4] == "str(":
                col_name = "str_" + arg[4:-1]
                col_number = str(arg[4:-1])
                return (col_name, col_number)
            elif arg[:7] == "string(":
                col_name = "string_" + arg[7:-1]
                col_number = str(arg[7:-1])
                return (col_name, col_number)
            elif arg[:6] == "float(":
                col_name = "float_" + arg[6:-1]
                col_number = float(arg[6:-1])
                return (col_name, col_number)
            else:
                try: # to be intelligent
                    col_number = int(arg)
                    if col_number > 0: # this fails for 0
                        col_name = "_int" + col_number
                        return (col_name, col_number)
                except:
                    pass

                try: # to see if it's a column number
                    col_number = fsdbh.get_column_number(arg)
                    return (arg, col_number)
                except:
                    pass

                return (arg, None)
        else:
            return (arg, None)
                

    def convert_argument_specifiers(self, function_list, fsdbh = None):
        """Given a list of function arguments, convert each of the items given
           a possible mapping function of column(), col(), int() or float()."""
        col_numbers = []
        for arg in function_list:
            (col_name, col_number) = self.convert_argument_specifier(arg, fsdbh)
            col_numbers.append(col_number)
        return col_numbers
    

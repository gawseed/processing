import gawseed.analysis
import gawseed.averageWindow
import collections

class EdgeDetect(gawseed.analysis.Analysis):
    def __init__(self, yaml_specification=None):
        self._debug_output = False
        self._analyze_column_list = []
        self._analyze_columns = {}
        super().__init__(None, None, yaml_specification)
    
    def debug(self, it):
        if self._debug_output:
            print("#+ " + str(it))

    def get_value(self, specified, name, specification, default = None):
        if specified:
            return specified
        if name in specification:
            return specification[name]
        return default

    def set_parameters(self, analyze_column_list=None,
                       bin_size=None, time_column=None, key_column=None,
                       zero_jump=None, scale_height=None,
                       window_size=None, middle_size=None, 
                       sort_first=False, min_value=None, input_fsdb=None):
        yaml_specification = self._specification or collections.defaultdict(lambda: None)

        if yaml_specification and 'edgeDetect' in self._specification:
            edge_specification = yaml_specification['edgeDetect']
        else:
            edge_specification = collections.defaultdict(lambda: None)

        # convert the array to a dict for easier later lookup
        self._column_specifications = {}
        if 'analyzeColumns' in edge_specification:
            for row in edge_specification['analyzeColumns']:
                if 'name' in row:
                    self._column_specifications[row['name']] = row
            

        # set parameters ()
        if bin_size:
            self._bin_size = bin_size
        if time_column:
            self._time_column = time_column

        # init the details structure
        if not self._analyze_columns:
            self._analyze_columns = {}

        # find the default list of columns to analyze, possibly
        # from the:
        # - passed in ilst
        # - failing that, the yaml specifications
        self._analyze_column_list = self.get_value(analyze_column_list, 'analyzeColumns', edge_specification)

        # if it's a dict (yaml) then convert it to a separate list/dict
        if type(self._analyze_column_list[0]) is dict:
            # convert the list to a dict
            for item in self._analyze_column_list:
                self._analyze_columns[item['name']] = item
            # now extract a real list from the names
            self._analyze_column_list = [x['name'] for x in self._analyze_column_list]
            

        self._key_column = self.get_value(key_column, 'keyColumn', edge_specification)
        self._zero_jump = self.get_value(zero_jump, 'zeroJump', edge_specification)
        self._scale_height = self.get_value(scale_height, 'scaleHeight', edge_specification)
        self._window_size = self.get_value(window_size, 'windowSize', edge_specification)
        self._middle_size = self.get_value(middle_size, 'middleSize', edge_specification)

        # calculate the event time offset
        self._event_offset = (self._window_size * 2 + self._middle_size - 1) * self._bin_size

        # need to multiply by the window size
        self._min_value = self.get_value(min_value, 'minValue', edge_specification, 0.0)

        analyze_column_list = self.set_column_parameters(analyze_column_list,
                                                         input_fsdb)

        return analyze_column_list

    def setup_column_spec(self, column_name, column_number, original_name=None):
        if column_name in self._analyze_columns:
            # it's already up to date.  Just ensure set the number is set
            self._analyze_columns[column_name]['column'] = column_number
        elif original_name and original_name in self._analyze_columns:
            # change the name (and create a double reference just in case)
            self._analyze_columns[original_name]['column'] = column_number
            self._analyze_columns[original_name]['name'] = column_name
            self._analyze_columns[column_name] = self._analyze_columns[original_name]
        else:
            # it doesn't exist at all; create it
            self._analyze_columns[column_name] = { 'name': column_name,
                                                   'column': column_number }

        return self._analyze_columns[column_name]

    def set_column_parameters(self, analyze_column_list=None, input_fsdb=None):
        if not analyze_column_list or len(analyze_column_list) <= 0:
            analyze_column_list = self._analyze_column_list
        self._analyze_column_list = analyze_column_list

        new_list = []
        for column in analyze_column_list:
            original_name = column
            if type(column) == str:
                (column, col_number) = self.convert_argument_specifier(column, input_fsdb)
                column_info = self.setup_column_spec(column, col_number, original_name)
            elif type(column) == int:
                column_info = self.setup_column_spec(str(column), column, None)
                column = str(column)
            elif type(column) == dict:
                # XXX: think this is obsolete; delete and run tests
                column_info = column
                (column, col_number) = self.convert_argument_specifier(column_info['name'], input_fsdb)
                column_info = self.setup_column_spec(column, col_number, original_name)
                

            # check for missing parameters and set from defaults
            if 'zeroJump' not in column_info:
                column_info['zeroJump'] = self._zero_jump
            if 'scaleHeight' not in column_info:
                column_info['scaleHeight'] = self._scale_height
            if 'minValue' not in column_info:
                column_info['minValue'] = self._min_value
            # need to multiply by the window size since
            # the averaging window returns window totals
            column_info['minValue'] *= self._window_size

            # attempt to get the column number, if needed
            if 'column' not in column_info or not column_info['column']:
                try:
                    column_info['column'] = input_fsdb.get_column_number(column)
                except:
                    pass

            # store the whole config away for later
            self._analyze_columns[column] = column_info
            new_list.append(column)

        self._analyze_column_list = new_list
        return new_list

    def edge_detect(self, f, outf, analyze_column_list=None,
                    bin_size=None, time_column=None, key_col=None,
                    zero_jump=None, scale_height=None,
                    window_size=None, middle_size=None, 
                    sort_first=False):

        analyze_column_list = self.set_parameters(analyze_column_list,
                                                  bin_size, time_column, key_col,
                                                  zero_jump, scale_height,
                                                  window_size, middle_size, 
                                                  sort_first, input_fsdb = f)

        # convert column names to numbers,
        # and create output columns
        analyze_cols = []
        output_columns = []
        for analyze in analyze_column_list:
            if type(analyze) is dict:
                import pdb ; pdb.set_trace()
            column_info = self._analyze_columns[analyze]
            analyze_cols.append(column_info['column'])
            
            output_columns.append(analyze + "_lhs")
            output_columns.append(analyze + "_rhs")
            output_columns.append(analyze + "_scale")
            output_columns.append(analyze + "_delta")

        self._time_column_name = ""
        self._key_column_name = ""
        try:
            outf.out_separator = f.separator # duplicate the style
        except:
            # basically it wasn't an fsdb object, so we assume it's
            # column intelligent in some other way
            pass
        
        (self._time_column_name, self._time_column) = self.convert_argument_specifier(self._time_column, f)
        (self._key_column_name, self._key_column) = self.convert_argument_specifier(self._key_column, f)

        outf.out_column_names = [self._time_column_name, self._key_column_name] + output_columns

        self.edge_detect_loop(f, outf, analyze_column_list)

    def edge_detect_loop(self, f, outf, analyze_column_list,
                         sort_first=False):
        
        if not analyze_column_list or len(analyze_column_list) <= 0:
            analyze_column_list = self._analyze_column_list
        else:
            self._analyze_column_list = analyze_column_list
            
        # if sorting is required, do so first
        if sort_first:
            out = []
            for row in f:
                out.append(row)

            def get_time_col(a):
                return a[self._time_column]

            out.sort(key=get_time_col)

            # replace our fsdb accessor with the new sorted array
            f = out

        # storage for most recently seen data
        data_seen = {}
        # storage dictionary to store window data in
        windows = {}
        # the initial timestamp that we can even begin running edge detection
        first_analysis_time = None
        # min time before we start event signals
        # based on the beginning data, None until then
        min_time = None
        last_time = None

        # loop over all data
        for row in f:
            key = row[self._key_column]
            time = int(row[self._time_column])
            values = {}
            for column in analyze_column_list:
                column_info = self._analyze_columns[column]
                if len(row) > column_info['column'] and row[column_info['column']] is not None and row[column_info['column']] is not "":
                    values[column] = float(row[column_info['column']])
            
            if not first_analysis_time:
                first_analysis_time = time + self._bin_size * (self._window_size * 2 + self._middle_size)
                last_time = time
                next

            if last_time != time:
                # add missing zeros
                self.debug("#+ data_seen: " + str(data_seen))
                for wkey in windows:
                    for column in analyze_column_list:
                        column_info = self._analyze_columns[column]
                        if wkey not in data_seen or column not in data_seen[wkey]:
                            self.debug("#+ missing data: %s/%s" % (wkey,column))
                            windows[wkey][column].add_element(0.0)
                        else:
                            self.debug("#+ had data: %s/%s" % (wkey,column))
                data_seen = {}

                # for anything in the bin window
                # add zeros for (time - last_time)/bin slots
                self.debug("#jumping " + str(last_time) + " -> " + str(time))
                last_time += self._bin_size
                if (last_time > time):
                    raise ValueError("bin size must be incorrect; jumped too far: new last_time=%d > time=%d; -- maybe incoming data was not sorted by time???" % (last_time, time))
                while last_time < time:
                    self.debug("catching up " + str(last_time))
                    for wkey in windows:
                        for column in windows[wkey]:
                            windows[wkey][column].add_element(0.0)
                    # XXX: we should analyze here to avoid missing downspikes
                    last_time += self._bin_size

                # don't analyze too early
                if time >= first_analysis_time:
                    self.analyze_windows(time, windows,
                                         outf)

            if key not in windows:
                # need to play catchup
                #print("creating key " + key)
                windows[key] = {}
                for column in analyze_column_list:
                    column_info = self._analyze_columns[column]
                    windows[key][column] = averageWindow.AverageWindow(self._window_size, self._middle_size)

            for column in analyze_column_list:
                column_info = self._analyze_columns[column]
                if column in values:
                    windows[key][column].add_element(values[column])
                else:
                    windows[key][column].add_element(0.0)
                if key not in data_seen:
                    data_seen[key] = {}
                data_seen[key][column] = 1
                #self.debug("#+ %s:\t%f\t%f\t%f" % (key, values[column['name']], windows[key][column['name']].get_lhs(), windows[key][column['name']].get_rhs()))

                
    def analyze_windows(self, time, windows, outf,
                        analyze_column_list=None):
        MIN_INTERESTING = .000001 # allow setting of this

        if not analyze_column_list or len(analyze_column_list) <= 0:
            analyze_column_list = self._analyze_column_list

        for aw_name in windows:
            print_all = False # self.debugging
            any_event = False
            results = {}
            for column in analyze_column_list:
                column_info = self._analyze_columns[column]
                aw = windows[aw_name][column]
                self.debug("analyzing key=%s column=%s t=%d" % (aw_name, column, time))
                lhs = aw.get_lhs()
                rhs = aw.get_rhs()
                colstr = str(column)

                results[colstr + '_scale_event'] = 0
                results[colstr + '_delta_event'] = 0
                min_value = column_info['minValue']

                if column_info['zeroJump']:
                    zero_jump = column_info['zeroJump']
                    # deal with the left hand average being zero(ish) (rising)
                    if rhs-lhs > zero_jump and rhs >= min_value:
                        #output.write("# zjr\n")
                        results[colstr + '_delta_event'] = rhs-lhs
                        any_event = True

                    # deal with the right hand average being zero(ish) (falling)
                    elif lhs-rhs > zero_jump and lhs >= min_value:
                        #output.write("# zjl\n")
                        results[colstr + '_delta_event'] = rhs-lhs
                        any_event = True

                if column_info['scaleHeight']:
                    scale_height = column_info['scaleHeight']
                    # deal with a scale change upward
                    if lhs > MIN_INTERESTING and rhs >= min_value and float(rhs)/float(lhs) > scale_height:
                        #output.write("# sr " + str(float(rhs)/float(lhs)) + "\n")
                        results[colstr + '_scale_event'] = float(rhs)/float(lhs)
                        any_event = True

                    # deal with a scale change downward
                    elif rhs > MIN_INTERESTING and lhs >= min_value and float(lhs)/float(rhs) > scale_height:
                        #output.write("# sl\n")
                        results[colstr + '_scale_event'] = -float(lhs)/float(rhs)
                        any_event = True

            if any_event or print_all:
                # calculate a time stamp not based on the current time,
                # but based on current - window size adjustments
                real_time = time - self._event_offset
                row = [str(real_time), aw_name]

                # eventually handle multiple keys
                for column in windows[aw_name]:
                    colstr = str(column)
                    aw = windows[aw_name][column]

                    row = row + [str(aw.get_lhs()),
                                 str(aw.get_rhs()),
                                 str(results[colstr + '_scale_event']),
                                 str(results[colstr + '_delta_event'])]

                outf.append(row)

    def map(self, input_data, output_stream,
            column_names=None, column_numbers=None,
            timestamp_name='timestamp', timestampcol=0,
            key_name='key', subkey_name='subkey', keycol=1, separator='-'):

        # if column names weren't passed, assume it's FSDB
        # based and extract the names from its header
        if not column_names:
            init_column_names = input_data.column_names
            column_names = []
            for name in init_column_names:
                if name not in [timestamp_name, key_name, subkey_name]:
                    column_names.append(name)
            column_numbers = input_data.get_column_numbers(column_names)

        for row in input_data:
            for (column_name, column_num) in zip(column_names, column_numbers):
                if column_num < len(row) and row[column_num] != '':
                    newrow = [
                        column_name + separator + row[keycol],
                        row[timestampcol],
                        row[column_num]
                    ]
                    output_stream.append(newrow)
        
    def reduce(self, input_data, key_column=0, timestamp_column=1,
               value_column=2):
        current_keyval = None
        current_values = []
        for row in input_data:
            if current_keyval and row[key_column] != current_keyval:
                yield current_values
                current_values = []
                current_keyval = row[key_column]

            # XXX: sometimes in hadoop this fails,
            # maybe because data is missing/gone?
            try:
                current_values.append([int(row[timestamp_column]),
                                       row[key_column],
                                       row[value_column]])
                current_keyval = row[key_column]
            except:
                pass

        yield current_values
                
    def reducer_edge_detect(self, inf, outf,
                            bin_size=None, timestamp_column='timestamp',
                            key_column='key',
                            zero_jump=None, scale_height=None,
                            window_size=None, middle_size=None, 
                            sort_first=False, value_column='value'):

        # get column numbers if needed

        # set up initial specifiers (with just the 'value' column)
        self.set_parameters([value_column],
                            bin_size, timestamp_column, key_column,
                            zero_jump, scale_height,
                            window_size, middle_size, 
                            sort_first, input_fsdb=inf)

        try:
            outf.out_column_names = ['timestamp', 'key', 'lhs',
                                     'rhs', 'scale', 'zero']
        except:
            pass # guess it wasn't an fsdb object

        # XXX: move this to set_parameters?
        (self._time_column_name, self._time_column) = self.convert_argument_specifier(self._time_column, inf)
        (self._key_column_name, self._key_column) = self.convert_argument_specifier(self._key_column, inf)

        # Start by running the reducer on it to get windows of data to process
        for reduced_rows in self.reduce(inf):
            # get the column name from the data
            if len(reduced_rows) <= 0:
                continue # should never happen

            # run edge detection on the existing grouped dataset
            # making sure to sort it by timestamp, as hadoop/reduce
            # doesn't sort by a second key

            # we need to change the 'value' column details in the
            # analyze_columns specifications built generically,
            # to match the potentially specific specifications
            # 
            first_row = reduced_rows[0]
            comma_index = first_row[self._key_column].index("-")
            column_name = first_row[self._key_column][0:comma_index]
            # key_val = first_row[key_column][comma_index+1:]

            # copy the column parameters to the value column
            # that we're actually analyzing
            column_lookup = 'col(' + column_name + ')' # XXX: Ick, this is wrong

            # reset to defaults
            self._analyze_columns[value_column]['zeroJump'] = self._zero_jump
            self._analyze_columns[value_column]['scaleHeight'] = self._scale_height
            self._analyze_columns[value_column]['minValue'] = self._min_value
            # over-ride with yaml specifics
            if column_lookup in self._column_specifications:
                col_spec = self._column_specifications[column_lookup]
                for item in ['zeroJump', 'scaleHeight', 'minValue']:
                    if item in col_spec:
                        self._analyze_columns[value_column][item] = col_spec[item]
                        
            self.edge_detect_loop(reduced_rows, outf,
                                  self._analyze_column_list,
                                  sort_first=True)
                            

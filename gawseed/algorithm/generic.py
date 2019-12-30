#
# key/subkey extraction functions
def identity(item, args):
    """Return the value from one column and an empty string.
       (ie, return a key and '' for no subkey).

       YAML usage:

       outputs:
         outcol:
           function: identity
           arguments: [KEYCOL]

       Where KEYCOL is the column name for the key to return.  The
       col() pseudo-function will convert the column name to a 0-based
       index offset to the row.
    """
    if len(item) <= args[0]:
        return ('','')
    return (item[args[0]], '')

def double_identity(item, cols):
    """Return the value from two columns.
       (ie, return a key and a subkey from two columns).

       YAML usage:

       outputs:
         outcol:
           function: double_identity
           arguments: [col(KEYCOL), col(SUBKEYCOL)]

       Where KEYCOL is the column name for the key to return and
       SUBKEYCOL is for the subkey.  The col() pseudo-function will
       convert the column name to a 0-based index offset to the row.
    """
    return (item[cols[0]], item[cols[1]])

def string(item, args = ['row_count']):
    return (args[0], '')

compiled_res = {}
def re_match_one(item, args):
    """Matches a regex with a group (argument 2) against the column (number in argument 1)"""
    import re

    # setup
    (re_col, re_expr) = args
    if re_expr not in compiled_res:
        compiled_res[re_expr] = re.compile(re_expr)

    # test if a match occurred
    match = compiled_res[re_expr].search(item[re_col])
    if match:
        return [match.group(1), '']
    return ['','']

#
# Value functions
def one(item, args = None):
    """Return the value 1.  This will typically have the effect of counting 
       the number of key or key,subkey instances seen.

       arguments: ignored

       YAML usage:

       outputs:
         outcol:
           function: identity
           arguments: [col(KEYCOL)]
           value:    one
    """

    return 1

def column_value(item, args):
    """Return the value stored in a column.

       YAML usage:

       outputs:
         outcol:
           function:         identity
           arguments:        [col(KEYCOL)]
           value:            column_value
           value_arguments:  [col(VALUECOL)]
    """
    return item[args[0]]

def column_value_int(item, args):
    """Return the value stored in a column cast to an int.

       YAML usage:

       outputs:
         outcol:
           function:         identity
           arguments:        [col(KEYCOL)]
           value:            column_value_int
           value_arguments:  [col(VALUECOL)]
    """
    try:
        return int(item[args[0]])
    except:
        return 0

def column_value_float(item, args):
    """Return the value stored in a column cast to a float.

       YAML usage:

       outputs:
         outcol:
           function:         identity
           arguments:        [col(KEYCOL)]
           value:            column_value_float
           value_arguments:  [col(VALUECOL)]
    """
    try:
        return float(item[args[0]])
    except:
        return 0.0

def fraction(item, args): # args = numerator_col, denominatior_col
    """Return the value stored in a column divided by the value in another

       YAML usage:

       outputs:
         outcol:
           function:         identity
           arguments:        [col(KEYCOL)]
           value:            fraction
           value_arguments:  [VALCOL_NUMERATOR, VALCOL_DENOMINATOR]
    """
    return float(item[args[0]]) / float(item[args[1]])

_max_data = {}
def value_max(item, args):
    """Returns the maximum value for a key or key,subkey within a
       timeframe.

       YAML usage:

       outputs:
         outcol:
           function:         identity
           arguments:        [col(KEYCOL)]
           value:            value_max
           value_arguments:  [VALCOL]
    """
    col = args[0]
    if col not in _max_data or _max_data[col] < item[col]:
        _max_data[col] = item[col]
    return _max_data[col]

#
# combining functions
#
def combine_summer(timebin, index, key, subkey, val1, val2):
    if val1:
        return val1 + val2
    return val2

def combine_value_max(timebin, index, key, subkey, val1, val2):
    if _max_data[key]:
        return _max_data[key]
    return None # shouldn't we always have data?


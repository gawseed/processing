import sys

# bundle: is all data at that timeslot (inside 'data' dict-entry)
# key, subkey
# current_slot is bundle[key][subkey] extracted already
# args = user passed arguments

def debug(bundle, key, subkey, current_slot, args):
    print("#+ " + str(current_slot))

def fraction(bundle, key, subkey, current_slot, args):
    "The float fraction of one column (arg[0]) divide by another (arg[1])"
    if args[0] in current_slot and args[1] in current_slot:
        return float(current_slot[args[0]]) / float(current_slot[args[1]])

def one(bundle, key, subkey, current_slot, args):
    return 1.0

def value(bundle, key, subkey, current_slot, args):
    if args[0] in current_slot:
        return current_slot[args[0]]

def lookup(bundle, key, subkey, current_slot, args):
    # key, subkey and current_slot are ignored; instead args specifies it:
    # args = [key-to-lookup, subkey-to-lookup, index]
    # return bundle[key][subkey][index]
    data = bundle['data']
    key = args[0]
    subkey = args[1]
    index = args[2]
    if key in data and subkey in data[key] and index in data[key][subkey]: 
        return(data[key][subkey][index])

def lookup_maybe_ra(bundle, key, subkey, current_slot, args):
    # key, subkey and current_slot are ignored; instead args specifies it:
    # args = [key-to-lookup, subkey-to-lookup, index]
    # return bundle[key][subkey][index]
    data = bundle['data']
    key = args[0]
    subkey = args[1]
    index = args[2]
    if key in data and subkey in data[key] and index in data[key][subkey]: 
        return(data[key][subkey][index])
    if key in data and index in data[key] and index in data[key][index]: 
        return(data[key][index][index])

def fraction_otherindex(bundle, key, subkey, current_slot, args):
    # args = [
    #   0: the current column (aka index) to use as the numerator
    #   1: the index of the denominator
    #   2: the key of the denominator
    #   3: the subkey of the denominator  ]
    numerator_index = args[0]
    if numerator_index in current_slot: # only process ones that are domains
        denominator_index = args[1]
        denominator_key = args[2]
        denominator_subkey = args[3]
        denominator = lookup(bundle, key, subkey, current_slot,
                             [denominator_key, denominator_subkey, denominator_index])
        if denominator and denominator != "0.0":
            return(float(current_slot[numerator_index]) / float(denominator))
    
def fraction_otherindex_keyval(bundle, key, subkey, current_slot, args):
    # keys = [
    #   0: the current column (aka index) to use as the numerator
    #   1: the index to pull the key value from for the denominator
    numerator_index = args[0]
    if numerator_index in current_slot: # only process ones that are domains
        denominator_index = args[1]
        key_in_other_index = key
        denominator = lookup(bundle, key, subkey, current_slot,
                             [key_in_other_index, '', denominator_index])
        if denominator and denominator != "0.0":
            return(float(current_slot[numerator_index]) / float(denominator))

_max_data = {}
def value_max(bundle, key, subkey, current_slot, args):
    col = args[0]

    if col not in current_slot:
        return None

    if key not in _max_data:
        _max_data[key] = { subkey: current_slot[col] }
        return current_slot[col]

    if subkey not in _max_data[key]:
        
        _max_data[key][subkey] = current_slot[col]
        return current_slot[col]

    _max_data[key][subkey] = max(_max_data[key][subkey], current_slot[col])
    return _max_data[key][subkey]
    

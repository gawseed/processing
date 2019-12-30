def summer(index, key, subkey, value, results, args = []):
    # index
    if index not in results:
        results[index] = {}

    # key
    if key not in results[index]:
        results[index][key] = {}

    # subkey - add it or sum it
    if subkey not in results[index][key]:
        results[index][key][subkey] = value
    else: 
        results[index][key][subkey] += value
    

def sumAndCountUnique(index, key, subkey, value, results, args = []):
    unique_index = None
    if len(args) > 0:
	    unique_index = args[0]
    else:
	    unique_index = index + "_unique" 

    # index
    if index not in results:
        results[index] = {}
        results[unique_index] = {}
        
    # key
    if key not in results[index]:
        results[index][key] = {}
        results[unique_index][key] = { 'unique': 0 }

    # subkey
    if subkey not in results[index][key]:
        results[index][key][subkey] = value
        results[unique_index][key]['unique'] += 1
    else: 
        results[index][key][subkey] += value

def stringSplitter(index, key, subkey, value, results, args = []):
    """Splits a value string into a series of new keys inside a new index
       (named in argument 1, or else old_index + '_split')"""

    # XXX: allow subkey to be specified other than ''
    subkey = ''

    split_index = None
    if len(args) > 0:
	    split_index = args[0]
    else:
	    split_index = index + "_split" 

    # create the index slots if they don't exist yet
    if split_index not in results:
        results[split_index] = {}
        
    # create new keys by splitting the old key
    # XXX: accept a split argument
    for newkey in key.split():
        # create the key slot if it doesn't exist
        if newkey not in results[split_index]:
            results[split_index][newkey] = { subkey: 1.0 }
        else:
            results[split_index][newkey][subkey] += 1

# unique:
#   Counts the number of unique subkey's within each key in a given index
#   
# arguments:
#   0: index string to match against
#   1: index string to store results in
def unique(index, key, subkey, value, results, args = []):
    matching_index = args[0]
    unique_index_string = args[1]

    if len(args) < 3:
        args.append({})

    storage = args[2]

    # only match a particular index value
    if matching_index != index:
        return

    # index
    if unique_index_string not in storage:
        storage[unique_index_string] = {}
    if unique_index_string not in results:
        results[unique_index_string] = {}
            
        
    # key
    if key not in storage[unique_index_string]:
        storage[unique_index_string][key] = { }
    if key not in results[unique_index_string]:
        results[unique_index_string][key] = { unique_index_string: 0 }

    # subkey
    if subkey not in storage[unique_index_string][key]:
        # XXX: really the summer should be listed before this
        # so that we don't have to force check something different
        # or we need temporary storage
        # XXX: could in theory store a temp hash in args[2]?
        storage[unique_index_string][key][subkey] = 1
        results[unique_index_string][key][unique_index_string] += 1
    
def value_max(index, key, subkey, value, results, args = []):
    unique_index = None
    if len(args) > 0:
	    unique_index = args[0]
    else:
	    unique_index = index + "_max" 

    # index
    if unique_index not in results:
        results[unique_index] = {}
        
    # key
    if key not in results[unique_index]:
        results[unique_index][key] = { }


    # subkey
    if subkey not in results[unique_index][key]:
        results[unique_index][key][subkey] = value
    else: 
        results[unique_index][key][subkey] = max(value, results[unique_index][key][subkey])
    

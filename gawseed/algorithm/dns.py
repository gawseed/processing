DNSSplitter = None

# The publicSuffixList module is used a lot in the following code
# The search_tree function returns a 3-variable array
# [prefix, suffix, public_point]

try:
    from dnssplitter import DNSSplitter
    from gawseed.algorithm.relationship import lookup, lookup_maybe_ra
except:
    import zipimport
    z = zipimport.zipimporter("gawseed-modules.mod")
    psl_mod = z.load_module("DNSSplitter")
    DNSSplitter = psl_mod.DNSSplitter

psl = DNSSplitter()

#
# public suffix list based featureCounter routines
#
def PSL_registration(row, cols):
    """Returns the registration point for a domain (.com for www.example.com)"""
    psl_data = psl.search_tree(row[cols[0]])
    if psl_data:
        return(psl_data[2], '')
    return (None, None)

def PSL_domain(row, cols):
    """Returns the domain point for a domain (example.com for www.example.com)"""
    psl_data = psl.search_tree(row[cols[0]])
    if psl_data:
        return(psl_data[1], '')
    return (None, None)

def PSL_prefix(row, cols):
    """Returns the prefix a domain (www.images for www.images.example.com)"""
    psl_data = psl.search_tree(row[cols[0]])
    if psl_data:
        return(psl_data[1], psl_data[0])
    return (None, None)

# returns key = registration point, subkey = domain
def PSL_reg_and_dom(row, cols):
    """Returns key=registration point, and subkey=domain name
       Example: returns ["com","example.com"] for www.example.com)"""
    psl_data = psl.search_tree(row[cols[0]])
    if psl_data:
        return(psl_data[2], psl_data[1])
    return (None, None)

# returns key = domain, subkey = prefix
def PSL_dom_and_pre(row, cols):
    """Returns key=domain, and subkey=prefix
       Example: returns ["exmaple.com","www"] for www.example.com)"""
    psl_data = psl.search_tree(row[cols[0]])
    if psl_data:
        return(psl_data[1], psl_data[0])
    return (None, None)

# returns key = domain, subkey = srcip
def PSL_dom_and_srcip(row, cols):
    """Returns key=domain, and subkey=srcip
       Example: returns ["exmaple.com","1.2.3.4"] for www.example.com from 1.2.3.4"""
    psl_data = psl.search_tree(row[cols[0]])
    if psl_data:
        return(psl_data[1], row[cols[1]])
    return (None, None)

# returns key = domain, subkey = srcip
def PSL_reg_and_srcip(row, cols):
    """Returns key=registration point, and subkey=srcip
       Example: returns ["com","1.2.3.4"] for www.example.com from 1.2.3.4"""
    psl_data = psl.search_tree(row[cols[0]])
    if psl_data:
        return(psl_data[2], row[cols[1]])
    return (None, None)

def PSL_registration_raw(name):
    psl_data = psl.search_tree(name)
    if psl_data:
        return psl_data[2]

def dns_label_count(rows, args):
    """Returns the number of labels in a given domain (eg: www.example.com = 3)"""
    label = rows[args[0]]
    parts = label.split(".")
    # deal with www.exmaple.com. with a trailing dot
    if parts[-1] == "":
        return (str(len(parts)-1),'')
    return (str(len(parts)),'')
    

#
# relationshipAnalysis functions
#

def dnsRegistrationFraction(bundle, key, subkey, thisone, args):
    # looks up the prefix of the key=domain given in a row with index=arg[0]
    # the row found will be in index=arg[1]
    # divides this by the value for this row, functionally
    # returning the fraction of the domain within its registration point
    # eg: count(example.com) / count(com)
    if args[0] in thisone: # only process ones that are domains
        reg_count = lookup(bundle, key, subkey, thisone,
                           [PSL_registration_raw(key), '', args[1]])
        if reg_count:
            return(float(thisone[args[0]]) / float(reg_count))

def dnsPrefixFraction(bundle, key, subkey, thisone, args):
    """Check the fraction of prefixes vs the base domain.
       Arguments should be [']"""
    if 'psldom' in thisone: # only process ones that are domains
        prefix_unique_count = lookup(bundle, key, subkey, thisone, [key, 'unique', 'pslpre_unique'])
        if prefix_unique_count:
            return(float(prefix_unique_count) / float(thisone['psldom']))

def dnsSrcipFraction(bundle, key, subkey, thisone, args):
    # args = ['srcipcount' index name]
    if args[0] in thisone: # only process ones that have the data we need 
        total_packets = lookup(bundle, key, subkey, thisone, ['2', '', 'allpackets'])
        if total_packets:
            return(float(thisone[args[0]]) / float(total_packets))
        
def dnsDDoSMetric(bundle, key, subkey, thisone, args):
    # args = [D=dnsname, domain_count_column, regpoint_count_column]
    # calculates relationshipAnalysis:
    # PP(cnt(PSL_D(D)) | cnt(PSL_S(D))) * SW(unique(PSL_P(D)) | cnt(PSL_D(D)))
    # = (count(PSL_D(D)) * unique(PSL_P(D))) / (count(PSL_S(D)) * count(PSL_D(D)))
    # = unique(PSL_P(D)) / count(PSL_S(D))
    psl_results = psl.search_tree(key)
    if not psl_results or psl_results[1] == '' or subkey != '':
        return None
    (prefix, domain, suffix) = psl_results

    # look up each of the individual math components
    domain_requests = lookup_maybe_ra(bundle, key, subkey, thisone, [suffix, '', args[0]])
    suffix_requests = lookup_maybe_ra(bundle, key, subkey, thisone, [suffix, '', args[1]])
    unique_prefixes = lookup_maybe_ra(bundle, key, subkey, thisone, [domain, '', args[2]])

    if float(suffix_requests) < int(args[3]):
        return None

    if not domain_requests or not suffix_requests or not unique_prefixes:
        return None
    
    return float(unique_prefixes)/float(suffix_requests)


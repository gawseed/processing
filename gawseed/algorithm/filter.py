import re

comps={}
def exclude_exprs(row, args):
    """Searches for matches against ROW column args[0] against any number
    of regexp matches in args[1:].  Uses re.search, which will match
    anywhere in a string.

    Returns FALSE on a match, to offer matches as an exclude list.
    """
    global comps
    for m in args[1:]:
        if m not in comps:
            comps[m] = re.compile(m)
        if comps[m].search(row[args[0]]):
            return False # inverse since we're excluding
    return True # True because nothing matched, so don't exclude

comps={}
def include_exprs(row, args):
    """Searches for matches against ROW column args[0] against any number
    of regexp matches in args[1:].  Uses re.search, which will match
    anywhere in a string.

    Returns TRUE on a match, to offer matches as an include list.
    """
    global comps
    for m in args[1:]:
        if m not in comps:
            comps[m] = re.compile(m)
        if comps[m].search(row[args[0]]):
            return True
    return False

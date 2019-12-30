# Valentino Crespi
# USC-ISI
# Project: GAWSEED
# extension of generic
#
#
from gawseed.algorithm.generic import compiled_res, re_match_one

compiled_res = {}
def re_match_both( item, args ):
    """Matches a regex with a group (argument 2) against the column (number in argument 1)"""
    import re

    # setup
    (re_col1, re_expr1, re_col2, re_expr2 ) = args
    if re_expr1 not in compiled_res:
        compiled_res[re_expr1] = re.compile(re_expr1)
    if re_expr2 not in compiled_res:
        compiled_res[re_expr2] = re.compile(re_expr2)

    # test if a match occurred
    match1 = compiled_res[re_expr1].search(item[re_col1])
    match2 = compiled_res[re_expr2].search(item[re_col2])
    if match1 and match2:
        return ["%s-%s" % (match1.group(1), match2.group(1)), '']
    return ['', '']

def re_match_both2( item, args ):
    """Matches a regex with a group (argument 2) against the column (number in argument 1)"""
    import re

    # setup
    (re_col1, re_expr1, re_col2, re_expr2 ) = args
    if re_expr1 not in compiled_res:
        compiled_res[re_expr1] = re.compile(re_expr1)
    if re_expr2 not in compiled_res:
        compiled_res[re_expr2] = re.compile(re_expr2)

    # test if a match occurred
    match1 = compiled_res[re_expr1].search(item[re_col1])
    match2 = compiled_res[re_expr2].search(item[re_col2])
    if match1 and match2:
        if match2.group( 1 ) == None and match2.group( 2 ) == None:
            return ['','']
        grp = "g1"
        if match2.group(1) == None:
            grp = "g2"
        return ["%s-%s" % (match1.group(1), grp ), '']
    return ['', '']

def re_match_one_rcnz( item, args ):
    """
        VC: selects only scrcp16 whose rcode or extended rcode is nonzero

        Input:
            x : col1
            y : regular expression
            r1 : col2
            r2 : col3

        Output
            [ re_match_one( x, y ), '' ] if item[ r1 ] > 0 or item[ r2 ] > 0

       YAML usage:

       outputs:
         outcol:
           function: re_match_one_rcnz
           arguments:
                - col( KEYCOL )
                - str( REGEXP )
                - col(SUBKEYCOL1)
                - col(SUBKEYCOL2)
    """

    ( x, y, r1, r2 ) = args

    if int( item[ r1 ] ) > 0 or int( item[ r1 ] ) > 0:
        [ xmy, dummy ] = re_match_one( item, list( ( x, y ) ) )
        return ( xmy, '' )
    else:
        return ( '', '' )

# compiled_res = {}
def re_match_two(item, args):
    """Matches a regex with a group (argument 2) against the column (number in argument 1)"""
    import re

    # setup
    (re_col, re_expr) = args
    if re_expr not in compiled_res:
        compiled_res[re_expr] = re.compile(re_expr)

    # test if a match occurred
    match = compiled_res[re_expr].search(item[re_col])
    if match:
        return [match.group(2), '']
    return ['','']
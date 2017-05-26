"""
This module provides functionality for parsing the string representations of
Bloomberg Open API Requests and Responses into equivalent JSON or dictionary
representations
"""

# A huge thank you to Paul McGuire who provided invaluable help working out
# the grammar for this. Discussion available at
# https://stackoverflow.com/questions/44144055/parsing-json-like-format-with-pyparsing/44172752#44172752
import json
import pyparsing as pp


def _parse(mystr):

    LBRACE, RBRACE, EQUAL = map(pp.Suppress, "{}=")
    field = pp.Word(pp.printables + ' ', excludeChars='[]=')
    field.addParseAction(pp.tokenMap(str.rstrip))
    string = pp.dblQuotedString().setParseAction(pp.removeQuotes)
    number = pp.pyparsing_common.number()
    date_expr = pp.Regex(r'\d\d\d\d-\d\d-\d\d')
    time_expr = pp.Regex(r'\d\d:\d\d:\d\d\.\d\d\d')
    scalar_value = (string | date_expr | time_expr | number)

    list_marker = pp.Suppress("[]")
    value_list = pp.Forward()
    jobject = pp.Forward()

    memberDef1 = pp.Group(field + EQUAL + scalar_value)
    memberDef2 = pp.Group(field + EQUAL + jobject)
    memberDef3 = pp.Group(field + list_marker + EQUAL + LBRACE + value_list +
                          RBRACE)
    memberDef = memberDef1 | memberDef2 | memberDef3

    value_list <<= (pp.delimitedList(scalar_value, ",") |
                    pp.ZeroOrMore(pp.Group(pp.Dict(memberDef2))))
    value_list.setParseAction(lambda t: [pp.ParseResults(t[:])])

    members = pp.OneOrMore(memberDef)
    jobject <<= pp.Dict(LBRACE + pp.ZeroOrMore(memberDef) + RBRACE)
    # force empty jobject to be a dict
    jobject.setParseAction(lambda t: t or {})

    parser = members
    parser = pp.OneOrMore(pp.Group(pp.Dict(memberDef)))

    return parser.parseString(mystr)


def to_dict_list(mystr):
    """
    Translate a string representation of a Bloomberg Open API Request/Response
    into a list of dictionaries.return

    Parameters
    ----------
    mystr: str
        A string representation of one or more blpapi.request.Request or
        blp.message.Message, these should be '\\n' seperated
    """
    res = _parse(mystr)
    dicts = []
    for res_dict in res:
        dicts.append(res_dict.asDict())
    return dicts


def to_json(mystr):
    """
    Translate a string representation of a Bloomberg Open API Request/Response
    into a JSON string

    Parameters
    ----------
    mystr: str
        A string representation of one or more blpapi.request.Request or
        blp.message.Message, these should be '\\n' seperated
    """
    dicts = to_dict_list(mystr)
    json.dumps(dicts, indent=2)

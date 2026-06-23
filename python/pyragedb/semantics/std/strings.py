from ..fragment import FunctionCallExpr

def lower(expr):
    return FunctionCallExpr("strings.lower", expr)

def upper(expr):
    return FunctionCallExpr("strings.upper", expr)

def strip(expr):
    return FunctionCallExpr("strings.strip", expr)

def concat(a, b):
    return FunctionCallExpr("strings.concat", a, b)

def join(list_expr, separator=""):
    return FunctionCallExpr("strings.join", list_expr, separator)

def string(expr):
    return FunctionCallExpr("strings.string", expr)

def replace(expr, old, new):
    return FunctionCallExpr("strings.replace", expr, old, new)

def like(expr, pattern):
    return FunctionCallExpr("strings.like", expr, pattern)

def startswith(expr, prefix):
    return FunctionCallExpr("strings.startswith", expr, prefix)

def endswith(expr, suffix):
    return FunctionCallExpr("strings.endswith", expr, suffix)

def split_part(expr, delimiter, part_idx):
    return FunctionCallExpr("strings.split_part", expr, delimiter, part_idx)

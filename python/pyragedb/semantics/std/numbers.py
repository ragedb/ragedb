from ..fragment import FunctionCallExpr

def integer(expr):
    return FunctionCallExpr("numbers.integer", expr)

def float(expr):
    return FunctionCallExpr("numbers.float", expr)

def number(expr, precision=None, scale=None):
    return FunctionCallExpr("numbers.number", expr)

def parse_number(expr, precision=None, scale=None):
    return FunctionCallExpr("numbers.parse_number", expr)

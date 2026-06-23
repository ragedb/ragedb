from ..fragment import FunctionCallExpr

def clip(expr, min_val, max_val):
    return FunctionCallExpr("math.clip", expr, min_val, max_val)

def round(expr, digits=None):
    if digits is not None:
        return FunctionCallExpr("math.round", expr, digits)
    return FunctionCallExpr("math.round", expr)

def floor(expr):
    return FunctionCallExpr("math.floor", expr)

def ceil(expr):
    return FunctionCallExpr("math.ceil", expr)

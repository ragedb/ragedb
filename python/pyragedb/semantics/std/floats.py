from ..fragment import FunctionCallExpr

def float(expr):
    return FunctionCallExpr("floats.float", expr)

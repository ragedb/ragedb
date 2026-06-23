from .model import Model
from .types import (
    String, Integer, Float, Boolean, DateTime,
    Any, AnyEntity, Number, Int, Int64, Int128, Decimal, Bool, Date, Error, Hash
)
from .fragment import CombinedFragment, NotCondition, Alias, Data
from . import std

def get_active_model():
    from .model import _active_model
    if _active_model is None:
        raise RuntimeError("No active model has been created.")
    return _active_model

def select(*projections):
    return get_active_model().select(*projections)

def where(*conditions):
    return get_active_model().where(*conditions)

def define(concept, rule_fragment):
    return get_active_model().define(concept, rule_fragment)

def require(*requirements, name=None):
    return get_active_model().require(*requirements, name=name)

def union(*fragments):
    if not fragments:
        raise ValueError("union expects at least one fragment")
    result = fragments[0]
    for f in fragments[1:]:
        result = CombinedFragment("or", result, f)
    return result

def not_(condition):
    return NotCondition(condition)

def alias(value, name):
    return Alias(value, name)

def distinct(*args):
    return get_active_model().select(*args).distinct()

def data(df_or_dict, name=None):
    return Data(df_or_dict, name=name)

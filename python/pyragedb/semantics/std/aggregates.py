from ..fragment import AggregateBuilder, OrderSpec, RankBuilder, TopBuilder

def count(target):
    return AggregateBuilder("count", target)

def sum(target):
    return AggregateBuilder("sum", target)

def avg(target):
    return AggregateBuilder("avg", target)

def min(target):
    return AggregateBuilder("min", target)

def max(target):
    return AggregateBuilder("max", target)

class GroupedAggregateBuilder:
    def __init__(self, concepts):
        self.concepts = concepts

    def count(self, target):
        return AggregateBuilder("count", target).per(*self.concepts)

    def sum(self, target):
        return AggregateBuilder("sum", target).per(*self.concepts)

    def avg(self, target):
        return AggregateBuilder("avg", target).per(*self.concepts)

    def min(self, target):
        return AggregateBuilder("min", target).per(*self.concepts)

    def max(self, target):
        return AggregateBuilder("max", target).per(*self.concepts)

def per(*concepts):
    return GroupedAggregateBuilder(concepts)

def asc(expr):
    return OrderSpec(expr, "ASC")

def desc(expr):
    return OrderSpec(expr, "DESC")

def rank(*sort_keys):
    return RankBuilder(*sort_keys)

def top(limit, *sort_keys):
    return TopBuilder(limit, *sort_keys, op="top")

def bottom(limit, *sort_keys):
    return TopBuilder(limit, *sort_keys, op="bottom")

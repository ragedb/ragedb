# Primitive types for pyragedb-semantics

class PrimitiveType:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"PrimitiveType({self.name})"

    def __format__(self, format_spec):
        if format_spec:
            return f"{{Primitive:{self.name}:{format_spec}}}"
        return f"{{Primitive:{self.name}}}"

String = PrimitiveType("String")
Integer = PrimitiveType("Integer")
Float = PrimitiveType("Float")
Boolean = PrimitiveType("Boolean")
DateTime = PrimitiveType("DateTime")
Any = PrimitiveType("Any")
AnyEntity = PrimitiveType("AnyEntity")
Number = PrimitiveType("Number")
Int = PrimitiveType("Integer")
Int64 = PrimitiveType("Int64")
Int128 = PrimitiveType("Int128")
Decimal = PrimitiveType("Decimal")
Bool = PrimitiveType("Boolean")
Date = PrimitiveType("Date")
Error = PrimitiveType("Error")
Hash = PrimitiveType("Hash")

# Mapping of lowercase primitive type name strings to their PrimitiveType objects.
# Used to resolve string representations of types in AttributeRefs.
PRIMITIVE_MAP = {
    "string": String,
    "integer": Integer,
    "float": Float,
    "boolean": Boolean,
    "datetime": DateTime,
    "any": Any,
    "anyentity": AnyEntity,
    "number": Number,
    "int": Integer,
    "int64": Int64,
    "int128": Int128,
    "decimal": Decimal,
    "bool": Boolean,
    "date": Date,
    "error": Error,
    "hash": Hash
}

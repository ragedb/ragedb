# Concept definition for pyragedb-semantics

class AttributeRef:
    def __init__(self, parent, name, is_relationship=False, target=None):
        self._parent = parent  # Concept or AttributeRef
        self._name = name
        self._is_relationship = is_relationship
        # Resolve target name string to the actual PrimitiveType object if it matches
        if isinstance(target, str):
            from .types import PRIMITIVE_MAP
            target = PRIMITIVE_MAP.get(target.lower(), target)
        self._target = target  # Concept or PrimitiveType

    def __repr__(self):
        return f"AttributeRef({self._parent}.{self._name})"

    def __getattr__(self, attr_name):
        from .concept import Concept
        if isinstance(self._target, Concept):
            try:
                target_attr = getattr(self._target, attr_name)
                return AttributeRef(self, attr_name, target_attr._is_relationship, target_attr._target)
            except AttributeError:
                pass
        return AttributeRef(self, attr_name)

    def __getitem__(self, key):
        # e.g., Shipment.shipped_for["carrier"]
        return AttributeRef(self, f"role_{key}")

    def __eq__(self, other):
        from .fragment import Condition
        return Condition("eq", self, other)

    def __ne__(self, other):
        from .fragment import Condition
        return Condition("ne", self, other)

    def __lt__(self, other):
        from .fragment import Condition
        return Condition("lt", self, other)

    def __le__(self, other):
        from .fragment import Condition
        return Condition("le", self, other)

    def __gt__(self, other):
        from .fragment import Condition
        return Condition("gt", self, other)

    def __ge__(self, other):
        from .fragment import Condition
        return Condition("ge", self, other)

    def __add__(self, other):
        from .fragment import ArithmeticExpr
        return ArithmeticExpr("+", self, other)

    def __sub__(self, other):
        from .fragment import ArithmeticExpr
        return ArithmeticExpr("-", self, other)

    def __mul__(self, other):
        from .fragment import ArithmeticExpr
        return ArithmeticExpr("*", self, other)

    def __truediv__(self, other):
        from .fragment import ArithmeticExpr
        return ArithmeticExpr("/", self, other)

    def __radd__(self, other):
        from .fragment import ArithmeticExpr
        return ArithmeticExpr("+", other, self)

    def __rsub__(self, other):
        from .fragment import ArithmeticExpr
        return ArithmeticExpr("-", other, self)

    def __rmul__(self, other):
        from .fragment import ArithmeticExpr
        return ArithmeticExpr("*", other, self)

    def __rtruediv__(self, other):
        from .fragment import ArithmeticExpr
        return ArithmeticExpr("/", other, self)

    def __or__(self, other):
        from .fragment import FallbackExpr
        return FallbackExpr(self, other)

    def as_bool(self):
        from .fragment import AsBoolExpr
        return AsBoolExpr(self)

    def ref(self, name=None):
        return AttributeRefRef(self, name)

    def alias(self, name):
        from .fragment import Alias
        return Alias(self, name)


class Concept:
    """
    Represents an entity type or value type in the semantic model.
    Supports single and multiple inheritance, identity schemes, and custom validation.
    """
    def __init__(self, name, identify_by=None, extends=None, identity_includes_type=False):
        self._name = name
        self._identify_by = identify_by
        # Normalize extends to a list of ancestor concepts
        if extends is None:
            self._extends = []
        elif isinstance(extends, Concept):
            self._extends = [extends]
        else:
            self._extends = list(extends)
        self._identity_includes_type = identity_includes_type
        self._attributes = {}  # name -> AttributeRef

    def __repr__(self):
        return f"Concept({self._name})"

    def __format__(self, format_spec):
        if format_spec:
            return f"{{Concept:{self._name}:{format_spec}}}"
        return f"{{Concept:{self._name}}}"

    def inherits_from(self, parent):
        """
        Recursively checks if this concept inherits from a given parent concept.
        
        Args:
            parent (Concept): The parent concept to search for in the inheritance hierarchy.
            
        Returns:
            bool: True if this concept matches parent, or inherits from it directly or transitively.
        """
        # If this is the parent concept itself, the hierarchy matches here.
        if self == parent:
            return True
            
        # Transitively check all concepts declared in our extends list.
        if self._extends:
            for p in self._extends:
                if p.inherits_from(parent):
                    return True
        return False

    def get_labels(self):
        """
        Retrieves all labels (including all parent concept names) in the inheritance tree.
        Used to insert nodes with multiple labels in GQL (supporting polymorphism).
        
        Returns:
            list[str]: A list of all concept names in the inheritance hierarchy.
        """
        labels = [self._name]
        visited = set()
        queue = list(self._extends)
        
        # Traverse the extends hierarchy using Breadth-First Search (BFS)
        while queue:
            parent = queue.pop(0)
            if parent not in visited:
                visited.add(parent)
                labels.append(parent._name)
                # Queue parent's parent concepts to continue traversal
                if parent._extends:
                    queue.extend(parent._extends)
        return labels

    def get_identity_scheme(self):
        """
        Retrieves the identity scheme for the concept.
        If not declared locally, traverses the extends hierarchy to find the first inherited one.
        
        Returns:
            dict[str, type] | None: The mapping of identifying fields to types, or None.
        """
        # Return locally defined identity scheme if we have one.
        if self._identify_by:
            return self._identify_by
            
        # Otherwise, fall back to check our parent concepts in extends list.
        if self._extends:
            for parent in self._extends:
                scheme = parent.get_identity_scheme()
                if scheme:
                    return scheme
        return None

    def get_identity_includes_type(self):
        """
        Recursively checks if identity_includes_type setting is enabled locally or on any ancestor.
        If True, sibling subconcepts are partitioned to be distinct from one another.
        
        Returns:
            bool: True if type-partitioning is enabled.
        """
        if self._identity_includes_type:
            return True
        if self._extends:
            for parent in self._extends:
                if parent.get_identity_includes_type():
                    return True
        return False

    def identify_by(self, *properties):
        """
        Defines the custom identity scheme using already declared property/relationship objects.
        Validates that properties are owned by this concept or inherited from an ancestor.
        
        Args:
            *properties (Property or Relationship): Properties or relationships defining identity.
        """
        resolved_props = []
        for prop in properties:
            # Resolve callable Property/Relationship objects to AttributeRef
            if not isinstance(prop, AttributeRef) and callable(prop):
                prop = prop()
            if not isinstance(prop, AttributeRef):
                raise ValueError("identify_by arguments must be properties or relationships of this concept")
            # Ensure the property is owned by this concept or inherits from the property's parent concept.
            if not self.inherits_from(prop._parent):
                raise ValueError("Only pass properties of the concept you are configuring")
            resolved_props.append(prop)
            
        # Save identifying keys and their resolved targets
        if self._identify_by is None:
            self._identify_by = {}
        for prop in resolved_props:
            self._identify_by[prop._name] = prop._target

    def __setattr__(self, name, value):
        if name.startswith('_'):
            super().__setattr__(name, value)
            return
        
        # Check if the value is a GraphAlgorithmExpr
        try:
            from .reasoners.graph import GraphAlgorithmExpr
            is_expr = isinstance(value, GraphAlgorithmExpr)
        except ImportError:
            is_expr = False

        if is_expr:
            from .model import _active_model
            _active_model._register_pending_graph_algorithm(self, name, value)
            
            is_rel = value.name in ("jaccard_similarity", "cosine_similarity", "adamic_adar", "preferential_attachment", "reachable", "distance")
            from .concept import AttributeRef
            ref = AttributeRef(self, name, is_relationship=is_rel, target=self if is_rel else value.target_type)
            self._attributes[name] = ref
        else:
            super().__setattr__(name, value)

    def __getattr__(self, attr_name):
        # Look up locally defined attributes first
        if attr_name in self._attributes:
            return self._attributes[attr_name]

        # Traverse parent concepts (extends hierarchy) for inherited attributes
        if self._extends:
            for parent in self._extends:
                try:
                    return getattr(parent, attr_name)
                except AttributeError:
                    continue

        # Fallback: dynamically return a new AttributeRef
        return AttributeRef(self, attr_name)

    def ref(self, name=None):
        return ConceptRef(self, name)

    def new(self, **properties):
        return ConceptNewInstance(self, properties)

    def require(self, *conditions, name=None):
        from .model import _active_model
        if _active_model is not None:
            _active_model.require(*conditions, name=name, scope=self)


def get_attribute_info(concept, name):
    try:
        attr = getattr(concept, name)
    except AttributeError:
        return False, None, name
    is_relationship = False
    target = None
    canonical_name = name
    if isinstance(attr, AttributeRef):
        is_relationship = attr._is_relationship
        target = attr._target
        canonical_name = attr._name
    elif hasattr(attr, "is_relationship"):
        is_relationship = attr.is_relationship
        from .model import _active_model
        target = _active_model.concept_index.get(attr.target)
        canonical_name = getattr(attr, "name", name)
    return is_relationship, target, canonical_name



class ConceptRef:
    def __init__(self, concept, name=None):
        self._concept = concept
        self._ref_name = name or f"{concept._name.lower()}_{id(self)}"

    def __setattr__(self, name, value):
        if name.startswith('_'):
            super().__setattr__(name, value)
            return
        self._concept.__setattr__(name, value)

    def __repr__(self):
        return f"ConceptRef({self._concept._name}, name={self._ref_name})"

    def __getattr__(self, attr_name):
        is_rel, target, canonical_name = get_attribute_info(self._concept, attr_name)
        return AttributeRef(self, canonical_name, is_rel, target)

    def require(self, *conditions, name=None):
        from .model import _active_model
        if _active_model is not None:
            _active_model.require(*conditions, name=name, scope=self)


class AttributeRefRef:
    def __init__(self, attribute_ref, name=None):
        self._attribute_ref = attribute_ref
        self._ref_name = name or f"{attribute_ref._name.lower()}_{id(self)}"

    def __repr__(self):
        return f"AttributeRefRef({self._attribute_ref}, name={self._ref_name})"

    def __getattr__(self, attr_name):
        is_rel, target, canonical_name = get_attribute_info(self._attribute_ref._target, attr_name)
        return AttributeRef(self, canonical_name, is_rel, target)


class ConceptNewInstance:
    def __init__(self, concept, properties):
        self._concept = concept
        self._properties = properties
        self._var_name = f"{concept._name.lower()}_{id(self)}"

    def __repr__(self):
        return f"ConceptNewInstance({self._concept._name}, properties={self._properties})"

    def __getattr__(self, name):
        is_rel, target, canonical_name = get_attribute_info(self._concept, name)
        if is_rel:
            ref = AttributeRef(self._concept, canonical_name, is_relationship=True, target=target)
            return BoundRelationship(self, ref)
        return AttributeRef(self, canonical_name, is_relationship=False, target=target)


class BoundRelationship:
    def __init__(self, source, attribute_ref):
        self._source = source
        self._attribute_ref = attribute_ref

    def __call__(self, target):
        return RelationshipFact(self._source, self._attribute_ref, target)


class RelationshipFact:
    def __init__(self, source, attribute_ref, target, properties=None):
        self._source = source
        self._attribute_ref = attribute_ref
        self._target = target
        self._properties = properties or {}

    def __repr__(self):
        return f"RelationshipFact({self._source} -> {self._attribute_ref._name} -> {self._target})"




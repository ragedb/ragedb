from ..relationship import Relationship
import requests

def _register_property(rel: Relationship, prop: str):
    if not isinstance(rel, Relationship):
        raise TypeError("alglib relation properties can only be declared on Relationship types.")
    
    from .. import get_active_model
    model = get_active_model()
    
    if not hasattr(rel, "_algebraic_properties"):
        rel._algebraic_properties = set()
    rel._algebraic_properties.add(prop)
    
    # Synchronize algebraic property registration with RageDB schema catalog
    rel_type = rel.name
    try:
        requests.post(
            f"{model.host}/db/{model.graph}/schema/relationships/{rel_type}/algebra",
            json=list(rel._algebraic_properties)
        )
    except requests.RequestException:
        pass

def symmetric(rel: Relationship):
    _register_property(rel, "symmetric")

def transitive(rel: Relationship):
    _register_property(rel, "transitive")

def reflexive(rel: Relationship, domain):
    # Reflexive holds for a domain concept
    _register_property(rel, "reflexive")

def irreflexive(rel: Relationship):
    _register_property(rel, "irreflexive")

def antisymmetric(rel: Relationship):
    _register_property(rel, "antisymmetric")

def equivalence_relation(rel: Relationship, domain):
    reflexive(rel, domain)
    symmetric(rel)
    transitive(rel)

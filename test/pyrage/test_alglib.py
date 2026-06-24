import unittest
from pyragedb.semantics import Model
from pyragedb.semantics.std import alglib

class TestAlglib(unittest.TestCase):
    def test_alglib_declarations(self):
        m = Model("alglib_test_model")
        Person = m.Concept("Person")
        
        # 1. Symmetric Relationship
        knows = m.Relationship(f"{Person} knows {Person:friend}")
        alglib.symmetric(knows)
        self.assertIn("symmetric", knows._algebraic_properties)
        
        # 2. Transitive and Irreflexive Relationship
        ancestor_of = m.Relationship(f"{Person} ancestor_of {Person:descendant}")
        alglib.transitive(ancestor_of)
        alglib.irreflexive(ancestor_of)
        self.assertIn("transitive", ancestor_of._algebraic_properties)
        self.assertIn("irreflexive", ancestor_of._algebraic_properties)
        
        # 3. Antisymmetric Relationship via Model property
        precedes = m.Relationship(f"{Person} precedes {Person:other}")
        m.alglib.antisymmetric(precedes)
        self.assertIn("antisymmetric", precedes._algebraic_properties)
        
        # 4. Equivalence Relation (sets reflexive, symmetric, and transitive)
        same_clan = m.Relationship(f"{Person} same_clan {Person:relative}")

        m.alglib.equivalence_relation(same_clan, domain=Person)
        self.assertIn("reflexive", same_clan._algebraic_properties)
        self.assertIn("symmetric", same_clan._algebraic_properties)
        self.assertIn("transitive", same_clan._algebraic_properties)

if __name__ == "__main__":
    unittest.main()

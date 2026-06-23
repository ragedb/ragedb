import unittest
from pyragedb.semantics import Model, String, Integer

class TestGqlCompilation(unittest.TestCase):
    def test_gql_compilation_simple(self):
        m = Model("test_model")
        Person = m.Concept("Person")
        m.Property(f"{Person} has age {Integer:age}")
        
        fragment = m.where(Person.age >= 18)
        gql = fragment.to_gql()
        
        # Check components of GQL
        self.assertIn("MATCH (person:Person)", gql)
        self.assertIn("person.age >= 18", gql)
        self.assertIn("RETURN person", gql)

    def test_gql_compilation_traversal(self):
        m = Model("test_model")
        Order = m.Concept("Order")
        Carrier = m.Concept("Carrier")
        
        m.Property(f"{Order} has status {String:status}")
        m.Relationship(f"{Order} shipped by {Carrier:shipped_by}")
        m.Property(f"{Carrier} has name {String:name}")
        
        # Build logic fragment
        fragment = m.where(Order.status == "pending")\
                    .where(Order.shipped_by.name == "DHL")
        gql = fragment.to_gql()
        
        # Check MATCH patterns and filters
        self.assertIn("(order:Order)", gql)
        self.assertIn("(order)-[:SHIPPED_BY]->(carrier:Carrier)", gql)
        self.assertIn("order.status = 'pending'", gql)
        self.assertIn("carrier.name = 'DHL'", gql)
        self.assertIn("RETURN order, carrier", gql)

    def test_global_wrappers_and_negation_union_alias(self):
        # Reset active model by instantiating a new one
        m = Model("active_test_model")
        Person = m.Concept("Person")
        m.Property(f"{Person} has name {String:name}")
        m.Property(f"{Person} has age {Integer:age}")

        # Test global wrappers delegating to the active model
        from pyragedb.semantics import where, select, define, require, not_, union, alias

        # define logic
        define(Person, where(Person.age >= 18))

        # Test global query compiling with not_, union, and alias
        q1 = where(Person.name == "Alice")
        q2 = where(not_(Person.age < 18))
        q_union = union(q1, q2)
        q_alias = q_union.select(alias(Person.name, "alias_name"))

        gql = q_alias.to_gql()
        self.assertIn("NOT (person.age < 18)", gql)
        self.assertIn("UNION", gql)
        self.assertIn("person.name AS alias_name", gql)

if __name__ == "__main__":
    unittest.main()

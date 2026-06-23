import unittest
from pyragedb.semantics import Model, String, Integer

class TestBaseConcepts(unittest.TestCase):
    def test_concept_and_types_format(self):
        m = Model("test_model")
        Person = m.Concept("Person")
        self.assertEqual(f"{Person}", "{Concept:Person}")
        self.assertEqual(f"{Person:name}", "{Concept:Person:name}")
        self.assertEqual(f"{String}", "{Primitive:String}")
        self.assertEqual(f"{String:val}", "{Primitive:String:val}")

    def test_property_and_relationship_reading(self):
        m = Model("test_model")
        Person = m.Concept("Person")
        Company = m.Concept("Company")
        
        # Test Property
        prop = m.Property(f"{Person} has name {String:name}")
        self.assertEqual(prop.name, "name")
        self.assertEqual(prop.subject, "Person")
        self.assertEqual(prop.target, "String")
        
        # Test Relationship
        rel = m.Relationship(f"{Person} works at {Company:employer}")
        self.assertEqual(rel.name, "employer")
        self.assertEqual(rel.subject, "Person")
        self.assertEqual(rel.target, "Company")
        
        # Test inferred name
        rel2 = m.Relationship(f"{Person} is parent of {Person}")
        self.assertEqual(rel2.name, "is_parent_of")

    def test_fact_derivation_and_referencing(self):
        m = Model("fact_derivation_model")
        Order = m.Concept("Order")
        Shipment = m.Concept("Shipment")
        m.Property(f"{Order} has id {Integer:id}")
        m.Property(f"{Shipment} has id {Integer:id}")
        Order.shipments = m.Relationship(f"{Order} has shipment {Shipment:shipment}")

        # 1. Test Concept.ref() and Chain.ref() query compilation
        o1 = Order.ref("o1")
        o2 = Order.ref("o2")
        q1 = m.where(o1.id < o2.id).select(o1.id.alias("o1_id"), o2.id.alias("o2_id"))
        gql1 = q1.to_gql()
        self.assertIn("o1.id < o2.id", gql1)
        self.assertIn("(o1:Order)", gql1)
        self.assertIn("(o2:Order)", gql1)

        s1 = Order.shipments.ref("s1")
        s2 = Order.shipments.ref("s2")
        q2 = m.where(s1.id < s2.id).select(Order.id.alias("order_id"), s1.id.alias("s1_id"), s2.id.alias("s2_id"))
        gql2 = q2.to_gql()
        self.assertIn("s1.id < s2.id", gql2)
        self.assertIn("(order)-[:SHIPMENT]->(s1:Shipment)", gql2)
        self.assertIn("(order)-[:SHIPMENT]->(s2:Shipment)", gql2)

        # 2. Test Concept.new() and RelationshipFact creation
        o_fact = Order.new(id=1, priority="high")
        s_fact1 = Shipment.new(id=101)
        s_fact2 = Shipment.new(id=102)
        rel_fact1 = o_fact.shipments(s_fact1)
        rel_fact2 = o_fact.shipments(s_fact2)

        self.assertEqual(o_fact._concept, Order)
        self.assertEqual(o_fact._properties["priority"], "high")
        self.assertEqual(rel_fact1._source, o_fact)
        self.assertEqual(rel_fact1._target, s_fact1)

        # Mock Model.exec_gql to verify GQL INSERT string built in m.define()
        original_exec = m.exec_gql
        captured_gqls = []
        m.exec_gql = lambda query: captured_gqls.append(query) or "[]"
        try:
            m.define(
                o_fact,
                s_fact1,
                s_fact2,
                rel_fact1,
                rel_fact2,
            )
            self.assertEqual(len(captured_gqls), 1)
            gql_insert = captured_gqls[0]
            self.assertTrue(gql_insert.startswith("INSERT "))
            # Check node declarations
            self.assertIn(f"({o_fact._var_name}:Order", gql_insert)
            self.assertIn("id: 1", gql_insert)
            self.assertIn("priority: 'high'", gql_insert)
            self.assertIn(f"({s_fact1._var_name}:Shipment", gql_insert)
            # Check relationship declarations
            self.assertIn(f"({o_fact._var_name})-[:SHIPMENT]->({s_fact1._var_name})", gql_insert)
            self.assertIn(f"({o_fact._var_name})-[:SHIPMENT]->({s_fact2._var_name})", gql_insert)
        finally:
            m.exec_gql = original_exec

if __name__ == "__main__":
    unittest.main()

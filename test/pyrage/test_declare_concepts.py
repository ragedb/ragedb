import unittest
import json
from pyragedb.semantics import Model, String, Integer

class TestDeclareConcepts(unittest.TestCase):
    def test_model_catalog_metadata(self):
        m = Model("catalog_test_model")
        Customer = m.Concept("Customer")
        Order = m.Concept("Order")

        # Test concepts property
        concepts_list = m.concepts
        self.assertEqual(len(concepts_list), 2)
        self.assertIn(Customer, concepts_list)
        self.assertIn(Order, concepts_list)

        # Test concept_index property
        concept_dict = m.concept_index
        self.assertEqual(len(concept_dict), 2)
        self.assertEqual(concept_dict["Customer"], Customer)
        self.assertEqual(concept_dict["Order"], Order)

    def test_auto_declared_properties_from_identify_by(self):
        m = Model("auto_declared_test")
        # Identify by primitive property
        Customer = m.Concept("Customer", identify_by={"id": Integer})
        self.assertIn("id", Customer._attributes)
        self.assertEqual(Customer._attributes["id"]._target, Integer)
        self.assertFalse(Customer._attributes["id"]._is_relationship)

        # Identify by composite including relationship
        Product = m.Concept("Product", identify_by={"sku": String})
        OrderItem = m.Concept("OrderItem", identify_by={"order": Customer, "product": Product})
        self.assertIn("order", OrderItem._attributes)
        self.assertEqual(OrderItem._attributes["order"]._target, Customer)
        self.assertTrue(OrderItem._attributes["order"]._is_relationship)

    def test_advanced_identify_by_method(self):
        m = Model("advanced_identity_test")
        Product = m.Concept("Product")
        Product.sku = m.Property(f"{Product} has SKU {String:sku}")
        
        # Test registering identity properties via identify_by method
        Product.identify_by(Product.sku)
        self.assertEqual(Product.get_identity_scheme(), {"sku": String})

        # Test passing invalid property raising ValueError
        Customer = m.Concept("Customer")
        Customer.cid = m.Property(f"{Customer} has id {Integer:cid}")
        with self.assertRaises(ValueError):
            # Cannot configure Product with Customer's property
            Product.identify_by(Customer.cid)

    def test_single_inheritance_lookup(self):
        m = Model("inheritance_test")
        Order = m.Concept("Order", identify_by={"id": Integer})
        m.Property(f"{Order} has priority {String:priority}")
        
        DelayedOrder = m.Concept("DelayedOrder", extends=[Order])
        
        # Test property lookup inheritance
        self.assertEqual(DelayedOrder.id._name, "id")
        self.assertEqual(DelayedOrder.priority._name, "priority")
        self.assertEqual(DelayedOrder.get_identity_scheme(), {"id": Integer})

    def test_multiple_inheritance_lookup(self):
        m = Model("multi_inheritance_test")
        Shipment = m.Concept("Shipment", identify_by={"id": Integer})
        Trackable = m.Concept("Trackable")
        m.Property(f"{Trackable} has tracker_id {String:tracker_id}")

        TrackedShipment = m.Concept("TrackedShipment", extends=[Shipment, Trackable])

        # Test attribute inheritance from both bases
        self.assertEqual(TrackedShipment.id._name, "id")
        self.assertEqual(TrackedShipment.tracker_id._name, "tracker_id")
        self.assertEqual(TrackedShipment.get_identity_scheme(), {"id": Integer})
        self.assertEqual(TrackedShipment.get_labels(), ["TrackedShipment", "Shipment", "Trackable"])

    def test_inheritance_gql_insert_labels(self):
        m = Model("labels_insert_test")
        Order = m.Concept("Order", identify_by={"id": Integer})
        DelayedOrder = m.Concept("DelayedOrder", extends=[Order])

        d_fact = DelayedOrder.new(id=101, delay_reason="weather")
        
        original_exec = m.exec_gql
        captured_gqls = []
        m.exec_gql = lambda query: captured_gqls.append(query) or "[]"
        try:
            m.define(d_fact)
            self.assertEqual(len(captured_gqls), 1)
            gql_insert = captured_gqls[0]
            # Ensure multi-label insert exists
            self.assertIn(f"({d_fact._var_name}:DelayedOrder:Order", gql_insert)
        finally:
            m.exec_gql = original_exec

    def test_identity_uniqueness_validation(self):
        m = Model("uniqueness_test")
        Customer = m.Concept("Customer", identify_by={"id": Integer})
        
        original_exec = m.exec_gql
        captured_gqls = []
        
        # Mock executing queries:
        # First query checks identity uniqueness (returns empty array initially, meaning valid)
        m.exec_gql = lambda query: captured_gqls.append(query) or "[]"
        try:
            m.check_requirements()
            self.assertEqual(len(captured_gqls), 1)
            self.assertIn("MATCH (c1:Customer), (c2:Customer)", captured_gqls[0])
            self.assertIn("c1 != c2 AND c1.id = c2.id", captured_gqls[0])
            
            # Now mock duplicates found
            m.exec_gql = lambda query: '[{"c1.id": 1}]'
            with self.assertRaises(ValueError) as ctx:
                m.check_requirements()
            self.assertIn("Identity uniqueness violation for concept 'Customer'", str(ctx.exception))
        finally:
            m.exec_gql = original_exec

    def test_identity_includes_type_partitioning(self):
        m = Model("partition_test")
        Shipment = m.Concept("Shipment", identify_by={"id": Integer}, identity_includes_type=True)
        OutboundShipment = m.Concept("OutboundShipment", extends=[Shipment])
        ReturnShipment = m.Concept("ReturnShipment", extends=[Shipment])

        # Verify includes type settings are inherited
        self.assertTrue(Shipment.get_identity_includes_type())
        self.assertTrue(OutboundShipment.get_identity_includes_type())
        self.assertTrue(ReturnShipment.get_identity_includes_type())

        # Compile uniqueness constraint query for base Shipment
        gql = m._build_identity_constraint_gql(Shipment)
        # Ensure it excludes subconcept labels
        self.assertIn("NOT (c1:OutboundShipment)", gql)
        self.assertIn("NOT (c1:ReturnShipment)", gql)
        self.assertIn("NOT (c2:OutboundShipment)", gql)
        self.assertIn("NOT (c2:ReturnShipment)", gql)

        # For subconcept OutboundShipment, it should NOT exclude others because it is the most specific
        sub_gql = m._build_identity_constraint_gql(OutboundShipment)
        self.assertNotIn("NOT (c1:", sub_gql)

if __name__ == "__main__":
    unittest.main()

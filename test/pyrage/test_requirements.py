import unittest
from pyragedb.semantics import Model, String, Integer

class TestRequirements(unittest.TestCase):
    def test_requirements_and_validation(self):
        m = Model("req_test_model")
        Order = m.Concept("Order")
        Shipment = m.Concept("Shipment")
        m.Property(f"{Order} has created_at {Integer:created_at}")
        m.Property(f"{Order} has promised_ship_date {Integer:promised_ship_date}")
        m.Property(f"{Order} has status {String:status}")
        m.Property(f"{Shipment} has shipped_at {Integer:shipped_at}")
        Order.shipments = m.Relationship(f"{Order} has shipment {Shipment:shipment}")

        # 1. Scoped Concept Requirement
        Order.require(Order.promised_ship_date >= Order.created_at, name="OrderShipDate")
        req1_name, req1_conds, req1_gql = m.requirements[0]
        self.assertEqual(req1_name, "OrderShipDate")
        self.assertIn("MATCH (order:Order)", req1_gql)
        self.assertIn("WHERE NOT (order.promised_ship_date >= order.created_at)", req1_gql)
        self.assertIn("RETURN order", req1_gql)

        # 2. Scoped Fragment Requirement
        m.where(
            Order.shipments(Shipment),
            Shipment.shipped_at > Order.promised_ship_date
        ).require(Order.status == "delayed", name="LateShipmentDelayed")
        
        req2_name, req2_conds, req2_gql = m.requirements[1]
        self.assertEqual(req2_name, "LateShipmentDelayed")
        self.assertIn("(order:Order)", req2_gql)
        self.assertIn("(order)-[:SHIPMENT]->(shipment:Shipment)", req2_gql)
        self.assertIn("WHERE shipment.shipped_at > order.promised_ship_date AND NOT (order.status = 'delayed')", req2_gql)

        # 3. Model Global Requirement
        m.require(Order.promised_ship_date >= Order.created_at, name="GlobalAtLeastOneValidOrder")
        req3_name, req3_conds, req3_gql = m.requirements[2]
        self.assertEqual(req3_name, "GlobalAtLeastOneValidOrder")
        self.assertIn("MATCH (order:Order)", req3_gql)
        self.assertIn("total_cnt > 0", req3_gql)
        self.assertIn("OPTIONAL MATCH (order_val:Order)", req3_gql)
        self.assertIn("order_val.promised_ship_date >= order_val.created_at", req3_gql)
        self.assertIn("valid_cnt = 0", req3_gql)

        # 4. Materialization Validation Testing (Success)
        original_exec = m.exec_gql
        m.exec_gql = lambda query: "[]"
        try:
            res = m.select(Order.id).to_dict()
            self.assertEqual(res, [])
        finally:
            m.exec_gql = original_exec

        # 5. Materialization Validation Testing (Violation)
        m.exec_gql = lambda query: '[{"order": 1}]'
        try:
            with self.assertRaises(ValueError) as context:
                m.select(Order.id).to_dict()
            self.assertIn("Requirement 'OrderShipDate' violated", str(context.exception))
        finally:
            m.exec_gql = original_exec

if __name__ == "__main__":
    unittest.main()

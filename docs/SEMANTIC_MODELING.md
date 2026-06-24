# Semantic Modeling Guide (Python)

This guide describes how to build and configure a **Semantic Model** using RageDB's Python Semantic Layer. The Python API matches the modeling semantics and catalog APIs of RelationalAI (`relationalai.semantics`), allowing you to declare graph nodes (Concepts), concept hierarchies (Inheritance), properties (Value Types), edges (Relationships), derived properties (Rules/Views), and integrity constraints (Invariants).

---

## 1. Instantiating a Model

A `Model` represents the main execution context containing your schema catalogs, rules, and constraints. Creating a model automatically connects to the server and prepares the target database instance.

```python
from pyragedb.semantics import Model

# Connects to the local RageDB server (port 7243) and initializes the 'rage' database context
model = Model(name="logistics_model", host="http://localhost:7243", graph="rage")
```

---

## 2. Declaring Concepts (Node Types)

A **Concept** represents a category of nodes (equivalent to a vertex type) in your graph.

```python
# A simple concept definition
Customer = model.Concept("Customer")
```

### Identity Schemes & Auto-Property Registration

Concepts are mapped in the database by an **Identity Scheme** (a set of properties that uniquely identify a node). Using the `identify_by` dictionary argument, properties and their primitive types are auto-declared and registered in the database catalog:

```python
from pyragedb.semantics import String, Integer, Float, Boolean

# Product identified by a unique SKU string
Product = model.Concept("Product", identify_by={"sku": String})

# OrderItem identified by a composite of two concepts (relationships)
OrderItem = model.Concept("OrderItem", identify_by={"order": Customer, "product": Product})
```

You can also register identity schemes explicitly:

```python
Product = model.Concept("Product")
Product.sku = model.Property(f"{Product} has SKU {String:sku}")

# Explicitly configure 'sku' as the unique identifier for Product
Product.identify_by(Product.sku)
```

---

## 3. Concept Inheritance

RageDB supports single and multiple inheritance of concepts. Child concepts automatically inherit all properties, relationships, and identity schemes from their parents.

### Single Inheritance
```python
Order = model.Concept("Order", identify_by={"id": Integer})
model.Property(f"{Order} has priority {String:priority}")

# DelayedOrder extends Order, inheriting 'id', 'priority', and identity scheme 'id'
DelayedOrder = model.Concept("DelayedOrder", extends=[Order])
```

### Multiple Inheritance
```python
Shipment = model.Concept("Shipment", identify_by={"id": Integer})
Trackable = model.Concept("Trackable")
model.Property(f"{Trackable} has tracker_id {String:tracker_id}")

# TrackedShipment inherits properties from both Shipment and Trackable
TrackedShipment = model.Concept("TrackedShipment", extends=[Shipment, Trackable])
```

---

## 4. Declaring Properties (Value Types)

Properties are declared using **readings** which specify subject-predicate-object grammar. Primitive value types include `String`, `Integer`, `Float`, `Double`, and `Boolean`.

```python
# Registers the property 'name' of type String on 'Customer'
Customer.name = model.Property(f"{Customer} has name {String:name}")

# Registers the property 'age' of type Integer on 'Customer'
Customer.age = model.Property(f"{Customer} has age {Integer:age}")
```

When a property is created, the model automatically issues a synchronous REST schema call to the RageDB server, adding the property and its mapped type to the graph catalog.

---

## 5. Declaring Relationships (Edges)

Relationships define directed graph edges connecting two concepts. Like properties, they are declared using readings:

```python
# A simple knows relationship between Person nodes
knows = model.Relationship("{Person} knows {Person:friend}")

# An order has shipment relationship
Order.shipments = model.Relationship(f"{Order} has shipment {Shipment:shipment}")
```

Creating a relationship dynamically creates the corresponding edge schema (e.g. `KNOWS` or `SHIPMENT`) on the RageDB server.

---

## 6. Logic Rules & Views (Derived Concepts)

To define a concept dynamically based on the state of other concepts, you use rules. A rule translates to a database view, compiled on the server into GQL.

```python
# 1. Declare the target derived concept
PremiumCustomer = model.Concept("PremiumCustomer")

# 2. Define the logic query (Fragment) that populates it
model.define(PremiumCustomer, model.where(Customer.age > 30))
```

On the RageDB server, this executes a DDL compilation:
```sql
CREATE VIEW PremiumCustomer AS MATCH (customer:Customer) WHERE customer.age > 30 RETURN customer
```

---

## 7. Integrity Constraints (Invariants)

Invariants ensure database consistency and schema invariants. In RageDB, invariants are declared using `require()`. During query execution, they are checked via negative-validation queries.

### A. Concept-Scoped Invariants
Ensures that a condition holds for all nodes of a concept:

```python
# Every Order must have promised_ship_date greater than or equal to created_at
Order.require(Order.promised_ship_date >= Order.created_at, name="OrderShipDate")
```

### B. Query-Scoped Invariants
Ensures that logic matches specific conditions:

```python
# If a shipment is late, the Order status MUST be marked as 'delayed'
model.where(
    Order.shipments(Shipment),
    Shipment.shipped_at > Order.promised_ship_date
).require(Order.status == "delayed", name="LateShipmentDelayed")
```

### C. Model-Global Invariants
Ensures model-wide invariants:

```python
# Asserts that at least one valid Order exists in the database
model.require(Order.promised_ship_date >= Order.created_at, name="GlobalAtLeastOneValidOrder")
```

### D. Validation Execution
When retrieving data via `to_dict()` or `to_df()`, the Python client runs the validation suite. If any invariant check finds violating records, a `ValueError` is raised, preventing invalid database execution:

```python
try:
    df = model.select(Customer.name).to_df()
except ValueError as e:
    print(f"Validation failed: {e}")
```

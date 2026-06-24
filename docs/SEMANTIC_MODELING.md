# Semantic Modeling Guide (Python)

This guide describes how to design, build, and query a **Semantic Model** using RageDB's Python Semantic Layer. Mapped after the modeling and reasoning API design of RelationalAI (`relationalai.semantics`), it allows you to declare graph nodes (Concepts), concept hierarchies (Inheritance), properties (Value Types), edges (Relationships), derived properties (Logic Rules/Views), and semantic integrity constraints (Invariants/Requirements).

---

## 1. Design & Modeling Principles

In RageDB, the semantic model represents your domain ontology. This is composed of:
1. **Concepts (Types)**: Represent classes of entities (nodes).
2. **Properties (Value Types)**: Map primitive scalar values (e.g. `String`, `Integer`, `Float`, `Double`, `Boolean`) to a concept.
3. **Relationships**: Directed connections (edges) between concept instances.

### Schema Synchronization
When you instantiate concepts, properties, or relationships in Python, the client automatically synchronizes these schemas dynamically with the RageDB C++ server's catalog using REST APIs. This ensures that node labels, relationship types, and property constraints are registered on the database before executing queries or inserting facts.

---

## 2. Instantiating a Model

A `Model` represents the main context of your schema. It manages the catalog index, registers logic rules, and compiles execution requests.

```python
from pyragedb.semantics import Model

# Instantiates a model pointing to the RageDB server instance running on localhost
model = Model(name="ecom_model", host="http://localhost:7243", graph="rage")
```

---

## 3. Declaring Concepts (Node Types)

A **Concept** corresponds to a node type in RageDB. 

```python
Customer = model.Concept("Customer")
```

### Identity Schemes
Concepts are defined in the catalog using an **Identity Scheme** (properties that uniquely identify a concept instance). You can define it automatically via the `identify_by` argument:

```python
from pyragedb.semantics import String, Integer

# Mapped as: Product has a unique String property 'sku'
Product = model.Concept("Product", identify_by={"sku": String})

# Mapped as: OrderItem is identified by a composite of other concept instances
OrderItem = model.Concept("OrderItem", identify_by={"order": Customer, "product": Product})
```

Alternatively, you can register identity schemes explicitly on a concept:

```python
Product = model.Concept("Product")
Product.sku = model.Property(f"{Product} has SKU {String:sku}")

# Configure 'sku' as the unique identity scheme
Product.identify_by(Product.sku)
```

---

## 4. Concept Inheritance

RageDB supports single and multiple inheritance. Inheriting child concepts transitively inherit all properties, relationships, and identity schemes from parent concepts.

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

# TrackedShipment inherits properties/identities from both base concepts
TrackedShipment = model.Concept("TrackedShipment", extends=[Shipment, Trackable])

# get_labels() returns: ["TrackedShipment", "Shipment", "Trackable"]
```

---

## 5. Declaring Properties & Relationships

Properties and relationships are defined using **readings** (grammatical subject-predicate-object strings). Mapped variables in brackets (e.g. `{Primitive:String:name}`) specify primitive value types or concept endpoints.

### Mapped Primitive Types
- `String`
- `Integer` (int, int64, int128)
- `Float` / `Double` (decimal, float, double)
- `Boolean` (bool)

### Declaring Properties
```python
# Mapped to a node property 'name' of type 'string' on Person nodes
Person.name = model.Property(f"{Person} has name {String:name}")

# Mapped to a node property 'age' of type 'integer' on Person nodes
Person.age = model.Property(f"{Person} has age {Integer:age}")
```

### Declaring Relationships
```python
# Mapped to a directed relationship type 'FRIEND' between two Person nodes
knows = model.Relationship("{Person} knows {Person:friend}")

# Mapped to a directed relationship type 'SHIPMENT' from Order to Shipment nodes
Order.shipments = model.Relationship(f"{Order} has shipment {Shipment:shipment}")
```

### Declaring Algebraic Properties (via `alglib`)
To enable the Semantic Query Optimizer to prune traversal paths, eliminate redundant join hops, and collapse loops, you can declare algebraic properties (such as symmetry, transitivity, reflexivity, etc.) on relationship schemas:

```python
from pyragedb.semantics.std import alglib

# 1. Symmetric Relationship
knows = model.Relationship("{Person} knows {Person:friend}")
alglib.symmetric(knows)

# 2. Transitive & Irreflexive Relationship
ancestor_of = model.Relationship("{Person} ancestor_of {Person:descendant}")
alglib.transitive(ancestor_of)
alglib.irreflexive(ancestor_of)

# 3. Equivalence Relation (reflexive, symmetric, transitive)
same_group = model.Relationship("{Person} same_group {Person:peer}")
alglib.equivalence_relation(same_group, domain=Person)
```

Adding these annotations automatically:
1. Registers the constraints to the RageDB `GqlVirtualCatalog`.
2. Enables C++ optimization passes (Phases 22 to 26) to rewrite GQL queries utilizing these traits at compile-time.

---

## 6. Derived Facts & Rules (Logic Views)

Rules allow you to derive new concepts and properties dynamically based on query logic. Defining a rule compiles a virtual database view on the server, ensuring it automatically optimizes and runs within C++ sharded traversals.

```python
# 1. Declare the derived concept
PremiumCustomer = model.Concept("PremiumCustomer")

# 2. Define the rule fragment using fluent query chaining
model.define(PremiumCustomer, model.where(Customer.age > 30))
```

This registers the view on the server:
```sql
CREATE VIEW PremiumCustomer AS MATCH (customer:Customer) WHERE customer.age > 30 RETURN customer
```

---

## 7. Working with Strings

The standard string library `pyragedb.semantics.std.strings` provides built-in string transformations and matching operators:

```python
from pyragedb.semantics.std import strings

# String modifications
s_lower = strings.lower(Person.name)       # lower(person.name)
s_upper = strings.upper(Person.name)       # upper(person.name)
s_strip = strings.strip(Person.name)       # trim(person.name)
s_concat = strings.concat(Person.name, "-tag") # person.name + '-tag'
s_join = strings.join([Person.name, "active"], " | ")

# Substring replacement & splitting
s_replace = strings.replace(Person.name, "old", "new")
s_split = strings.split_part(Person.name, "-", 1) # split(person.name, '-')[1]

# String comparison filters
query = model.where(
    strings.like(Person.name, "%Alice%"),      # person.name LIKE '%Alice%'
    strings.startswith(Person.name, "Ali"),   # person.name STARTS WITH 'Ali'
    strings.endswith(Person.name, "son")      # person.name ENDS WITH 'son'
)
```

---

## 8. Working with Numbers & Math

The `math` and `numbers` modules provide overloaded arithmetic and numeric conversion utilities:

```python
from pyragedb.semantics.std import math, numbers

# Overloaded arithmetic operators (+, -, *, /)
raw_score = (Person.score * 10) + (Person.bonus / 2)

# Math functions
clip_score = math.clip(raw_score, 0.0, 100.0) # CASE WHEN ...
round_val = math.round(raw_score, 2)          # Rounds to 2 decimal places
floor_val = math.floor(raw_score)            # floor(score)
ceil_val = math.ceil(raw_score)              # ceil(score)

# Type conversions & parsing
int_val = numbers.integer(Person.age_str)     # toInteger(person.age_str)
float_val = numbers.float(Person.score_str)   # toFloat(person.score_str)
```

---

## 9. Handling Missing Data (Nulls & Optionals)

In semantic modeling, missing data is represented by null values. The Python API provides several mechanisms to handle null boundaries:

### Coalesce / Fallback Operator (`|`)
You can use the overloaded pipe operator (`|`) to define default values when a property or relation is missing:

```python
# Compiles to: coalesce(person.priority, 'unknown')
priority_fallback = Person.priority | "unknown"
```

### Presence Check (`.as_bool()`)
Checks if a value is present (returns true if not null):

```python
# Compiles to: person.priority IS NOT NULL
has_priority = Person.priority.as_bool()
```

### Optional Matching
Queries that traverse paths that might not exist can use optional matches. If optional data is missing, variables are bound to `null`.
* On the server side, `OPTIONAL MATCH` is used.
* The query optimizer automatically promotes optional matches to inner matches when filters reject nulls (Phase 12), or converts optional matches with `IS NULL` filters into anti-semi-joins (Phase 18) for speedups.

---

## 10. Aggregation & Grouping

RageDB supports powerful aggregate expressions, including grouped evaluations, distinct sets, and window rankings.

```python
from pyragedb.semantics.std import aggregates as agg
from pyragedb.semantics import distinct

# 1. Base Aggregations
avg_score = agg.avg(Person.score)
max_age = agg.max(Person.age)

# 2. Grouped Aggregations (per-concept boundaries)
# Computes comment count per Ticket where Ticket has a comment
comment_count = agg.count(Comment).per(Ticket).where(Ticket.has_comment(Comment))

# 3. Distinct counts and distinct projections
unique_author_count = agg.count(distinct(Ticket.has_comment.author)).per(Ticket)
# Compiles to: count(DISTINCT tag)

# 4. Sorting & Window Rankings
# Rank accounts grouped by Open Ticket Count (descending) and Account ID (ascending)
open_ticket_rank = agg.rank(
    agg.desc(Account.open_ticket_count),
    agg.asc(Account.id),
)
```

### Top-K / Bottom-K Selection
You can restrict results using Top-K or Bottom-K filters:

```python
# Global Top-2 accounts by open ticket count
open_ticket_top = agg.top(2, agg.desc(Account.open_ticket_count))
query = model.select(Account.id).where(open_ticket_top)
# Compiles to: ORDER BY account.open_ticket_count DESC LIMIT 2

# Grouped Top-2 accounts per Queue
open_ticket_top_grouped = agg.top(2, agg.desc(Account.open_ticket_count)).per(Queue)
query = model.select(Queue.name, Account.id).where(open_ticket_top_grouped)
```

---

## 11. Integrity Invariants (Constraints)

You can define invariants using `require()` to prevent invalid data insertion or enforce business rules. RageDB runs validation queries before executing updates or returning query frames.

### Scoped Concept Requirements
```python
# Every Order must have promised_ship_date greater than or equal to created_at
Order.require(Order.promised_ship_date >= Order.created_at, name="OrderShipDate")
```

### Scoped Query Requirements
```python
# If a shipment is late, the Order status must be marked as 'delayed'
model.where(
    Order.shipments(Shipment),
    Shipment.shipped_at > Order.promised_ship_date
).require(Order.status == "delayed", name="LateShipmentDelayed")
```

### Model-Global Requirements
```python
# Asserts that at least one valid Order exists in the database
model.require(Order.promised_ship_date >= Order.created_at, name="GlobalAtLeastOneValidOrder")
```

When you retrieve results using `to_dict()` or `to_df()`, RageDB automatically evaluates these requirements. If any invariant is violated, a `ValueError` is raised, detailing the rule violation.

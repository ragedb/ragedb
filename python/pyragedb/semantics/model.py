# Model definition for pyragedb-semantics
import requests
import json
from .concept import Concept, AttributeRef
from .relationship import Property, Relationship
from .fragment import Fragment, Condition, AndCondition, OrCondition

_active_model = None

class Model:
    """
    Represents the main context for building, managing, and validating 
    a semantic ontology schema and rules in RageDB.
    """
    def __init__(self, name, host="http://localhost:7243", graph="rage"):
        self.name = name
        self.host = host
        self.graph = graph
        self._concept_index = {}  # Internal mapping of concept name string -> Concept object
        self.rules = {}
        self.requirements = []
        self._pending_graph_algorithms = []
        global _active_model
        _active_model = self

    @property
    def concept_index(self):
        """
        Returns a mapping of all declared concept names to their Concept objects.
        Matches RelationalAI Model.concept_index catalog API.
        """
        return self._concept_index

    @property
    def concepts(self):
        """
        Returns a list of all declared Concept objects in the model.
        Matches RelationalAI Model.concepts catalog API.
        """
        return list(self._concept_index.values())

    def Concept(self, name, identify_by=None, extends=None, identity_includes_type=False):
        """
        Declares a new concept type. Automatically handles:
        - Parent inheritance normalization
        - Database node type schema creation
        - Auto-registration of properties/relationships from identify_by dictionary argument
        """
        concept = Concept(name, identify_by=identify_by, extends=extends, identity_includes_type=identity_includes_type)
        self._concept_index[name] = concept
        # Automatically register concept type in database schema
        try:
            requests.post(f"{self.host}/db/{self.graph}/schema/nodes/{name}")
        except Exception:
            pass  # Ignored if server not running during definition

        # Automatically register properties/relationships from identify_by
        if identify_by:
            for prop_name, prop_type in identify_by.items():
                is_relationship = isinstance(prop_type, Concept)
                ref = AttributeRef(concept, prop_name, is_relationship=is_relationship, target=prop_type)
                concept._attributes[prop_name] = ref
                
                if is_relationship:
                    try:
                        requests.post(f"{self.host}/db/{self.graph}/schema/relationships/{prop_name.upper()}")
                    except Exception:
                        pass
                else:
                    # Map primitive target type to DB data type
                    target_name = getattr(prop_type, "name", str(prop_type)).lower()
                    if target_name == "string":
                        db_type = "string"
                    elif target_name in ("integer", "int", "int64", "int128"):
                        db_type = "integer"
                    elif target_name in ("float", "double", "decimal", "number"):
                        db_type = "double"
                    elif target_name in ("boolean", "bool"):
                        db_type = "boolean"
                    else:
                        db_type = "string"
                    try:
                        requests.post(f"{self.host}/db/{self.graph}/schema/nodes/{name}/properties/{prop_name}/{db_type}")
                    except Exception:
                        pass
        return concept

    def Property(self, reading):
        prop = Property(reading)
        subject_concept = self._concept_index.get(prop.subject)
        if subject_concept:
            # Register property ref
            target_type = prop.target.lower()
            if target_type == "string":
                db_type = "string"
            elif target_type in ("integer", "int"):
                db_type = "integer"
            elif target_type in ("float", "double"):
                db_type = "double"
            elif target_type in ("boolean", "bool"):
                db_type = "boolean"
            else:
                db_type = "string"

            # Register in Concept attributes
            ref = AttributeRef(subject_concept, prop.name, is_relationship=False, target=prop.target)
            subject_concept._attributes[prop.name] = ref

            # Register in database schema
            try:
                requests.post(f"{self.host}/db/{self.graph}/schema/nodes/{prop.subject}/properties/{prop.name}/{db_type}")
            except Exception:
                pass
        return prop

    def Relationship(self, reading):
        rel = Relationship(reading)
        subject_concept = self._concept_index.get(rel.subject)
        target_concept = self._concept_index.get(rel.target)
        if subject_concept and target_concept:
            # Register in Concept attributes
            ref = AttributeRef(subject_concept, rel.name, is_relationship=True, target=target_concept)
            subject_concept._attributes[rel.name] = ref

            # Register relationship type in database schema
            try:
                requests.post(f"{self.host}/db/{self.graph}/schema/relationships/{rel.name.upper()}")
            except Exception:
                pass
        return rel

    def define(self, *args):
        from .concept import Concept
        from .fragment import Fragment, CombinedFragment
        
        if len(args) == 2 and isinstance(args[0], Concept) and isinstance(args[1], (Fragment, CombinedFragment)):
            concept = args[0]
            rule_fragment = args[1]
            self.rules[concept._name] = rule_fragment
            # Synchronize virtual view with RageDB
            gql_query = rule_fragment.to_gql()
            ddl = f"CREATE VIEW {concept._name} AS {gql_query}"
            try:
                self.exec_gql(ddl)
            except Exception:
                pass
            return
            
        # Otherwise, treat as base facts insertion
        from .concept import ConceptNewInstance, RelationshipFact
        nodes = []
        relationships = []
        for fact in args:
            if isinstance(fact, ConceptNewInstance):
                if fact not in nodes:
                    nodes.append(fact)
            elif isinstance(fact, RelationshipFact):
                relationships.append(fact)
                if fact._source not in nodes:
                    nodes.append(fact._source)
                if fact._target not in nodes:
                    nodes.append(fact._target)

        insert_parts = []
        for node in nodes:
            props_str = ""
            if node._properties:
                props = []
                for k, v in node._properties.items():
                    val_str = f"'{v}'" if isinstance(v, str) else str(v)
                    props.append(f"{k}: {val_str}")
                props_str = " {" + ", ".join(props) + "}"
            labels_str = ":".join(node._concept.get_labels())
            insert_parts.append(f"({node._var_name}:{labels_str}{props_str})")
            
        for rel in relationships:
            rel_type = rel._attribute_ref._name.upper()
            props_str = ""
            if getattr(rel, "_properties", None):
                props = []
                for k, v in rel._properties.items():
                    val_str = f"'{v}'" if isinstance(v, str) else str(v)
                    props.append(f"{k}: {val_str}")
                props_str = " {" + ", ".join(props) + "}"
            insert_parts.append(f"({rel._source._var_name})-[:{rel_type}{props_str}]->({rel._target._var_name})")
            
        if not insert_parts:
            return
            
        gql = "INSERT " + ", ".join(insert_parts)
        try:
            self.exec_gql(gql)
        except Exception:
            pass

    def _compile_conditions(self, conditions, var_suffix=""):
        from .concept import Concept, ConceptRef, AttributeRefRef, AttributeRef
        from .fragment import (
            Condition, AndCondition, OrCondition, NotCondition, Alias,
            ArithmeticExpr, FallbackExpr, AsBoolExpr, FunctionCallExpr,
            AggregateBuilder, OrderSpec, RankBuilder, TopBuilder, Fragment,
            format_literal
        )

        referenced_concepts = set()

        def find_concepts(node):
            if isinstance(node, Concept):
                referenced_concepts.add(node)
            elif isinstance(node, ConceptRef):
                referenced_concepts.add(node)
            elif isinstance(node, AttributeRefRef):
                referenced_concepts.add(node)
                find_concepts(node._attribute_ref._parent)
            elif isinstance(node, AttributeRef):
                find_concepts(node._parent)
                if isinstance(node._target, Concept):
                    referenced_concepts.add(node._target)
            elif isinstance(node, Condition):
                find_concepts(node.left)
                find_concepts(node.right)
            elif isinstance(node, (AndCondition, OrCondition)):
                find_concepts(node.left)
                find_concepts(node.right)
            elif isinstance(node, NotCondition):
                find_concepts(node.condition)
            elif isinstance(node, Alias):
                find_concepts(node.value)
            elif isinstance(node, ArithmeticExpr):
                find_concepts(node.left)
                find_concepts(node.right)
            elif isinstance(node, FallbackExpr):
                find_concepts(node.left)
                find_concepts(node.right)
            elif isinstance(node, AsBoolExpr):
                find_concepts(node.expr)
            elif isinstance(node, FunctionCallExpr):
                for arg in node.args:
                    if isinstance(arg, list):
                        for item in arg:
                            find_concepts(item)
                    else:
                        find_concepts(arg)
            elif isinstance(node, AggregateBuilder):
                find_concepts(node.target)
                for concept in node.group_by_concepts:
                    find_concepts(concept)
                for cond in node.where_conditions:
                    find_concepts(cond)
            elif isinstance(node, OrderSpec):
                find_concepts(node.expr)
            elif isinstance(node, RankBuilder):
                for key in node.sort_keys:
                    find_concepts(key)
                for concept in node.group_by_concepts:
                    find_concepts(concept)
            elif isinstance(node, TopBuilder):
                for key in node.sort_keys:
                    find_concepts(key)
                for concept in node.group_by_concepts:
                    find_concepts(concept)
            elif isinstance(node, Fragment):
                for proj in node.projections:
                    find_concepts(proj)
                for cond in node.conditions:
                    find_concepts(cond)

        for cond in conditions:
            find_concepts(cond)

        var_map = {}
        for c in referenced_concepts:
            if isinstance(c, (ConceptRef, AttributeRefRef)):
                var_map[c] = c._ref_name + var_suffix
            else:
                var_map[c] = c._name.lower() + var_suffix

        matches = []

        def resolve_ref(ref):
            if isinstance(ref, ConceptRef):
                var = var_map[ref]
                match_str = f"({var}:{ref._concept._name})"
                if match_str not in matches:
                    matches.append(match_str)
                return var
            if isinstance(ref, AttributeRefRef):
                var = var_map[ref]
                parent_var = resolve_ref(ref._attribute_ref._parent)
                rel_type = ref._attribute_ref._name.upper()
                target_name = ref._attribute_ref._target._name
                trav_pattern = f"({parent_var})-[:{rel_type}]->({var}:{target_name})"
                if trav_pattern not in matches:
                    matches.append(trav_pattern)
                return var
            if isinstance(ref, Concept):
                var = var_map[ref]
                match_str = f"({var}:{ref._name})"
                if match_str not in matches:
                    matches.append(match_str)
                return var
            if not isinstance(ref, AttributeRef):
                return format_literal(ref)
            parent = ref._parent
            if isinstance(parent, (Concept, ConceptRef)):
                var = var_map[parent]
                parent_name = parent._concept._name if isinstance(parent, ConceptRef) else parent._name
                match_str = f"({var}:{parent_name})"
                if match_str not in matches:
                    matches.append(match_str)
                if ref._is_relationship:
                    target_var = var_map[ref._target]
                    rel_type = ref._name.upper()
                    trav_pattern = f"({var})-[:{rel_type}]->({target_var}:{ref._target._name})"
                    if trav_pattern not in matches:
                        matches.append(trav_pattern)
                    return target_var
                else:
                    return f"{var}.{ref._name}"
            else:
                parent_expr = resolve_ref(parent)
                if ref._is_relationship:
                    target_var = var_map[ref._target]
                    rel_type = ref._name.upper()
                    trav_pattern = f"({parent_expr})-[:{rel_type}]->({target_var}:{ref._target._name})"
                    if trav_pattern not in matches:
                        matches.append(trav_pattern)
                    return target_var
                else:
                    return f"{parent_expr}.{ref._name}"

        def compile_cond(cond):
            if isinstance(cond, Condition):
                left_expr = resolve_ref(cond.left)
                right_expr = resolve_ref(cond.right)
                op_map = {
                    "eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="
                }
                return f"{left_expr} {op_map[cond.op]} {right_expr}"
            elif isinstance(cond, AndCondition):
                return f"({compile_cond(cond.left)} AND {compile_cond(cond.right)})"
            elif isinstance(cond, OrCondition):
                return f"({compile_cond(cond.left)} OR {compile_cond(cond.right)})"
            elif isinstance(cond, NotCondition):
                return f"NOT ({compile_cond(cond.condition)})"
            elif isinstance(cond, (Concept, ConceptRef)):
                var = var_map[cond]
                cond_name = cond._concept._name if isinstance(cond, ConceptRef) else cond._name
                match_str = f"({var}:{cond_name})"
                if match_str not in matches:
                    matches.append(match_str)
                return "true"
            elif isinstance(cond, (FunctionCallExpr, ArithmeticExpr, FallbackExpr, AsBoolExpr)):
                return resolve_ref(cond)
            elif isinstance(cond, AttributeRef):
                resolve_ref(cond)
                return "true"
            else:
                return "true"

        where_filters = []
        for cond in conditions:
            if isinstance(cond, Fragment):
                for f_cond in cond.conditions:
                    f_str = compile_cond(f_cond)
                    if f_str != "true":
                        where_filters.append(f_str)
            else:
                f_str = compile_cond(cond)
                if f_str != "true":
                    where_filters.append(f_str)

        variables = list(var_map.values())
        return matches, where_filters, variables

    def require(self, *requirements, name=None, scope=None):
        if name is None:
            name = f"Constraint_{len(self.requirements)}"

        from .concept import Concept, ConceptRef
        from .fragment import Fragment

        if scope is None:
            # Model-wide global requirement: at least one match of requirements if concepts exist
            matches_exist, _, vars_exist = self._compile_conditions(requirements, var_suffix="")
            matches_valid, filters_valid, _ = self._compile_conditions(requirements, var_suffix="_val")

            if not matches_exist:
                for r in requirements:
                    if isinstance(r, (Concept, ConceptRef)):
                        c_name = r._concept._name if isinstance(r, ConceptRef) else r._name
                        var = r._ref_name if isinstance(r, ConceptRef) else r._name.lower()
                        matches_exist.append(f"({var}:{c_name})")

            if not matches_exist:
                return

            all_are_concepts = all(isinstance(r, (Concept, ConceptRef)) for r in requirements)
            if all_are_concepts:
                gql = f"OPTIONAL MATCH {', '.join(matches_valid)} WITH count(*) as valid_cnt WHERE valid_cnt = 0 RETURN valid_cnt"
            else:
                match_exist_str = ", ".join(matches_exist)
                match_valid_str = ", ".join(matches_valid)
                where_valid_str = " AND ".join(filters_valid)
                
                gql = f"MATCH {match_exist_str} WITH count(*) as total_cnt WHERE total_cnt > 0 "
                gql += f"OPTIONAL MATCH {match_valid_str} "
                if where_valid_str:
                    gql += f"WHERE {where_valid_str} "
                gql += f"WITH total_cnt, count(*) as valid_cnt WHERE valid_cnt = 0 RETURN total_cnt"

        elif isinstance(scope, (Concept, ConceptRef)):
            # Concept-level scoped requirement: every instance must satisfy requirements
            matches, where_filters, _ = self._compile_conditions(requirements, var_suffix="")
            scope_name = scope._concept._name if isinstance(scope, ConceptRef) else scope._name
            scope_var = scope._ref_name if isinstance(scope, ConceptRef) else scope._name.lower()
            scope_match = f"({scope_var}:{scope_name})"
            if scope_match not in matches:
                matches.insert(0, scope_match)

            gql = "MATCH " + ", ".join(matches)
            if where_filters:
                gql += " WHERE NOT (" + " AND ".join(where_filters) + ")"
            gql += f" RETURN {scope_var}"

        elif isinstance(scope, Fragment):
            # Fragment-scoped requirement: every match in the scope must satisfy requirements
            scope_matches, scope_filters, scope_vars = self._compile_conditions(scope.conditions, var_suffix="")
            req_matches, req_filters, _ = self._compile_conditions(requirements, var_suffix="")

            all_matches = list(scope_matches)
            for rm in req_matches:
                if rm not in all_matches:
                    all_matches.append(rm)

            gql = "MATCH " + ", ".join(all_matches)
            combined_filters = []
            if scope_filters:
                combined_filters.append(" AND ".join(scope_filters))
            if req_filters:
                combined_filters.append("NOT (" + " AND ".join(req_filters) + ")")

            if combined_filters:
                gql += " WHERE " + " AND ".join(combined_filters)
            gql += " RETURN " + ", ".join(scope_vars)
        else:
            raise ValueError(f"Invalid scope type: {type(scope)}")

        self.requirements.append((name, requirements, gql))

        # Register in database as a constraint
        ddl = f"CREATE CONSTRAINT {name} AS {gql}"
        try:
            r = requests.post(f"{self.host}/db/{self.graph}/gql", data=ddl)
            if r.status_code != 200:
                raise RuntimeError(f"Failed to register constraint: {r.text}")
        except Exception:
            pass

    def _get_all_subconcepts(self, concept):
        """
        Finds all subconcepts in the model that inherit from the given concept.
        """
        subs = []
        for c in self._concept_index.values():
            if c != concept and c.inherits_from(concept):
                subs.append(c)
        return subs

    def _build_identity_constraint_gql(self, concept):
        """
        Dynamically constructs a GQL query to check for identity scheme uniqueness violations.
        - Supports property identity keys (e.g. c1.prop = c2.prop)
        - Supports relationship identity keys (e.g. (c1)-[:REL]->(t) and (c2)-[:REL]->(t))
        - Excludes subconcept labels if identity_includes_type is True
        """
        scheme = concept.get_identity_scheme()
        if not scheme:
            return ""

        matches = [f"(c1:{concept._name})", f"(c2:{concept._name})"]
        filters = ["c1 != c2"]
        target_idx = 1

        for prop_name, prop_type in scheme.items():
            from .concept import Concept
            if isinstance(prop_type, Concept):
                # Identity key is a relationship target
                target_var = f"t{target_idx}"
                target_idx += 1
                rel_type = prop_name.upper()
                matches.append(f"(c1)-[:{rel_type}]->({target_var}:{prop_type._name})")
                matches.append(f"(c2)-[:{rel_type}]->({target_var})")
            else:
                # Identity key is a primitive property
                filters.append(f"c1.{prop_name} = c2.{prop_name}")

        # If identity_includes_type is True, we only check uniqueness within the base concept
        # by excluding any nodes that carry a subconcept label
        if concept.get_identity_includes_type():
            subs = self._get_all_subconcepts(concept)
            for sub in subs:
                filters.append(f"NOT (c1:{sub._name})")
                filters.append(f"NOT (c2:{sub._name})")

        gql = "MATCH " + ", ".join(matches) + " WHERE " + " AND ".join(filters) + " RETURN c1"
        return gql

    def check_requirements(self):
        """
        Executes all active database constraint checks:
        1. Dynamically compiled identity uniqueness constraints.
        2. User-defined requirements and invariants.
        Raises ValueError if any violations are returned from RageDB.
        """
        # 1. Check identity uniqueness constraints for all concepts
        for concept in self._concept_index.values():
            scheme = concept.get_identity_scheme()
            if scheme:
                gql = self._build_identity_constraint_gql(concept)
                if gql:
                    try:
                        res = self.exec_gql(gql)
                        rows = json.loads(res)
                        if rows:
                            raise ValueError(f"Identity uniqueness violation for concept '{concept._name}'. Violating elements: {rows}")
                    except Exception as e:
                        if isinstance(e, ValueError):
                            raise e
                        pass

        # 2. Check user-defined requirements
        for name, reqs, gql in self.requirements:
            try:
                res = self.exec_gql(gql)
                rows = json.loads(res)
                if rows:
                    raise ValueError(f"Requirement '{name}' violated. Violating elements: {rows}")
            except Exception as e:
                if isinstance(e, ValueError):
                    raise e
                pass

    def where(self, *conditions):
        return Fragment(self, conditions=list(conditions))

    def select(self, *projections):
        return Fragment(self, projections=list(projections))

    def _register_pending_graph_algorithm(self, concept, property_name, expr):
        if not hasattr(self, "_pending_graph_algorithms"):
            self._pending_graph_algorithms = []
        self._pending_graph_algorithms.append({
            "concept": concept,
            "property_name": property_name,
            "expr": expr
        })

    def _evaluate_pending_graph_algorithms(self):
        if not hasattr(self, "_pending_graph_algorithms") or not self._pending_graph_algorithms:
            return
        
        # Copy and clear the pending list to prevent recursive loops
        pending = list(self._pending_graph_algorithms)
        self._pending_graph_algorithms = []

        from .reasoners.graph import generate_lua_script
        for item in pending:
            concept = item["concept"]
            prop_name = item["property_name"]
            expr = item["expr"]
            graph = expr.graph
            
            # 1. Create property schema on nodes/relationships if not exists
            is_rel = expr.name in ("jaccard_similarity", "cosine_similarity", "adamic_adar", "preferential_attachment", "reachable", "distance", "triangle", "unique_triangle")
            if is_rel:
                try:
                    requests.post(f"{self.host}/db/{self.graph}/schema/relationships/{prop_name.upper()}")
                except Exception:
                    pass
            else:
                target_type = expr.target_type.lower()
                db_type = "double" if target_type == "float" else ("integer" if target_type == "integer" else ("boolean" if target_type == "boolean" else "string"))
                
                node_types = []
                if graph.node_concept:
                    node_types.append(graph.node_concept._name)
                else:
                    for c in graph.node_concepts:
                        node_types.append(c._name)
                
                for t_name in node_types:
                    try:
                        requests.post(f"{self.host}/db/{self.graph}/schema/nodes/{t_name}/properties/{prop_name}/{db_type}")
                    except Exception:
                        pass

            # 2. Generate Lua script
            lua_script = generate_lua_script(graph, prop_name, expr)

            # 3. Post Lua script to RageDB server
            r = requests.post(f"{self.host}/db/{self.graph}/lua", data=lua_script)
            if r.status_code != 200:
                raise RuntimeError(f"Failed to execute graph algorithm Lua script on server: {r.text}")

    def exec_gql(self, query):
        self._evaluate_pending_graph_algorithms()
        r = requests.post(f"{self.host}/db/{self.graph}/gql", data=query)
        if r.status_code != 200:
            raise RuntimeError(f"GQL Error: {r.text}")
        return r.text

    def exec_to_df(self, query):
        import pandas as pd
        res = self.exec_gql(query)
        data = json.loads(res)
        return pd.DataFrame(data)

# Fragment and Condition classes for pyragedb-semantics

def format_literal(val):
    if isinstance(val, str):
        return f"'{val}'"
    elif isinstance(val, bool):
        return "true" if val else "false"
    elif val is None:
        return "null"
    else:
        return str(val)


class NotCondition:
    def __init__(self, condition):
        self.condition = condition


class Alias:
    def __init__(self, value, name):
        self.value = value
        self.name = name


class Data:
    def __init__(self, data_ref, name=None):
        self.data_ref = data_ref
        self.name = name


class ArithmeticExpr:
    def __init__(self, op, left, right):
        self.op = op
        self.left = left
        self.right = right

    def __add__(self, other):
        return ArithmeticExpr("+", self, other)
    def __sub__(self, other):
        return ArithmeticExpr("-", self, other)
    def __mul__(self, other):
        return ArithmeticExpr("*", self, other)
    def __truediv__(self, other):
        return ArithmeticExpr("/", self, other)
    def __radd__(self, other):
        return ArithmeticExpr("+", other, self)
    def __rsub__(self, other):
        return ArithmeticExpr("-", other, self)
    def __rmul__(self, other):
        return ArithmeticExpr("*", other, self)
    def __rtruediv__(self, other):
        return ArithmeticExpr("/", other, self)

    def __eq__(self, other):
        return Condition("eq", self, other)
    def __ne__(self, other):
        return Condition("ne", self, other)
    def __lt__(self, other):
        return Condition("lt", self, other)
    def __le__(self, other):
        return Condition("le", self, other)
    def __gt__(self, other):
        return Condition("gt", self, other)
    def __ge__(self, other):
        return Condition("ge", self, other)

    def alias(self, name):
        return Alias(self, name)


class FallbackExpr:
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __or__(self, other):
        return FallbackExpr(self, other)

    def alias(self, name):
        return Alias(self, name)

    def __eq__(self, other):
        return Condition("eq", self, other)


class AsBoolExpr:
    def __init__(self, expr):
        self.expr = expr

    def alias(self, name):
        return Alias(self, name)


class FunctionCallExpr:
    def __init__(self, name, *args):
        self.name = name
        self.args = args

    def alias(self, name):
        return Alias(self, name)

    def __eq__(self, other):
        return Condition("eq", self, other)
    def __ne__(self, other):
        return Condition("ne", self, other)
    def __lt__(self, other):
        return Condition("lt", self, other)
    def __le__(self, other):
        return Condition("le", self, other)
    def __gt__(self, other):
        return Condition("gt", self, other)
    def __ge__(self, other):
        return Condition("ge", self, other)


class AggregateBuilder:
    def __init__(self, op, target):
        self.op = op  # "count", "sum", "avg", "min", "max"
        self.target = target
        self.group_by_concepts = []
        self.where_conditions = []

    @property
    def group_by_concept(self):
        return self.group_by_concepts[0] if self.group_by_concepts else None

    @group_by_concept.setter
    def group_by_concept(self, value):
        self.group_by_concepts = [value] if value is not None else []

    def per(self, *concepts):
        self.group_by_concepts.extend(concepts)
        return self

    def where(self, *conditions):
        self.where_conditions.extend(conditions)
        return self

    def alias(self, name):
        return Alias(self, name)


class OrderSpec:
    def __init__(self, expr, direction="ASC"):
        self.expr = expr
        self.direction = direction

    def alias(self, name):
        return Alias(self, name)


class RankBuilder:
    def __init__(self, *sort_keys):
        self.sort_keys = sort_keys
        self.group_by_concepts = []

    def per(self, *concepts):
        self.group_by_concepts.extend(concepts)
        return self

    def alias(self, name):
        return Alias(self, name)


class TopBuilder:
    def __init__(self, limit, *sort_keys, op="top"):
        self.limit = limit
        self.sort_keys = sort_keys
        self.op = op  # "top" or "bottom"
        self.group_by_concepts = []

    def per(self, *concepts):
        self.group_by_concepts.extend(concepts)
        return self

    def alias(self, name):
        return Alias(self, name)


class Condition:
    def __init__(self, op, left, right):
        self.op = op  # "eq", "ne", "lt", "le", "gt", "ge"
        self.left = left
        self.right = right

    def __and__(self, other):
        return AndCondition(self, other)

    def __or__(self, other):
        return OrCondition(self, other)


class AndCondition:
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __and__(self, other):
        return AndCondition(self, other)

    def __or__(self, other):
        return OrCondition(self, other)


class OrCondition:
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __and__(self, other):
        return AndCondition(self, other)

    def __or__(self, other):
        return OrCondition(self, other)


class CombinedFragment:
    def __init__(self, op, left, right):
        self.op = op  # "or", "and"
        self.left = left
        self.right = right

    def select(self, *projections):
        return CombinedFragment(self.op, self.left.select(*projections), self.right.select(*projections))

    def where(self, *conditions):
        return CombinedFragment(self.op, self.left.where(*conditions), self.right.where(*conditions))

    def distinct(self):
        return CombinedFragment(self.op, self.left.distinct(), self.right.distinct())

    def limit(self, count):
        return CombinedFragment(self.op, self.left.limit(count), self.right.limit(count))

    def order_by(self, *specs):
        return CombinedFragment(self.op, self.left.order_by(*specs), self.right.order_by(*specs))

    def to_gql(self):
        if self.op == "or":
            return f"{self.left.to_gql()} UNION {self.right.to_gql()}"
        return f"{self.left.to_gql()} INTERSECT {self.right.to_gql()}"

    def to_df(self):
        import pandas as pd
        return pd.DataFrame(self.to_dict())

    def to_dict(self):
        import json
        self.left.model.check_requirements()
        res = self.left.model.exec_gql(self.to_gql())
        return json.loads(res)

    def __and__(self, other):
        return CombinedFragment("and", self, other)

    def __or__(self, other):
        if isinstance(other, (Fragment, CombinedFragment)):
            return CombinedFragment("or", self, other)
        return FallbackExpr(self, other)


class Fragment:
    def __init__(self, model, concepts=None, conditions=None, projections=None, order_by=None, limit_val=None, is_distinct=False):
        self.model = model
        self.concepts = concepts or []
        self.conditions = conditions or []
        self.is_distinct = is_distinct
        
        processed_projections = []
        for proj in (projections or []):
            if isinstance(proj, Fragment):
                processed_projections.extend(proj.projections)
                if proj.is_distinct:
                    self.is_distinct = True
            else:
                processed_projections.append(proj)
        self.projections = processed_projections
        
        self.order_by_specs = order_by or []
        self.limit_val = limit_val

    def where(self, *conditions):
        new_conditions = list(self.conditions)
        for cond in conditions:
            if isinstance(cond, Fragment):
                new_conditions.extend(cond.conditions)
            else:
                new_conditions.append(cond)
        return Fragment(
            self.model,
            concepts=self.concepts,
            conditions=new_conditions,
            projections=self.projections,
            order_by=self.order_by_specs,
            limit_val=self.limit_val,
            is_distinct=self.is_distinct
        )

    def require(self, *conditions, name=None):
        self.model.require(*conditions, name=name, scope=self)

    def select(self, *projections):
        return Fragment(
            self.model,
            concepts=self.concepts,
            conditions=self.conditions,
            projections=list(projections),
            order_by=self.order_by_specs,
            limit_val=self.limit_val,
            is_distinct=self.is_distinct
        )

    def distinct(self):
        return Fragment(
            self.model,
            concepts=self.concepts,
            conditions=self.conditions,
            projections=self.projections,
            order_by=self.order_by_specs,
            limit_val=self.limit_val,
            is_distinct=True
        )

    def limit(self, count):
        return Fragment(
            self.model,
            concepts=self.concepts,
            conditions=self.conditions,
            projections=self.projections,
            order_by=self.order_by_specs,
            limit_val=count,
            is_distinct=self.is_distinct
        )

    def order_by(self, *specs):
        return Fragment(
            self.model,
            concepts=self.concepts,
            conditions=self.conditions,
            projections=self.projections,
            order_by=list(specs),
            limit_val=self.limit_val,
            is_distinct=self.is_distinct
        )

    def __and__(self, other):
        return self.where(other)

    def __or__(self, other):
        if isinstance(other, (Fragment, CombinedFragment)):
            return CombinedFragment("or", self, other)
        return FallbackExpr(self, other)

    def to_gql(self):
        from .concept import Concept, AttributeRef, ConceptRef, AttributeRefRef
        
        # 1. Collect all referenced concepts in conditions and projections
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
        
        for cond in self.conditions:
            find_concepts(cond)
            
        for proj in self.projections:
            find_concepts(proj)
            
        for spec in self.order_by_specs:
            find_concepts(spec)
            
        # 2. Build var_map
        var_map = {}
        for c in referenced_concepts:
            if isinstance(c, (ConceptRef, AttributeRefRef)):
                var_map[c] = c._ref_name
            else:
                var_map[c] = c._name.lower()
            
        matches = []
        where_filters = []
        
        # Helper to resolve refs
        def resolve_ref(ref):
            if isinstance(ref, Alias):
                return f"{resolve_ref(ref.value)} AS {ref.name}"
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
                
            parent = ref._parent if isinstance(ref, AttributeRef) else None
            if parent is not None:
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


            if isinstance(ref, ArithmeticExpr):
                return f"({resolve_ref(ref.left)} {ref.op} {resolve_ref(ref.right)})"

            if isinstance(ref, FallbackExpr):
                return f"coalesce({resolve_ref(ref.left)}, {resolve_ref(ref.right)})"

            if isinstance(ref, AsBoolExpr):
                return f"({resolve_ref(ref.expr)} IS NOT NULL)"

            if isinstance(ref, FunctionCallExpr):
                gql_args = []
                for arg in ref.args:
                    if isinstance(arg, list):
                        gql_args.append("[" + ", ".join(resolve_ref(item) for item in arg) + "]")
                    else:
                        gql_args.append(resolve_ref(arg))
                name = ref.name.lower()
                if name == "strings.lower":
                    return f"lower({gql_args[0]})"
                elif name == "strings.upper":
                    return f"upper({gql_args[0]})"
                elif name == "strings.strip":
                    return f"trim({gql_args[0]})"
                elif name == "strings.concat":
                    return f"({gql_args[0]} + {gql_args[1]})"
                elif name == "strings.join":
                    sep = gql_args[1] if len(gql_args) > 1 else "''"
                    list_arg = ref.args[0]
                    if isinstance(list_arg, list):
                        parts = [resolve_ref(item) for item in list_arg]
                        return f" + {sep} + ".join(parts)
                    return f"({gql_args[0]})"
                elif name == "strings.string":
                    return f"toString({gql_args[0]})"
                elif name == "strings.replace":
                    return f"replace({gql_args[0]}, {gql_args[1]}, {gql_args[2]})"
                elif name == "strings.like":
                    return f"{gql_args[0]} LIKE {gql_args[1]}"
                elif name == "strings.startswith":
                    return f"{gql_args[0]} STARTS WITH {gql_args[1]}"
                elif name == "strings.endswith":
                    return f"{gql_args[0]} ENDS WITH {gql_args[1]}"
                elif name == "strings.split_part":
                    return f"split({gql_args[0]}, {gql_args[1]})[{gql_args[2]}]"
                elif name == "numbers.integer":
                    return f"toInteger({gql_args[0]})"
                elif name in ("floats.float", "numbers.float"):
                    return f"toFloat({gql_args[0]})"
                elif name == "numbers.number":
                    return f"toFloat({gql_args[0]})"
                elif name == "numbers.parse_number":
                    return f"toFloat({gql_args[0]})"
                elif name == "math.clip":
                    return f"(CASE WHEN {gql_args[0]} < {gql_args[1]} THEN {gql_args[1]} WHEN {gql_args[0]} > {gql_args[2]} THEN {gql_args[2]} ELSE {gql_args[0]} END)"
                elif name == "math.round":
                    if len(gql_args) > 1:
                        digits = ref.args[1]
                        if isinstance(digits, int):
                            mult = 10**digits
                            return f"(toFloat(round({gql_args[0]} * {mult})) / {float(mult)})"
                    return f"round({gql_args[0]})"
                elif name == "math.floor":
                    return f"floor({gql_args[0]})"
                elif name == "math.ceil":
                    return f"ceil({gql_args[0]})"
                elif name in ("datetime.datetime", "datetime.date"):
                    return f"({gql_args[0]})"
                return f"{ref.name.split('.')[-1]}({', '.join(gql_args)})"

            if isinstance(ref, OrderSpec):
                return f"{resolve_ref(ref.expr)} {ref.direction}"

            if isinstance(ref, RankBuilder):
                return "null"

            if isinstance(ref, TopBuilder):
                return "true"

            if isinstance(ref, Fragment):
                distinct_str = "DISTINCT " if ref.is_distinct else ""
                return f"{distinct_str}{', '.join(resolve_ref(p) for p in ref.projections)}"

            if isinstance(ref, AggregateBuilder):
                target_expr = resolve_ref(ref.target)
                op_map = {
                    "count": "count", "sum": "sum", "avg": "avg", "min": "min", "max": "max"
                }
                return f"{op_map[ref.op]}({target_expr})"

            if not isinstance(ref, AttributeRef):
                return format_literal(ref)


        # Compile conditions
        def compile_cond(cond):
            if isinstance(cond, Condition):
                left_expr = resolve_ref(cond.left)
                right_expr = resolve_ref(cond.right)
                op_map = {
                    "eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="
                }
                return f"{left_expr} {op_map[cond.op]} {right_expr}"
            elif isinstance(cond, NotCondition):
                return f"NOT ({compile_cond(cond.condition)})"
            elif isinstance(cond, AndCondition):
                return f"({compile_cond(cond.left)} AND {compile_cond(cond.right)})"
            elif isinstance(cond, OrCondition):
                return f"({compile_cond(cond.left)} OR {compile_cond(cond.right)})"
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

        # Scan for global top-K filter
        global_top_filter = None
        for cond in self.conditions:
            if isinstance(cond, TopBuilder) and not cond.group_by_concepts:
                global_top_filter = cond
                break

        extra_conditions = []
        def collect_extra_conditions(node):
            if isinstance(node, Alias):
                collect_extra_conditions(node.value)
            elif isinstance(node, AggregateBuilder):
                extra_conditions.extend(node.where_conditions)
                
        for proj in self.projections:
            collect_extra_conditions(proj)

        all_conditions = self.conditions + extra_conditions
        for cond in all_conditions:
            if cond is global_top_filter:
                continue
            f_str = compile_cond(cond)
            if f_str != "true":
                where_filters.append(f_str)

        # Collect client-side operations to post-process
        client_ranks = []
        client_tops = []

        for proj in self.projections:
            if isinstance(proj, Alias) and isinstance(proj.value, RankBuilder):
                client_ranks.append({
                    "alias_name": proj.name,
                    "sort_keys": proj.value.sort_keys,
                    "group_by_keys": proj.value.group_by_concepts
                })

        for cond in self.conditions:
            if isinstance(cond, TopBuilder) and cond.group_by_concepts:
                client_tops.append({
                    "limit": cond.limit,
                    "sort_keys": cond.sort_keys,
                    "group_by_keys": cond.group_by_concepts,
                    "op": cond.op
                })

        hidden_projections = []
        sort_key_alias_map = {}
        group_key_alias_map = {}
        alias_counter = 0

        def get_expr(key):
            return key.expr if isinstance(key, OrderSpec) else key

        for r in client_ranks:
            sort_key_alias_map[id(r)] = []
            for key in r["sort_keys"]:
                alias_name = f"__sort_key_{alias_counter}"
                alias_counter += 1
                hidden_projections.append(f"{resolve_ref(get_expr(key))} AS {alias_name}")
                sort_key_alias_map[id(r)].append(alias_name)
            
            group_key_alias_map[id(r)] = []
            for key in r["group_by_keys"]:
                alias_name = f"__group_key_{alias_counter}"
                alias_counter += 1
                hidden_projections.append(f"{resolve_ref(get_expr(key))} AS {alias_name}")
                group_key_alias_map[id(r)].append(alias_name)

        for t in client_tops:
            sort_key_alias_map[id(t)] = []
            for key in t["sort_keys"]:
                alias_name = f"__sort_key_{alias_counter}"
                alias_counter += 1
                hidden_projections.append(f"{resolve_ref(get_expr(key))} AS {alias_name}")
                sort_key_alias_map[id(t)].append(alias_name)
            
            group_key_alias_map[id(t)] = []
            for key in t["group_by_keys"]:
                alias_name = f"__group_key_{alias_counter}"
                alias_counter += 1
                hidden_projections.append(f"{resolve_ref(get_expr(key))} AS {alias_name}")
                group_key_alias_map[id(t)].append(alias_name)

        # Store post-processing metadata
        self._post_process_meta = {
            "ranks": [
                {
                    "alias_name": r["alias_name"],
                    "sort_keys_meta": [
                        (sort_alias, key.direction if isinstance(key, OrderSpec) else "ASC")
                        for sort_alias, key in zip(sort_key_alias_map[id(r)], r["sort_keys"])
                    ],
                    "group_by_aliases": group_key_alias_map[id(r)]
                }
                for r in client_ranks
            ],
            "tops": [
                {
                    "limit": t["limit"],
                    "op": t["op"],
                    "sort_keys_meta": [
                        (sort_alias, key.direction if isinstance(key, OrderSpec) else ("DESC" if t["op"] == "top" else "ASC"))
                        for sort_alias, key in zip(sort_key_alias_map[id(t)], t["sort_keys"])
                    ],
                    "group_by_aliases": group_key_alias_map[id(t)]
                }
                for t in client_tops
            ]
        }

        # Local order_by and limit
        local_order_by_specs = list(self.order_by_specs)
        local_limit_val = self.limit_val

        if global_top_filter:
            for key in global_top_filter.sort_keys:
                if isinstance(key, OrderSpec):
                    local_order_by_specs.append(key)
                else:
                    direction = "DESC" if global_top_filter.op == "top" else "ASC"
                    local_order_by_specs.append(OrderSpec(key, direction))
            local_limit_val = global_top_filter.limit

        # 3. Assemble GQL query string
        gql = ""
        
        # If no matches are found, default to referencing self.concepts if provided
        if not matches:
            for c in self.concepts:
                var = c._name.lower()
                matches.append(f"({var}:{c._name})")
                
        if matches:
            gql += "MATCH " + ", ".join(matches)
            
        if where_filters:
            gql += " WHERE " + " AND ".join(where_filters)
            
        # Projections
        proj_exprs = []
        for proj in self.projections:
            proj_exprs.append(resolve_ref(proj))
            
        if not proj_exprs:
            # Default to all matched concept variables
            for m_str in matches:
                import re
                vars_in_match = re.findall(r'\(([a-zA-Z0-9_]+):', m_str)
                for var in vars_in_match:
                    if var not in proj_exprs:
                        proj_exprs.append(var)

        # Append hidden projections
        proj_exprs.extend(hidden_projections)
                    
        if proj_exprs:
            distinct_str = " DISTINCT" if self.is_distinct else ""
            gql += f" RETURN{distinct_str} " + ", ".join(proj_exprs)
            
        # Order by
        if local_order_by_specs:
            sort_exprs = []
            for spec in local_order_by_specs:
                sort_exprs.append(resolve_ref(spec))
            gql += " ORDER BY " + ", ".join(sort_exprs)
            
        # Limit
        if local_limit_val is not None:
            gql += f" LIMIT {local_limit_val}"
            
        return gql

    def to_df(self):
        import pandas as pd
        return pd.DataFrame(self.to_dict())

    def to_dict(self):
        import json
        self.model.check_requirements()
        gql = self.to_gql()
        res = self.model.exec_gql(gql)
        rows = json.loads(res)
        
        meta = getattr(self, "_post_process_meta", None)
        if meta:
            # 1. Handle Ranking
            for rank_meta in meta.get("ranks", []):
                alias_name = rank_meta["alias_name"]
                sort_keys_meta = rank_meta["sort_keys_meta"]
                group_by_aliases = rank_meta["group_by_aliases"]
                
                groups = {}
                for row in rows:
                    g_key = tuple(row.get(g) for g in group_by_aliases) if group_by_aliases else ()
                    groups.setdefault(g_key, []).append(row)
                    
                sorted_rows = []
                for g_key in sorted(groups.keys()):
                    group_rows = groups[g_key]
                    # Stable sort descending/ascending
                    for sort_alias, direction in reversed(sort_keys_meta):
                        is_reverse = (direction == "DESC")
                        def sort_key_fn(r, sa=sort_alias, ir=is_reverse):
                            val = r.get(sa)
                            if val is None:
                                return (-1, 0) if ir else (1, 0)
                            return (0, val)
                        group_rows.sort(key=sort_key_fn, reverse=is_reverse)
                    
                    # Compute ranks (standard ranking 1, 2, 2, 4)
                    current_rank = 1
                    prev_keys = None
                    for idx, r in enumerate(group_rows):
                        r_keys = tuple(r.get(sort_alias) for sort_alias, _ in sort_keys_meta)
                        if prev_keys is not None and r_keys != prev_keys:
                            current_rank = idx + 1
                        r[alias_name] = current_rank
                        prev_keys = r_keys
                    sorted_rows.extend(group_rows)
                rows = sorted_rows

            # 2. Handle Grouped Top-K Filtering
            filtered_rows = list(rows)
            for top_meta in meta.get("tops", []):
                limit = top_meta["limit"]
                sort_keys_meta = top_meta["sort_keys_meta"]
                group_by_aliases = top_meta["group_by_aliases"]
                
                groups = {}
                for row in filtered_rows:
                    g_key = tuple(row.get(g) for g in group_by_aliases) if group_by_aliases else ()
                    groups.setdefault(g_key, []).append(row)
                    
                new_filtered_rows = []
                for g_key, group_rows in groups.items():
                    for sort_alias, direction in reversed(sort_keys_meta):
                        is_reverse = (direction == "DESC")
                        def sort_key_fn(r, sa=sort_alias, ir=is_reverse):
                            val = r.get(sa)
                            if val is None:
                                return (-1, 0) if ir else (1, 0)
                            return (0, val)
                        group_rows.sort(key=sort_key_fn, reverse=is_reverse)
                    
                    new_filtered_rows.extend(group_rows[:limit])
                filtered_rows = new_filtered_rows
            rows = filtered_rows

            # 3. Clean up hidden columns
            cleaned_rows = []
            for r in rows:
                cleaned_row = {k: v for k, v in r.items() if not (k.startswith("__sort_key_") or k.startswith("__group_key_"))}
                cleaned_rows.append(cleaned_row)
            rows = cleaned_rows
            
        return rows

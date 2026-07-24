/*
 * Copyright RageDB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// The query-analysis helpers in OptimizerUtils: turning a parsed query/expression into the variable,
// interval and filter facts the passes reason over. These sit above the primitive Interval/equivalence
// helpers pinned in OptimizerUtilsTests.cpp -- extract_intervals_from_expr (the pruners' unsatisfiability
// analysis), collect_query_vars / collect_all_query_vars (the VarInfo the selectivity/pushdown passes read),
// is_variable_referenced_outside_count (the COUNT{} projection gate) and rebuild_expression_without_pushed_predicates
// (the post-pushdown residual). All AST/value level, so they are pinned directly rather than through a query result.

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/optimizer/OptimizerUtils.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
// Extract the range intervals a WHERE predicate implies for variable x. This is the analysis that lets the
// contradiction/domain pruners decide a query is unsatisfiable (an empty interval), so its operator mapping,
// operand reversal, and NOT/De-Morgan handling all have to be exact.
std::map<std::string, Interval> intervals_for(const std::string& where) {
    auto q = GqlParser::parse("MATCH (x) WHERE " + where + " RETURN x");
    std::map<std::string, Interval> ivs;
    extract_intervals_from_expr(q.where_expr.get(), "x", ivs, false);
    return ivs;
}
}  // namespace

TEST_CASE("extract_intervals_from_expr maps each comparison operator to a bound", "[gql_optimizer]") {
    SECTION("x.age < 5 is an exclusive upper bound") {
        auto iv = intervals_for("x.age < 5").at("age");
        REQUIRE(iv.has_upper); REQUIRE(iv.upper_val == 5); REQUIRE_FALSE(iv.upper_inclusive);
        REQUIRE_FALSE(iv.has_lower);
    }
    SECTION("x.age <= 5 is an inclusive upper bound") {
        auto iv = intervals_for("x.age <= 5").at("age");
        REQUIRE(iv.has_upper); REQUIRE(iv.upper_val == 5); REQUIRE(iv.upper_inclusive);
    }
    SECTION("x.age > 5 is an exclusive lower bound") {
        auto iv = intervals_for("x.age > 5").at("age");
        REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 5); REQUIRE_FALSE(iv.lower_inclusive);
        REQUIRE_FALSE(iv.has_upper);
    }
    SECTION("x.age >= 5 is an inclusive lower bound") {
        auto iv = intervals_for("x.age >= 5").at("age");
        REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 5); REQUIRE(iv.lower_inclusive);
    }
    SECTION("x.age = 5 pins both bounds to the same point") {
        auto iv = intervals_for("x.age = 5").at("age");
        REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 5); REQUIRE(iv.lower_inclusive);
        REQUIRE(iv.has_upper); REQUIRE(iv.upper_val == 5); REQUIRE(iv.upper_inclusive);
        REQUIRE_FALSE(iv.is_empty());
    }
}

TEST_CASE("extract_intervals_from_expr normalizes a comparison with the constant on the left", "[gql_optimizer]") {
    SECTION("5 < x.age is read as x.age > 5") {
        auto iv = intervals_for("5 < x.age").at("age");
        REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 5); REQUIRE_FALSE(iv.lower_inclusive);
    }
    SECTION("5 >= x.age is read as x.age <= 5") {
        auto iv = intervals_for("5 >= x.age").at("age");
        REQUIRE(iv.has_upper); REQUIRE(iv.upper_val == 5); REQUIRE(iv.upper_inclusive);
    }
}

TEST_CASE("extract_intervals_from_expr intersects an AND and detects a contradiction", "[gql_optimizer]") {
    SECTION("x.age >= 3 AND x.age <= 10 is the closed range [3, 10]") {
        auto iv = intervals_for("x.age >= 3 AND x.age <= 10").at("age");
        REQUIRE(iv.lower_val == 3); REQUIRE(iv.upper_val == 10);
        REQUIRE_FALSE(iv.is_empty());
    }
    SECTION("x.age >= 10 AND x.age <= 3 is an empty interval -- the pruner's unsatisfiable case") {
        auto iv = intervals_for("x.age >= 10 AND x.age <= 3").at("age");
        REQUIRE(iv.is_empty());
    }
}

TEST_CASE("extract_intervals_from_expr applies NOT and De Morgan", "[gql_optimizer]") {
    SECTION("NOT (x.age < 5) flips to x.age >= 5") {
        auto iv = intervals_for("NOT (x.age < 5)").at("age");
        REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 5); REQUIRE(iv.lower_inclusive);
    }
    SECTION("NOT (x.age > 3 OR x.age > 10) becomes x.age <= 3 (the tighter bound)") {
        auto iv = intervals_for("NOT (x.age > 3 OR x.age > 10)").at("age");
        REQUIRE(iv.has_upper); REQUIRE(iv.upper_val == 3); REQUIRE(iv.upper_inclusive);
        REQUIRE_FALSE(iv.has_lower);
    }
    SECTION("NOT (x.age = 5) yields no interval -- inequality is not a single range") {
        REQUIRE(intervals_for("NOT (x.age = 5)").empty());
    }
    SECTION("NOT (x.age > 3 AND x.age < 10) yields no interval -- a negated conjunction is a disjunction") {
        REQUIRE(intervals_for("NOT (x.age > 3 AND x.age < 10)").empty());
    }
}

TEST_CASE("extract_intervals_from_expr ignores predicates outside the target variable's scope", "[gql_optimizer]") {
    SECTION("a comparison on a different variable contributes nothing") {
        REQUIRE(intervals_for("y.age > 5").empty());
    }
    SECTION("a non-numeric comparison contributes nothing") {
        REQUIRE(intervals_for("x.name > 'foo'").empty());
    }
}

namespace {
const VarInfo* find_var(const std::vector<VarInfo>& vs, const std::string& name) {
    for (const auto& v : vs) {
        if (v.variable == name) return &v;
    }
    return nullptr;
}
}  // namespace

TEST_CASE("collect_query_vars gathers node variables but excludes edges", "[gql_optimizer]") {
    auto vs = collect_query_vars(GqlParser::parse("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a"));
    REQUIRE(vs.size() == 2);
    REQUIRE(find_var(vs, "a") != nullptr);
    REQUIRE(find_var(vs, "b") != nullptr);
    REQUIRE(find_var(vs, "e") == nullptr);          // the edge variable is a node-only pass concern
    REQUIRE(find_var(vs, "a")->label == "Person");
}

TEST_CASE("collect_all_query_vars also gathers edge variables", "[gql_optimizer]") {
    auto vs = collect_all_query_vars(GqlParser::parse("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a"));
    REQUIRE(vs.size() == 3);
    const VarInfo* e = find_var(vs, "e");
    REQUIRE(e != nullptr);
    REQUIRE(e->label == "KNOWS");
}

TEST_CASE("collect_query_vars skips anonymous nodes and edges", "[gql_optimizer]") {
    auto vs = collect_query_vars(GqlParser::parse("MATCH (a:Person)-[:KNOWS]->() RETURN a"));
    REQUIRE(vs.size() == 1);
    REQUIRE(vs[0].variable == "a");
}

TEST_CASE("collect_query_vars records a label only for a plain literal label", "[gql_optimizer]") {
    SECTION("a single label is captured") {
        auto vs = collect_query_vars(GqlParser::parse("MATCH (a:Person) RETURN a"));
        REQUIRE(find_var(vs, "a")->label == "Person");
    }
    SECTION("a composite label expression leaves the label empty (subsumption handles it elsewhere)") {
        auto vs = collect_query_vars(GqlParser::parse("MATCH (a:Person&Employee) RETURN a"));
        REQUIRE(find_var(vs, "a")->label.empty());
    }
}

TEST_CASE("collect_query_vars folds property constraints into a variable's intervals", "[gql_optimizer]") {
    auto interval_of = [](const std::string& q, const std::string& prop) {
        return collect_query_vars(GqlParser::parse(q)).at(0).intervals.at(prop);
    };
    SECTION("an inline map property is an equality point") {
        auto iv = interval_of("MATCH (a:Person {age: 30}) RETURN a", "age");
        REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 30); REQUIRE(iv.lower_inclusive);
        REQUIRE(iv.has_upper); REQUIRE(iv.upper_val == 30); REQUIRE(iv.upper_inclusive);
    }
    SECTION("a segment WHERE comparison becomes a bound") {
        auto iv = interval_of("MATCH (a:Person) WHERE a.age > 30 RETURN a", "age");
        REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 30); REQUIRE_FALSE(iv.lower_inclusive);
    }
    SECTION("an inline node WHERE comparison becomes the same bound") {
        auto iv = interval_of("MATCH (a:Person WHERE a.age > 30) RETURN a", "age");
        REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 30); REQUIRE_FALSE(iv.lower_inclusive);
    }
}

TEST_CASE("collect_all_query_vars folds an edge's map property into its intervals", "[gql_optimizer]") {
    auto vs = collect_all_query_vars(GqlParser::parse("MATCH (a)-[e:KNOWS {weight: 5}]->(b) RETURN a"));
    const VarInfo* e = find_var(vs, "e");
    REQUIRE(e != nullptr);
    auto iv = e->intervals.at("weight");
    REQUIRE(iv.has_lower); REQUIRE(iv.lower_val == 5); REQUIRE(iv.lower_inclusive);
    REQUIRE(iv.has_upper); REQUIRE(iv.upper_val == 5); REQUIRE(iv.upper_inclusive);
}

namespace {
// Whether variable x is referenced anywhere in the RETURN expression except inside a count(). This gate
// decides whether a count-only variable can be dropped from a subquery's projection, so the count
// short-circuit has to be exact.
bool references_outside_count(const std::string& e) {
    auto q = GqlParser::parse("MATCH (x), (y) RETURN " + e + " AS r");
    return is_variable_referenced_outside_count(q.returns[0].expr.get(), "x");
}
}  // namespace

TEST_CASE("is_variable_referenced_outside_count ignores references inside count()", "[gql_optimizer]") {
    SECTION("a variable used only inside count is not an outside reference") {
        REQUIRE_FALSE(references_outside_count("count(x)"));
        REQUIRE_FALSE(references_outside_count("count(DISTINCT x)"));
        REQUIRE_FALSE(references_outside_count("count(x) > 5"));   // only appearance is the count argument
    }
    SECTION("the same variable used outside the count is an outside reference") {
        REQUIRE(references_outside_count("count(x) + x.age"));
    }
    SECTION("a non-COUNT aggregate does reference its argument (only COUNT short-circuits)") {
        REQUIRE(references_outside_count("sum(x.age)"));
    }
}

TEST_CASE("is_variable_referenced_outside_count walks every expression form", "[gql_optimizer]") {
    REQUIRE(references_outside_count("x"));                                       // bare variable
    REQUIRE(references_outside_count("x.age"));                                   // property lookup
    REQUIRE(references_outside_count("abs(x.age)"));                              // function-call argument
    REQUIRE(references_outside_count("CASE WHEN x.age > 5 THEN 1 ELSE 0 END"));   // case branch
    REQUIRE(references_outside_count("NOT (x.age > 5)"));                         // unary over binary
    REQUIRE(references_outside_count("x.age IN [1, 2]"));                         // in-list value
    // A different variable or a bare literal is not a reference to x.
    REQUIRE_FALSE(references_outside_count("y.age"));
    REQUIRE_FALSE(references_outside_count("42"));
}

TEST_CASE("rebuild_expression_without_pushed_predicates removes exactly the pushed conjuncts", "[gql_optimizer]") {
    // extract_filters populates the pushdown map from a WHERE; rebuild then drops precisely those conjuncts
    // so a pushed predicate is not re-applied after the scan. The two have to agree on what was pushed.
    auto pushdowns_of = [](Expression* w) {
        std::map<std::string, std::vector<PropertyFilter>> pd;
        extract_filters(w, pd);
        return pd;
    };

    SECTION("every conjunct pushed leaves no residual expression") {
        auto q = GqlParser::parse("MATCH (a:P) WHERE a.x = 1 AND a.y > 2 RETURN a");
        auto pd = pushdowns_of(q.where_expr.get());
        auto rebuilt = rebuild_expression_without_pushed_predicates(std::move(q.where_expr), pd);
        REQUIRE(rebuilt == nullptr);
    }
    SECTION("a conjunct that was not pushed survives") {
        // a.x = a.y is property-to-property, so extract_filters never pushes it; only a.x = 1 is pushed,
        // and the surviving residual is the a.x = a.y comparison.
        auto q = GqlParser::parse("MATCH (a:P) WHERE a.x = 1 AND a.x = a.y RETURN a");
        auto pd = pushdowns_of(q.where_expr.get());
        auto rebuilt = rebuild_expression_without_pushed_predicates(std::move(q.where_expr), pd);
        REQUIRE(rebuilt != nullptr);
        REQUIRE(rebuilt->kind == ExpressionKind::BINARY_OP);
        REQUIRE(static_cast<const BinaryOpExpr*>(rebuilt.get())->op == BinaryOpKind::EQ);
    }
    SECTION("an OR is never pushed, so it is kept whole") {
        auto q = GqlParser::parse("MATCH (a:P) WHERE a.x = 1 OR a.y = 2 RETURN a");
        auto pd = pushdowns_of(q.where_expr.get());
        REQUIRE(pd.empty());
        auto rebuilt = rebuild_expression_without_pushed_predicates(std::move(q.where_expr), pd);
        REQUIRE(rebuilt != nullptr);
        REQUIRE(static_cast<const BinaryOpExpr*>(rebuilt.get())->op == BinaryOpKind::OR);
    }
    SECTION("with no pushdowns nothing is removed") {
        auto q = GqlParser::parse("MATCH (a:P) WHERE a.x = 1 RETURN a");
        std::map<std::string, std::vector<PropertyFilter>> empty;
        auto rebuilt = rebuild_expression_without_pushed_predicates(std::move(q.where_expr), empty);
        REQUIRE(rebuilt != nullptr);
    }
}

namespace {
SelectivityClass selectivity(const std::string& q, const std::string& var) {
    return estimate_selectivity(var, collect_query_vars(GqlParser::parse(q)));
}
}  // namespace

TEST_CASE("estimate_selectivity ranks a unique id equality above a range above a scan", "[gql_optimizer]") {
    SECTION("an unconstrained variable is a full scan") {
        REQUIRE(selectivity("MATCH (a:Person) RETURN a", "a") == SelectivityClass::SCAN);
    }
    SECTION("an absent or empty variable name is a scan") {
        REQUIRE(selectivity("MATCH (a:Person) RETURN a", "z") == SelectivityClass::SCAN);
        REQUIRE(selectivity("MATCH (a:Person) RETURN a", "") == SelectivityClass::SCAN);
    }
    SECTION("an id equality point is UNIQUE") {
        REQUIRE(selectivity("MATCH (a:Person {id: 5}) RETURN a", "a") == SelectivityClass::UNIQUE);
    }
    SECTION("a non-id constraint is INDEXED, not unique") {
        REQUIRE(selectivity("MATCH (a:Person {age: 30}) RETURN a", "a") == SelectivityClass::INDEXED);
    }
    SECTION("an id range rather than a single point is INDEXED, not unique") {
        REQUIRE(selectivity("MATCH (a:Person) WHERE a.id > 5 RETURN a", "a") == SelectivityClass::INDEXED);
    }
}

TEST_CASE("reverse_match_pattern_if_safe reverses topology and flips edge directions", "[gql_optimizer]") {
    SECTION("a RIGHT hop reverses node order and becomes a LEFT hop") {
        auto q = GqlParser::parse("MATCH (a)-[:R]->(b) RETURN a");
        auto& m = q.matches[0];
        REQUIRE(reverse_match_pattern_if_safe(m));
        REQUIRE(m.pattern.nodes[0].variable == "b");
        REQUIRE(m.pattern.nodes[1].variable == "a");
        REQUIRE(m.pattern.edges[0].direction == EdgeDirection::LEFT);
    }
    SECTION("a LEFT hop becomes a RIGHT hop") {
        auto q = GqlParser::parse("MATCH (a)<-[:R]-(b) RETURN a");
        auto& m = q.matches[0];
        REQUIRE(reverse_match_pattern_if_safe(m));
        REQUIRE(m.pattern.edges[0].direction == EdgeDirection::RIGHT);
    }
    SECTION("an undirected hop keeps its direction but still reverses node order") {
        auto q = GqlParser::parse("MATCH (a)-[:R]-(b) RETURN a");
        auto& m = q.matches[0];
        REQUIRE(reverse_match_pattern_if_safe(m));
        REQUIRE(m.pattern.nodes[0].variable == "b");
        REQUIRE(m.pattern.edges[0].direction == EdgeDirection::ANY);
    }
    SECTION("a bound path variable makes reversal unsafe, so it is declined and left unchanged") {
        auto q = GqlParser::parse("MATCH p = (a)-[:R]->(b) RETURN a");
        auto& m = q.matches[0];
        REQUIRE_FALSE(reverse_match_pattern_if_safe(m));
        REQUIRE(m.pattern.nodes[0].variable == "a");                       // unchanged
        REQUIRE(m.pattern.edges[0].direction == EdgeDirection::RIGHT);
    }
    SECTION("a shortest-path selector is also declined") {
        auto q = GqlParser::parse("MATCH p = ANY SHORTEST (a)-[:R]-{1,2}(b) RETURN a");
        REQUIRE_FALSE(reverse_match_pattern_if_safe(q.matches[0]));
    }
}

TEST_CASE("is_simple_unbounded_right_hop recognises only the plain unbounded RIGHT hop", "[gql_optimizer]") {
    auto is_hop = [](const std::string& query) {
        auto q = GqlParser::parse(query);
        const auto& m = q.matches[0];
        return !m.pattern.edges.empty() && is_simple_unbounded_right_hop(m, m.pattern.edges[0]);
    };
    REQUIRE(is_hop("MATCH (a)-[*]->(b) RETURN a"));            // the algebraic fast-path shape
    REQUIRE_FALSE(is_hop("MATCH (a)-[e*]->(b) RETURN a"));     // an edge binding disqualifies
    REQUIRE_FALSE(is_hop("MATCH (a)-[:R]->(b) RETURN a"));     // a fixed single hop is not variable-length
    REQUIRE_FALSE(is_hop("MATCH (a)<-[*]-(b) RETURN a"));      // a LEFT hop is not right-directed
    REQUIRE_FALSE(is_hop("MATCH (a)-[*2..5]->(b) RETURN a"));  // a bounded hop is not unbounded
}

TEST_CASE("rewrite_count_to_sum_degree rewrites a matching count into sum(start.degree)", "[gql_optimizer]") {
    // start a, end b, edge e: a count over the end or edge variable (or count(*)) is the number of
    // out-edges, so it becomes sum(a.deg). A distinct count or a count over another variable is untouched.
    auto becomes_sum_degree = [](const std::string& e) {
        auto q = GqlParser::parse("MATCH (a)-[e:R]->(b) RETURN " + e + " AS c");
        bool fired = false;
        rewrite_count_to_sum_degree(q.returns[0].expr, "a", "b", "e", "deg", fired);
        auto* ex = q.returns[0].expr.get();
        if (!fired || ex->kind != ExpressionKind::AGGREGATION) return false;
        auto* agg = static_cast<AggregateExpr*>(ex);
        if (agg->fn_kind != AggregateKind::SUM || !agg->expr ||
            agg->expr->kind != ExpressionKind::PROPERTY_LOOKUP) return false;
        auto* pl = static_cast<PropertyLookupExpr*>(agg->expr.get());
        return pl->variable == "a" && pl->property == "deg";
    };
    auto left_as_count = [](const std::string& e) {
        auto q = GqlParser::parse("MATCH (a)-[e:R]->(b) RETURN " + e + " AS c");
        bool fired = false;
        rewrite_count_to_sum_degree(q.returns[0].expr, "a", "b", "e", "deg", fired);
        return !fired && q.returns[0].expr->kind == ExpressionKind::AGGREGATION &&
               static_cast<AggregateExpr*>(q.returns[0].expr.get())->fn_kind == AggregateKind::COUNT;
    };
    SECTION("count over the end variable becomes sum(a.deg)") { REQUIRE(becomes_sum_degree("count(b)")); }
    SECTION("count over the edge variable becomes sum(a.deg)") { REQUIRE(becomes_sum_degree("count(e)")); }
    SECTION("count(*) becomes sum(a.deg)") { REQUIRE(becomes_sum_degree("count(*)")); }
    SECTION("count(DISTINCT ...) is left as a distinct count") { REQUIRE(left_as_count("count(DISTINCT b)")); }
    SECTION("count over a non-target variable is left untouched") { REQUIRE(left_as_count("count(a)")); }
}

TEST_CASE("rewrite_khop_count_to_var replaces a matching count with the bare variable", "[gql_optimizer]") {
    auto becomes_var = [](const std::string& e) {
        auto q = GqlParser::parse("MATCH (a)-[e:R]->(b) RETURN " + e + " AS c");
        rewrite_khop_count_to_var(q.returns[0].expr, "b");
        auto* ex = q.returns[0].expr.get();
        return ex->kind == ExpressionKind::VARIABLE && static_cast<VariableExpr*>(ex)->name == "b";
    };
    auto left_as_count = [](const std::string& e) {
        auto q = GqlParser::parse("MATCH (a)-[e:R]->(b) RETURN " + e + " AS c");
        rewrite_khop_count_to_var(q.returns[0].expr, "b");
        return q.returns[0].expr->kind == ExpressionKind::AGGREGATION;
    };
    SECTION("count over the variable becomes the variable") { REQUIRE(becomes_var("count(b)")); }
    SECTION("count(*) becomes the variable") { REQUIRE(becomes_var("count(*)")); }
    SECTION("count(DISTINCT ...) is left untouched") { REQUIRE(left_as_count("count(DISTINCT b)")); }
    SECTION("count over a different variable is left untouched") { REQUIRE(left_as_count("count(a)")); }
}

TEST_CASE("extract_rel_types collects the literal types from an edge label expression", "[gql_optimizer]") {
    auto contains = [](const std::vector<std::string>& v, const std::string& s) {
        for (const auto& x : v) {
            if (x == s) return true;
        }
        return false;
    };
    SECTION("an OR of labels yields each literal type") {
        auto q = GqlParser::parse("MATCH (a)-[e:R|S|T]->(b) RETURN a");
        std::vector<std::string> types;
        extract_rel_types(q.matches[0].pattern.edges[0].label_expr.get(), types);
        REQUIRE(types.size() == 3);
        REQUIRE(contains(types, "R"));
        REQUIRE(contains(types, "S"));
        REQUIRE(contains(types, "T"));
    }
    SECTION("a single literal label yields exactly that type") {
        auto q = GqlParser::parse("MATCH (a)-[e:R]->(b) RETURN a");
        std::vector<std::string> types;
        extract_rel_types(q.matches[0].pattern.edges[0].label_expr.get(), types);
        REQUIRE(types.size() == 1);
        REQUIRE(types[0] == "R");
    }
}

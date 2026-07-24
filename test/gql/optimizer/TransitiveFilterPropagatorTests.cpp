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

// TransitiveFilterPropagator copies a constant filter (e.g. a.age > 30) onto every variable proven equal to
// its subject, exposing per-node index pushdown. Soundness turns on WHY two variables are equal: a whole-node
// equality (a = b) carries every property, but a single-property equality (a.age = b.age) may ONLY carry that
// one property -- propagating an a.name filter across an a.age = b.age link would be a wrong answer. These pin
// that it propagates on a genuine equality and, crucially, declines to propagate across a different property.

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/optimizer/TransitiveFilterPropagator.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
// Whether the expression tree contains a comparison whose left side is `var.prop`.
int count_prop_comparison(const Expression* e, const std::string& var, const std::string& prop) {
    if (!e) return 0;
    int n = 0;
    if (e->kind == ExpressionKind::BINARY_OP) {
        const auto* b = static_cast<const BinaryOpExpr*>(e);
        if (b->left && b->left->kind == ExpressionKind::PROPERTY_LOOKUP) {
            const auto* pl = static_cast<const PropertyLookupExpr*>(b->left.get());
            if (pl->variable == var && pl->property == prop) n++;
        }
        n += count_prop_comparison(b->left.get(), var, prop);
        n += count_prop_comparison(b->right.get(), var, prop);
    } else if (e->kind == ExpressionKind::UNARY_OP) {
        n += count_prop_comparison(static_cast<const UnaryOpExpr*>(e)->expr.get(), var, prop);
    }
    return n;
}
// Run only the propagation pass and report whether it produced a `var.prop` comparison.
bool propagates_to(const std::string& q, const std::string& var, const std::string& prop) {
    GqlQuery query = GqlParser::parse(q);
    TransitiveFilterPropagator::transitive_filter_propagation_pass(query);
    return count_prop_comparison(query.where_expr.get(), var, prop) > 0;
}
}  // namespace

TEST_CASE("transitive filter propagation copies a filter across a genuine equality", "[gql_optimizer]") {
    SECTION("a whole-node equality carries the filter to the equated variable") {
        REQUIRE(propagates_to("MATCH (a:P), (b:P) WHERE a = b AND a.age > 30 RETURN a", "b", "age"));
    }
    SECTION("a same-property equality carries that property's filter") {
        REQUIRE(propagates_to("MATCH (a:P), (b:P) WHERE a.age = b.age AND a.age > 30 RETURN a", "b", "age"));
    }
}

TEST_CASE("transitive filter propagation declines to propagate unsoundly", "[gql_optimizer]") {
    SECTION("a different-property equality does NOT carry an unrelated property's filter") {
        // a.x = b.x says nothing about a.age vs b.age, so an a.age filter must not become a b.age filter.
        REQUIRE_FALSE(propagates_to("MATCH (a:P), (b:P) WHERE a.x = b.x AND a.age > 30 RETURN a", "b", "age"));
    }
    SECTION("with no equality at all there is nothing to propagate") {
        REQUIRE_FALSE(propagates_to("MATCH (a:P), (b:P) WHERE a.age > 30 RETURN a", "b", "age"));
    }
}

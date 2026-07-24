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

// AntiSemiJoinPromoter turns the `OPTIONAL MATCH (a)-[:R]->(b) ... WHERE b IS NULL` idiom -- keep only
// rows where the optional pattern did NOT match -- into a `NOT EXISTS { (a)-[:R]->(b) }` anti-join,
// dropping the now-redundant optional match. It is a row-dropping rewrite, so its trigger must be exact:
// firing on the wrong shape silently changes the result set. These pin the fire conditions AND every
// non-fire boundary. The pass is exercised in isolation because in the full pipeline its output interacts
// with later passes; here the concern is the promotion decision itself.

#include <catch2/catch.hpp>
#include "../../../src/gql/GqlParser.h"
#include "../../../src/gql/optimizer/AntiSemiJoinPromoter.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
// Parse, run only the anti-semi-join promotion pass, and hand back the rewritten query.
GqlQuery promoted(const std::string& q) {
    GqlQuery query = GqlParser::parse(q);
    AntiSemiJoinPromoter::optional_match_to_antisemijoin_pass(query);
    return query;
}
}  // namespace

TEST_CASE("anti-semi-join promotion fires only on a null-checked optional variable", "[gql_optimizer]") {
    SECTION("an optional variable IS NULL is promoted to NOT EXISTS and the optional match is dropped") {
        auto q = promoted("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b IS NULL RETURN a");
        REQUIRE(q.matches.size() == 1);                      // the optional match is erased
        REQUIRE(q.where_expr);
        REQUIRE(q.where_expr->kind == ExpressionKind::UNARY_OP);
        auto* un = static_cast<UnaryOpExpr*>(q.where_expr.get());
        REQUIRE(un->op == UnaryOpKind::NOT);                 // NOT EXISTS
        REQUIRE(un->expr->kind == ExpressionKind::EXISTS);
    }

    SECTION("a conjoined predicate survives the promotion") {
        // `b IS NULL AND a.age > 30`: the IS NULL is lifted into the anti-join, the other conjunct stays.
        auto q = promoted("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b IS NULL AND a.age > 30 RETURN a");
        REQUIRE(q.matches.size() == 1);
        REQUIRE(q.where_expr);
        REQUIRE(q.where_expr->kind == ExpressionKind::BINARY_OP);   // (a.age > 30) AND (NOT EXISTS ...)
    }

    SECTION("a variable newly bound by a second optional hop still promotes past a prior required match") {
        auto q = promoted("MATCH (a:Person)-[e:KNOWS]->(x) OPTIONAL MATCH (a)-[:LIKES]->(c) WHERE c IS NULL RETURN a");
        REQUIRE(q.matches.size() == 1);                      // only the optional match is erased
        REQUIRE(q.where_expr->kind == ExpressionKind::UNARY_OP);
    }
}

TEST_CASE("anti-semi-join promotion declines the unsound shapes", "[gql_optimizer]") {
    // In every case the pass must leave both matches in place (no promotion) so the result is unchanged.
    auto declines = [](const std::string& q) {
        auto res = promoted(q);
        REQUIRE(res.matches.size() == 2);
    };

    SECTION("IS NOT NULL is a semi-join, not an anti-join") {
        declines("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b IS NOT NULL RETURN a");
    }
    SECTION("a null check on a property, not the variable, is a real value test") {
        declines("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b.name IS NULL RETURN a");
    }
    SECTION("a disjunctive null check cannot be lifted (the other arm may still admit the row)") {
        declines("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b IS NULL OR a.name = 'x' RETURN a");
    }
    SECTION("IS NULL on the optional match's pre-bound anchor is a contradiction, not an anti-join") {
        // `a` is bound by the required first match, so `a IS NULL` is always false (an always-empty
        // result). Promoting it would drop the IS NULL and wrongly return the pattern's anti-join instead.
        declines("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE a IS NULL RETURN a");
    }
    SECTION("IS NULL on a variable a prior required match bound is likewise a contradiction") {
        declines("MATCH (a:Person)-[:KNOWS]->(b:Person) OPTIONAL MATCH (b)-[:LIKES]->(c) WHERE b IS NULL RETURN a");
    }
}

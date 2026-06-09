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

#include <catch2/catch.hpp>
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Optimizer pushdown test", "[gql_optimizer]") {
    std::string query_str = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name";
    auto query = GqlParser::parse(query_str);

    // Prior to optimization, the node's properties map is empty
    REQUIRE(query.matches[0].pattern.nodes[0].properties.empty());
    REQUIRE(query.where_expr != nullptr);

    GqlOptimizer::optimize(query);

    // After optimization, 'name = Alice' should be pushed down into the node patterns
    auto& properties = query.matches[0].pattern.nodes[0].properties;
    REQUIRE(properties.count("name") == 1);
    REQUIRE(std::get<std::string>(properties.at("name")) == "Alice");

    // The where expression should be simplified and rebuild to nullptr since the only predicate was pushed down
    REQUIRE(query.where_expr == nullptr);
}

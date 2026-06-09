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

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Parser builds AST", "[gql_parser]") {
    std::string query = "MATCH (p:Person) RETURN p.name";
    auto q = GqlParser::parse(query);

    REQUIRE(q.matches.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes.size() == 1);
    REQUIRE(q.matches[0].pattern.nodes[0].variable == "p");
    REQUIRE(q.matches[0].pattern.nodes[0].label == "Person");
    REQUIRE(q.returns.size() == 1);
}

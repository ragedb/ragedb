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
#include "../../src/gql/GqlLexer.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("GQL Lexer tokenizes basic query", "[gql_lexer]") {
    std::string query = "MATCH (p:Person) WHERE p.age >= 21 RETURN p.name";
    auto tokens = GqlLexer::tokenize(query);

    REQUIRE(tokens.size() > 0);
    REQUIRE(tokens[0].type == TokenType::MATCH);
    REQUIRE(tokens[1].type == TokenType::LPAREN);
    REQUIRE(tokens[2].type == TokenType::NAME);
    REQUIRE(tokens[2].text == "p");
    REQUIRE(tokens[3].type == TokenType::COLON);
    REQUIRE(tokens[4].type == TokenType::NAME);
    REQUIRE(tokens[4].text == "Person");
    REQUIRE(tokens[5].type == TokenType::RPAREN);
    REQUIRE(tokens[6].type == TokenType::WHERE);
}

TEST_CASE("GQL Lexer handles // single-line comments", "[gql_lexer]") {
    std::string query = "MATCH (p:Person)\n// this is a comment\nRETURN p.name";
    auto tokens = GqlLexer::tokenize(query);

    REQUIRE(tokens.size() > 0);
    REQUIRE(tokens[0].type == TokenType::MATCH);
    REQUIRE(tokens[1].type == TokenType::LPAREN);
    REQUIRE(tokens[2].type == TokenType::NAME);
    REQUIRE(tokens[3].type == TokenType::COLON);
    REQUIRE(tokens[4].type == TokenType::NAME);
    REQUIRE(tokens[5].type == TokenType::RPAREN);
    REQUIRE(tokens[6].type == TokenType::RETURN);
}

TEST_CASE("GQL Lexer handles backtick-escaped identifiers", "[gql_lexer]") {
    std::string query = "MATCH (`Person Type` IS `My Label`)";
    auto tokens = GqlLexer::tokenize(query);

    REQUIRE(tokens.size() >= 7);
    REQUIRE(tokens[0].type == TokenType::MATCH);
    REQUIRE(tokens[1].type == TokenType::LPAREN);
    REQUIRE(tokens[2].type == TokenType::NAME);
    REQUIRE(tokens[2].text == "Person Type");
    REQUIRE(tokens[3].type == TokenType::IS);
    REQUIRE(tokens[4].type == TokenType::NAME);
    REQUIRE(tokens[4].text == "My Label");
    REQUIRE(tokens[5].type == TokenType::RPAREN);
}

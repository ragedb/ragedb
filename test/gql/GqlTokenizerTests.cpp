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

TEST_CASE("GQL Lexer handles comments and whitespace variants", "[gql_lexer]") {
    SECTION("Multi-line comment parsing") {
        std::string query = "MATCH (p) /* multi-line \n comment here */ RETURN p";
        auto tokens = GqlLexer::tokenize(query);
        REQUIRE(tokens.size() >= 5);
        REQUIRE(tokens[0].type == TokenType::MATCH);
        REQUIRE(tokens[1].type == TokenType::LPAREN);
        REQUIRE(tokens[2].type == TokenType::NAME);
        REQUIRE(tokens[3].type == TokenType::RPAREN);
        REQUIRE(tokens[4].type == TokenType::RETURN);
    }

    SECTION("Dashed single-line comments") {
        std::string query = "MATCH (p) -- standard single line comment\nRETURN p";
        auto tokens = GqlLexer::tokenize(query);
        REQUIRE(tokens.size() >= 5);
        REQUIRE(tokens[0].type == TokenType::MATCH);
        REQUIRE(tokens[4].type == TokenType::RETURN);
    }
}

TEST_CASE("GQL Lexer tokenizes literals and numbers", "[gql_lexer]") {
    SECTION("Floating point numbers") {
        std::string query = "RETURN 12.34, 0.5";
        auto tokens = GqlLexer::tokenize(query);
        REQUIRE(tokens.size() >= 4);
        REQUIRE(tokens[0].type == TokenType::RETURN);
        REQUIRE(tokens[1].type == TokenType::FLOAT_LIT);
        REQUIRE(tokens[1].float_value == Approx(12.34));
        REQUIRE(tokens[2].type == TokenType::COMMA);
        REQUIRE(tokens[3].type == TokenType::FLOAT_LIT);
        REQUIRE(tokens[3].float_value == Approx(0.5));
    }

    SECTION("Escaped string characters") {
        std::string query = "RETURN 'Alice\\'s book', \"Bob\\\\Charlie\"";
        auto tokens = GqlLexer::tokenize(query);
        REQUIRE(tokens.size() >= 4);
        REQUIRE(tokens[1].type == TokenType::STRING_LIT);
        REQUIRE(tokens[1].text == "Alice's book");
        REQUIRE(tokens[3].type == TokenType::STRING_LIT);
        REQUIRE(tokens[3].text == "Bob\\Charlie");
    }
}

TEST_CASE("GQL Lexer tokenizes all operators and compound keywords", "[gql_lexer]") {
    SECTION("Operator and inequality symbols") {
        std::string query = "+ - * / = != <> < > <= >= ! | &";
        auto tokens = GqlLexer::tokenize(query);
        
        // Match 14 operators + 1 EOF token
        REQUIRE(tokens.size() == 15);
        REQUIRE(tokens[0].type == TokenType::PLUS);
        REQUIRE(tokens[1].type == TokenType::MINUS);
        REQUIRE(tokens[2].type == TokenType::STAR);
        REQUIRE(tokens[3].type == TokenType::SLASH);
        REQUIRE(tokens[4].type == TokenType::EQ);
        REQUIRE(tokens[5].type == TokenType::NE); // !=
        REQUIRE(tokens[6].type == TokenType::NE); // <>
        REQUIRE(tokens[7].type == TokenType::LT);
        REQUIRE(tokens[8].type == TokenType::GT);
        REQUIRE(tokens[9].type == TokenType::LE);
        REQUIRE(tokens[10].type == TokenType::GE);
        REQUIRE(tokens[11].type == TokenType::BANG);
        REQUIRE(tokens[12].type == TokenType::PIPE);
        REQUIRE(tokens[13].type == TokenType::AMP);
    }

    SECTION("ORDER BY with whitespace variants") {
        std::string query = "RETURN p ORDER    \n   BY p.age";
        auto tokens = GqlLexer::tokenize(query);
        REQUIRE(tokens.size() >= 3);
        REQUIRE(tokens[2].type == TokenType::ORDER_BY);
    }

    SECTION("String operators and concat symbols") {
        std::string query = "STARTS WITH ENDS WITH CONTAINS ||";
        auto tokens = GqlLexer::tokenize(query);
        REQUIRE(tokens.size() == 5);
        REQUIRE(tokens[0].type == TokenType::STARTS_WITH);
        REQUIRE(tokens[1].type == TokenType::ENDS_WITH);
        REQUIRE(tokens[2].type == TokenType::CONTAINS);
        REQUIRE(tokens[3].type == TokenType::PIPE_PIPE);
    }
}

TEST_CASE("GQL Lexer throws exceptions on invalid input", "[gql_lexer]") {
    SECTION("Unrecognized character") {
        REQUIRE_THROWS_AS(GqlLexer::tokenize("MATCH @p"), std::runtime_error);
    }

    SECTION("Unterminated string") {
        REQUIRE_THROWS_AS(GqlLexer::tokenize("RETURN 'Alice"), std::runtime_error);
    }

    SECTION("Unterminated backtick identifier") {
        REQUIRE_THROWS_AS(GqlLexer::tokenize("MATCH (`Unterminated"), std::runtime_error);
    }
}


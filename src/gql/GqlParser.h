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

#ifndef RAGEDB_GQLPARSER_H
#define RAGEDB_GQLPARSER_H

#include "GqlLexer.h"
#include "GqlAst.h"
#include <vector>
#include <string>
#include <memory>

namespace ragedb::gql {

class GqlParser {
private:
    std::vector<Token> tokens;
    size_t pos = 0;

    const Token& peek(size_t offset = 0) const;
    const Token& advance();
    bool check(TokenType type) const;
    bool match(TokenType type);
    void consume(TokenType type, const std::string& error_message);

    // Parsing routines
    GqlQuery parse_query();
    GqlQuery parse_union();
    GqlQuery parse_intersect();
    GqlQuery parse_single_query();
    MatchStatement parse_match();
    PathPattern parse_path_pattern();
    PatternNode parse_node_pattern();
    std::map<std::string, property_type_t> parse_properties();

    // Expression parsing (Precedence climbing)
    std::unique_ptr<Expression> parse_expression();
    std::unique_ptr<Expression> parse_or();
    std::unique_ptr<Expression> parse_and();
    std::unique_ptr<Expression> parse_comparison();
    std::unique_ptr<Expression> parse_add_sub();
    std::unique_ptr<Expression> parse_mul_div();
    std::unique_ptr<Expression> parse_unary();
    std::unique_ptr<Expression> parse_primary();

public:
    explicit GqlParser(std::vector<Token> tokens) : tokens(std::move(tokens)) {}
    static GqlQuery parse(const std::string& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLPARSER_H

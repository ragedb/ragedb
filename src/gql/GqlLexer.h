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

#ifndef RAGEDB_GQLLEXER_H
#define RAGEDB_GQLLEXER_H

#include <string>
#include <vector>

namespace ragedb::gql {

enum class TokenType {
    // Literals
    NAME,
    NUMBER,
    FLOAT_LIT,
    STRING_LIT,

    // Keywords
    TRUE_KW,
    FALSE_KW,
    NULL_KW,
    WHERE,
    AND,
    OR,
    NOT,
    AS,
    IS,
    MATCH,
    OPTIONAL,
    RETURN,
    WITH,
    NEXT_KW,    // ISO GQL linear-query segment boundary
    LET_KW,     // ISO GQL value binding (LET x = expr)
    FILTER_KW,  // ISO GQL post-projection predicate (FILTER pred)
    CASE_KW,    // CASE WHEN ... THEN ... ELSE ... END
    WHEN_KW,
    THEN_KW,
    ELSE_KW,
    END_KW,
    DISTINCT,
    ORDER_BY,
    LIMIT,
    OFFSET,
    CAST,
    LABELED,
    ASC,
    DESC,
    INSERT,
    DELETE,
    SET,
    REMOVE,
    DETACH,
    UNION,
    INTERSECT,
    EXCEPT,
    ALL_KW,
    SHORTEST_KW,
    ANY_KW,
    GROUP_KW,
    CHEAPEST_KW,
    COST_KW,
    DIFFERENT_KW,
    REPEATABLE_KW,
    TRAIL_KW,
    ACYCLIC_KW,
    SIMPLE_KW,
    WALK_KW,
    EXISTS,
    EXPLAIN,
    PROFILE,
    NO_SEMANTIC,
    STARTS_WITH,
    ENDS_WITH,
    CONTAINS,

    // DDL Keywords
    CREATE,
    DROP,
    ALTER,
    ADD,
    TYPE,
    NODE,
    RELATIONSHIP,
    STRING_KW,
    INTEGER_KW,
    DOUBLE_KW,
    BOOLEAN_KW,
    STRING_LIST_KW,
    INTEGER_LIST_KW,
    DOUBLE_LIST_KW,
    BOOLEAN_LIST_KW,
    INDEX,
    INDEXES,
    ON,
    SHOW,

    // FTS Keywords
    FULLTEXT,
    SEARCH,
    FOR,
    OPTIONS,
    YIELD,
    IN_KW,
    KHOP,

    // Admin Keywords
    CALL,
    CLEAR,
    CACHE,

    // Symbols
    LPAREN,      // (
    RPAREN,      // )
    LBRACKET,    // [
    RBRACKET,    // ]
    LBRACE,      // {
    RBRACE,      // }
    COLON,       // :
    COMMA,       // ,
    DOT,         // .
    STAR,        // *
    SLASH,       // /
    PLUS,        // +
    MINUS,       // -
    BANG,        // !
    EQ,          // =
    NE,          // != or <>
    LT,          // <
    GT,          // >
    LE,          // <=
    GE,          // >=
    PIPE,        // |
    PIPE_PIPE,   // ||
    AMP,         // &
    QUESTION,    // ?
    PERCENT,     // %

    // Edges
    RIGHT_ARROW, // ->
    LEFT_ARROW,  // <-
    DASH_LB,     // -[
    RB_DASH_GT,  // ]->
    RB_DASH,     // ]-
    LT_DASH_LB,  // <-[

    EOF_TOK
};

struct Token {
    TokenType type;
    std::string text;
    double float_value = 0.0;
    int64_t int_value = 0;
};

class GqlLexer {
public:
    static std::vector<Token> tokenize(const std::string& input);
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLLEXER_H

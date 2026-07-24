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

#include "GqlLexer.h"
#include <cctype>
#include <algorithm>
#include <stdexcept>

namespace ragedb::gql {

/**
 * @brief Helper utility to convert a string to uppercase.
 * Used for case-insensitive keyword comparisons.
 */
static std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::toupper(c); });
    return s;
}

/**
 * @brief Main lexical analysis entry point.
 * Scans the GQL query string and produces a list of structured tokens.
 * 
 * @param input The raw GQL query string.
 * @return std::vector<Token> List of parsed tokens, ending with an EOF token.
 */
std::vector<Token> GqlLexer::tokenize(const std::string& input) {
    std::vector<Token> tokens;
    size_t pos = 0;
    size_t length = input.length();

    // Look ahead at characters in the input stream without advancing the cursor.
    auto peek = [&](size_t offset = 0) -> char {
        if (pos + offset >= length) return '\0';
        return input[pos + offset];
    };

    // Move the cursor forward by a given number of characters.
    auto advance = [&](size_t count = 1) {
        pos += count;
    };

    // Skips standard whitespace as well as single-line and multi-line comments.
    auto skip_whitespace_and_comments = [&]() {
        while (pos < length) {
            char c = peek();
            if (std::isspace(c)) {
                advance();
            } else if (c == '/' && peek(1) == '*') {
                // Multi-line comment: /* comment */
                size_t start_pos = pos;
                advance(2);
                while (pos < length && !(peek() == '*' && peek(1) == '/')) {
                    advance();
                }
                size_t end_pos = pos;
                if (pos < length) {
                    advance(2);
                }
                std::string comment_content = input.substr(start_pos + 2, end_pos - start_pos - 2);
                // trim
                comment_content.erase(0, comment_content.find_first_not_of(" \t\n\r"));
                size_t last = comment_content.find_last_not_of(" \t\n\r");
                if (last != std::string::npos) {
                    comment_content = comment_content.substr(0, last + 1);
                } else {
                    comment_content = "";
                }
                if (to_upper(comment_content) == "NO_SEMANTIC") {
                    tokens.push_back({TokenType::NO_SEMANTIC, "NO_SEMANTIC"});
                }
            } else if (c == '/' && peek(1) == '/') {
                // Single-line comment style 1: // comment
                advance(2);
                while (pos < length && peek() != '\n') {
                    advance();
                }
            } else if (c == '-' && peek(1) == '-' && peek(2) != '>') {
                // Single-line comment style 2: -- comment (ignoring relationship arrow '-->')
                advance(2);
                while (pos < length && peek() != '\n') {
                    advance();
                }
            } else {
                break;
            }
        }
    };

    // Main tokenizer loop
    while (pos < length) {
        skip_whitespace_and_comments();
        if (pos >= length) break;

        char c = peek();

        // 1. Check compound relationship tokens first (greedily match arrows)
        // Left incoming relationship: <-[
        if (c == '<' && peek(1) == '-' && peek(2) == '[') {
            tokens.push_back({TokenType::LT_DASH_LB, "<-["});
            advance(3);
            continue;
        }
        // Left incoming arrow: <-
        if (c == '<' && peek(1) == '-') {
            tokens.push_back({TokenType::LEFT_ARROW, "<-"});
            advance(2);
            continue;
        }
        // Right outgoing arrow: ->
        if (c == '-' && peek(1) == '>') {
            tokens.push_back({TokenType::RIGHT_ARROW, "->"});
            advance(2);
            continue;
        }
        // Undirected details or outgoing details start: -[
        if (c == '-' && peek(1) == '[') {
            tokens.push_back({TokenType::DASH_LB, "-["});
            advance(2);
            continue;
        }
        // Relationship details end + right arrow: ]->
        if (c == ']' && peek(1) == '-' && peek(2) == '>') {
            tokens.push_back({TokenType::RB_DASH_GT, "]->"});
            advance(3);
            continue;
        }
        // Relationship details end: ]-
        if (c == ']' && peek(1) == '-') {
            tokens.push_back({TokenType::RB_DASH, "]-"});
            advance(2);
            continue;
        }

        // 2. Single-character symbols and operators
        if (c == '(') { tokens.push_back({TokenType::LPAREN, "("}); advance(); continue; }
        if (c == ')') { tokens.push_back({TokenType::RPAREN, ")"}); advance(); continue; }
        if (c == '[') { tokens.push_back({TokenType::LBRACKET, "["}); advance(); continue; }
        if (c == ']') { tokens.push_back({TokenType::RBRACKET, "]"}); advance(); continue; }
        if (c == '{') { tokens.push_back({TokenType::LBRACE, "{"}); advance(); continue; }
        if (c == '}') { tokens.push_back({TokenType::RBRACE, "}"}); advance(); continue; }
        if (c == ':') { tokens.push_back({TokenType::COLON, ":"}); advance(); continue; }
        if (c == ',') { tokens.push_back({TokenType::COMMA, ","}); advance(); continue; }
        if (c == '.') { tokens.push_back({TokenType::DOT, "."}); advance(); continue; }
        if (c == '*') { tokens.push_back({TokenType::STAR, "*"}); advance(); continue; }
        if (c == '/') { tokens.push_back({TokenType::SLASH, "/"}); advance(); continue; }
        if (c == '+') { tokens.push_back({TokenType::PLUS, "+"}); advance(); continue; }
        if (c == '-') { tokens.push_back({TokenType::MINUS, "-"}); advance(); continue; }
        if (c == '|') {
            if (peek(1) == '|') {
                tokens.push_back({TokenType::PIPE_PIPE, "||"});
                advance(2);
            } else {
                tokens.push_back({TokenType::PIPE, "|"});
                advance();
            }
            continue;
        }
        if (c == '&') { tokens.push_back({TokenType::AMP, "&"}); advance(); continue; }
        if (c == '?') { tokens.push_back({TokenType::QUESTION, "?"}); advance(); continue; }
        if (c == '%') { tokens.push_back({TokenType::PERCENT, "%"}); advance(); continue; }

        // Inequality and logical operators
        if (c == '!') {
            if (peek(1) == '=') {
                tokens.push_back({TokenType::NE, "!="});
                advance(2);
            } else {
                tokens.push_back({TokenType::BANG, "!"});
                advance();
            }
            continue;
        }
        if (c == '=') {
            tokens.push_back({TokenType::EQ, "="});
            advance();
            continue;
        }
        if (c == '<') {
            if (peek(1) == '>') {
                tokens.push_back({TokenType::NE, "<>"});
                advance(2);
            } else if (peek(1) == '=') {
                tokens.push_back({TokenType::LE, "<="});
                advance(2);
            } else {
                tokens.push_back({TokenType::LT, "<"});
                advance();
            }
            continue;
        }
        if (c == '>') {
            if (peek(1) == '=') {
                tokens.push_back({TokenType::GE, ">="});
                advance(2);
            } else {
                tokens.push_back({TokenType::GT, ">"});
                advance();
            }
            continue;
        }

        // 3. Backticks: Backtick-escaped identifiers (e.g. `My Type Name`)
        if (c == '`') {
            advance();
            std::string name;
            while (pos < length && peek() != '`') {
                if (peek() == '\\' && pos + 1 < length) {
                    name += peek(1);
                    advance(2);
                } else {
                    name += peek();
                    advance();
                }
            }
            if (pos >= length) {
                throw std::runtime_error("Unterminated backtick identifier");
            }
            advance(); // Consume the terminating backtick
            tokens.push_back({TokenType::NAME, name});
            continue;
        }

        // 4. String Literals (e.g., 'Alice' or "Bob")
        if (c == '\'' || c == '"') {
            char quote_char = c;
            advance();
            std::string str;
            while (pos < length && peek() != quote_char) {
                if (peek() == '\\' && pos + 1 < length) {
                    str += peek(1);
                    advance(2);
                } else {
                    str += peek();
                    advance();
                }
            }
            if (pos >= length) {
                throw std::runtime_error("Unterminated string literal");
            }
            advance(); // Consume the closing quote
            tokens.push_back({TokenType::STRING_LIT, str});
            continue;
        }

        // 5. Numeric Literals (Integers and Floating Points)
        if (std::isdigit(c)) {
            std::string num_str;
            while (pos < length && std::isdigit(peek())) {
                num_str += peek();
                advance();
            }
            // If we hit a dot followed by digits, it's a floating point number
            if (peek() == '.' && std::isdigit(peek(1))) {
                num_str += '.';
                advance();
                while (pos < length && std::isdigit(peek())) {
                    num_str += peek();
                    advance();
                }
                tokens.push_back({TokenType::FLOAT_LIT, num_str, std::stod(num_str), 0});
            } else {
                tokens.push_back({TokenType::NUMBER, num_str, 0.0, std::stoll(num_str)});
            }
            continue;
        }

        // 6. Alphabetic Identifiers and Keywords
        if (std::isalpha(c) || c == '_') {
            std::string name;
            while (pos < length && (std::isalnum(peek()) || peek() == '_')) {
                name += peek();
                advance();
            }

            // Convert to uppercase to match keywords case-insensitively
            std::string upper_name = to_upper(name);
            TokenType type = TokenType::NAME;

            if (upper_name == "MATCH") type = TokenType::MATCH;
            else if (upper_name == "KHOP") type = TokenType::KHOP;
            else if (upper_name == "OPTIONAL") type = TokenType::OPTIONAL;
            else if (upper_name == "WHERE") type = TokenType::WHERE;
            else if (upper_name == "RETURN") type = TokenType::RETURN;
            else if (upper_name == "WITH") type = TokenType::WITH;
            else if (upper_name == "DISTINCT") type = TokenType::DISTINCT;
            else if (upper_name == "LIMIT") type = TokenType::LIMIT;
            else if (upper_name == "OFFSET" || upper_name == "SKIP") type = TokenType::OFFSET;
            else if (upper_name == "ASC" || upper_name == "ASCENDING") type = TokenType::ASC;
            else if (upper_name == "DESC" || upper_name == "DESCENDING") type = TokenType::DESC;
            else if (upper_name == "TRUE") type = TokenType::TRUE_KW;
            else if (upper_name == "FALSE") type = TokenType::FALSE_KW;
            else if (upper_name == "NULL") type = TokenType::NULL_KW;
            else if (upper_name == "AND") type = TokenType::AND;
            else if (upper_name == "OR") type = TokenType::OR;
            else if (upper_name == "NOT") type = TokenType::NOT;
            else if (upper_name == "AS") type = TokenType::AS;
            else if (upper_name == "IS") type = TokenType::IS;
            else if (upper_name == "CAST") type = TokenType::CAST;
            else if (upper_name == "LABELED") type = TokenType::LABELED;
            // ISO GQL linear-query keywords
            else if (upper_name == "NEXT") type = TokenType::NEXT_KW;
            else if (upper_name == "LET") type = TokenType::LET_KW;
            else if (upper_name == "FILTER") type = TokenType::FILTER_KW;
            // CASE expression keywords
            else if (upper_name == "CASE") type = TokenType::CASE_KW;
            else if (upper_name == "WHEN") type = TokenType::WHEN_KW;
            else if (upper_name == "THEN") type = TokenType::THEN_KW;
            else if (upper_name == "ELSE") type = TokenType::ELSE_KW;
            else if (upper_name == "END") type = TokenType::END_KW;
            // GQL Write/Modification keywords
            else if (upper_name == "INSERT") type = TokenType::INSERT;
            else if (upper_name == "DELETE") type = TokenType::DELETE;
            else if (upper_name == "SET") type = TokenType::SET;
            else if (upper_name == "REMOVE") type = TokenType::REMOVE;
            else if (upper_name == "DETACH") type = TokenType::DETACH;
            else if (upper_name == "UNION") type = TokenType::UNION;
            else if (upper_name == "INTERSECT") type = TokenType::INTERSECT;
            else if (upper_name == "EXCEPT") type = TokenType::EXCEPT;
            else if (upper_name == "ALL") type = TokenType::ALL_KW;
            else if (upper_name == "SHORTEST") type = TokenType::SHORTEST_KW;
            else if (upper_name == "ANY") type = TokenType::ANY_KW;
            else if (upper_name == "GROUP") type = TokenType::GROUP_KW;
            else if (upper_name == "CHEAPEST") type = TokenType::CHEAPEST_KW;
            else if (upper_name == "COST") type = TokenType::COST_KW;
            else if (upper_name == "DIFFERENT") type = TokenType::DIFFERENT_KW;
            else if (upper_name == "REPEATABLE") type = TokenType::REPEATABLE_KW;
            else if (upper_name == "TRAIL") type = TokenType::TRAIL_KW;
            else if (upper_name == "ACYCLIC") type = TokenType::ACYCLIC_KW;
            else if (upper_name == "SIMPLE") type = TokenType::SIMPLE_KW;
            else if (upper_name == "WALK") type = TokenType::WALK_KW;
            else if (upper_name == "EXISTS") type = TokenType::EXISTS;
            else if (upper_name == "EXPLAIN") type = TokenType::EXPLAIN;
            else if (upper_name == "PROFILE") type = TokenType::PROFILE;
            else if (upper_name == "NO_SEMANTIC") type = TokenType::NO_SEMANTIC;
            else if (upper_name == "CREATE") type = TokenType::CREATE;
            else if (upper_name == "DROP") type = TokenType::DROP;
            else if (upper_name == "ALTER") type = TokenType::ALTER;
            else if (upper_name == "ADD") type = TokenType::ADD;
            else if (upper_name == "TYPE") type = TokenType::TYPE;
            else if (upper_name == "NODE") type = TokenType::NODE;
            else if (upper_name == "RELATIONSHIP" || upper_name == "REL") type = TokenType::RELATIONSHIP;
            else if (upper_name == "STRING") type = TokenType::STRING_KW;
            else if (upper_name == "INTEGER" || upper_name == "INT") type = TokenType::INTEGER_KW;
            else if (upper_name == "DOUBLE") type = TokenType::DOUBLE_KW;
            else if (upper_name == "BOOLEAN" || upper_name == "BOOL") type = TokenType::BOOLEAN_KW;
            else if (upper_name == "STRING_LIST") type = TokenType::STRING_LIST_KW;
            else if (upper_name == "INTEGER_LIST" || upper_name == "INT_LIST") type = TokenType::INTEGER_LIST_KW;
            else if (upper_name == "DOUBLE_LIST") type = TokenType::DOUBLE_LIST_KW;
            else if (upper_name == "BOOLEAN_LIST" || upper_name == "BOOL_LIST") type = TokenType::BOOLEAN_LIST_KW;
            else if (upper_name == "CALL") type = TokenType::CALL;
            else if (upper_name == "CLEAR") type = TokenType::CLEAR;
            else if (upper_name == "CACHE") type = TokenType::CACHE;
            else if (upper_name == "INDEX") type = TokenType::INDEX;
            else if (upper_name == "INDEXES") type = TokenType::INDEXES;
            else if (upper_name == "ON") type = TokenType::ON;
            else if (upper_name == "SHOW") type = TokenType::SHOW;
            else if (upper_name == "FULLTEXT") type = TokenType::FULLTEXT;
            else if (upper_name == "SEARCH") type = TokenType::SEARCH;
            else if (upper_name == "FOR") type = TokenType::FOR;
            else if (upper_name == "OPTIONS") type = TokenType::OPTIONS;
            else if (upper_name == "YIELD") type = TokenType::YIELD;
            else if (upper_name == "IN") type = TokenType::IN_KW;
            else if (upper_name == "CONTAINS") type = TokenType::CONTAINS;
            else if (upper_name == "STARTS") {
                size_t temp_pos = pos;
                while (temp_pos < length && std::isspace(input[temp_pos])) {
                    temp_pos++;
                }
                if (temp_pos + 4 <= length && to_upper(input.substr(temp_pos, 4)) == "WITH") {
                    pos = temp_pos + 4;
                    type = TokenType::STARTS_WITH;
                    name = "STARTS WITH";
                }
            }
            else if (upper_name == "ENDS") {
                size_t temp_pos = pos;
                while (temp_pos < length && std::isspace(input[temp_pos])) {
                    temp_pos++;
                }
                if (temp_pos + 4 <= length && to_upper(input.substr(temp_pos, 4)) == "WITH") {
                    pos = temp_pos + 4;
                    type = TokenType::ENDS_WITH;
                    name = "ENDS WITH";
                }
            }
            // Compound keyword check: ORDER BY
            else if (upper_name == "ORDER") {
                size_t temp_pos = pos;
                while (temp_pos < length && std::isspace(input[temp_pos])) {
                    temp_pos++;
                }
                if (temp_pos + 2 <= length && to_upper(input.substr(temp_pos, 2)) == "BY") {
                    pos = temp_pos + 2;
                    type = TokenType::ORDER_BY;
                    name = "ORDER BY";
                }
            }

            Token tok{type, name};
            // Set simple numeric representation for booleans
            if (type == TokenType::TRUE_KW) {
                tok.int_value = 1;
            } else if (type == TokenType::FALSE_KW) {
                tok.int_value = 0;
            }
            tokens.push_back(tok);
            continue;
        }

        // Throw error on any unexpected character that couldn't be parsed
        throw std::runtime_error(std::string("Unexpected character '") + c + "' in GQL query");
    }

    // Always append an EOF token to mark the end of the token stream
    tokens.push_back({TokenType::EOF_TOK, ""});
    return tokens;
}

} // namespace ragedb::gql

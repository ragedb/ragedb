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

static std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::toupper(c); });
    return s;
}

std::vector<Token> GqlLexer::tokenize(const std::string& input) {
    std::vector<Token> tokens;
    size_t pos = 0;
    size_t length = input.length();

    auto peek = [&](size_t offset = 0) -> char {
        if (pos + offset >= length) return '\0';
        return input[pos + offset];
    };

    auto advance = [&](size_t count = 1) {
        pos += count;
    };

    auto skip_whitespace_and_comments = [&]() {
        while (pos < length) {
            char c = peek();
            if (std::isspace(c)) {
                advance();
            } else if (c == '/' && peek(1) == '*') {
                advance(2);
                while (pos < length && !(peek() == '*' && peek(1) == '/')) {
                    advance();
                }
                if (pos < length) {
                    advance(2);
                }
            } else if (c == '/' && peek(1) == '/') {
                advance(2);
                while (pos < length && peek() != '\n') {
                    advance();
                }
            } else if (c == '-' && peek(1) == '-' && peek(2) != '>') {
                advance(2);
                while (pos < length && peek() != '\n') {
                    advance();
                }
            } else {
                break;
            }
        }
    };

    while (pos < length) {
        skip_whitespace_and_comments();
        if (pos >= length) break;

        char c = peek();

        // Check compound relationship tokens first
        // <-[
        if (c == '<' && peek(1) == '-' && peek(2) == '[') {
            tokens.push_back({TokenType::LT_DASH_LB, "<-["});
            advance(3);
            continue;
        }
        // <-
        if (c == '<' && peek(1) == '-') {
            tokens.push_back({TokenType::LEFT_ARROW, "<-"});
            advance(2);
            continue;
        }
        // ->
        if (c == '-' && peek(1) == '>') {
            tokens.push_back({TokenType::RIGHT_ARROW, "->"});
            advance(2);
            continue;
        }
        // -[
        if (c == '-' && peek(1) == '[') {
            tokens.push_back({TokenType::DASH_LB, "-["});
            advance(2);
            continue;
        }
        // ]->
        if (c == ']' && peek(1) == '-' && peek(2) == '>') {
            tokens.push_back({TokenType::RB_DASH_GT, "]->"});
            advance(3);
            continue;
        }
        // ]-
        if (c == ']' && peek(1) == '-') {
            tokens.push_back({TokenType::RB_DASH, "]-"});
            advance(2);
            continue;
        }

        // Single symbols
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
        if (c == '|') { tokens.push_back({TokenType::PIPE, "|"}); advance(); continue; }
        if (c == '&') { tokens.push_back({TokenType::AMP, "&"}); advance(); continue; }

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
        // Backticks (escaped identifiers)
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
            advance(); // Consume end backtick
            tokens.push_back({TokenType::NAME, name});
            continue;
        }

        // Strings
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
            advance(); // Consume end quote
            tokens.push_back({TokenType::STRING_LIT, str});
            continue;
        }

        // Numbers
        if (std::isdigit(c)) {
            std::string num_str;
            while (pos < length && std::isdigit(peek())) {
                num_str += peek();
                advance();
            }
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

        // Names & Keywords
        if (std::isalpha(c) || c == '_') {
            std::string name;
            while (pos < length && (std::isalnum(peek()) || peek() == '_')) {
                name += peek();
                advance();
            }

            std::string upper_name = to_upper(name);
            TokenType type = TokenType::NAME;

            if (upper_name == "MATCH") type = TokenType::MATCH;
            else if (upper_name == "OPTIONAL") type = TokenType::OPTIONAL;
            else if (upper_name == "WHERE") type = TokenType::WHERE;
            else if (upper_name == "RETURN") type = TokenType::RETURN;
            else if (upper_name == "DISTINCT") type = TokenType::DISTINCT;
            else if (upper_name == "LIMIT") type = TokenType::LIMIT;
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
            else if (upper_name == "INSERT") type = TokenType::INSERT;
            else if (upper_name == "DELETE") type = TokenType::DELETE;
            else if (upper_name == "SET") type = TokenType::SET;
            else if (upper_name == "REMOVE") type = TokenType::REMOVE;
            else if (upper_name == "DETACH") type = TokenType::DETACH;
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
            if (type == TokenType::TRUE_KW) {
                tok.int_value = 1;
            } else if (type == TokenType::FALSE_KW) {
                tok.int_value = 0;
            }
            tokens.push_back(tok);
            continue;
        }

        throw std::runtime_error(std::string("Unexpected character '") + c + "' in GQL query");
    }

    tokens.push_back({TokenType::EOF_TOK, ""});
    return tokens;
}

} // namespace ragedb::gql

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

// The scalar-expression grammar: precedence-climbing recursive descent from OR down through the
// arithmetic and unary levels to the primary, plus the postfix comparison/IS/IN operators and the
// braced subquery body that EXISTS/COUNT predicates parse.

#include "GqlParser.h"
#include "GqlValue.h"
#include <stdexcept>
#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <vector>

namespace ragedb::gql {

/**
 * @brief Helper rule to initiate precedence climbing/recursive descent for expressions.
 *
 * @return std::unique_ptr<Expression> The root node of the parsed expression AST.
 */
std::unique_ptr<Expression> GqlParser::parse_expression() {
    return parse_or();
}

/**
 * @brief Parses boolean OR expressions.
 *
 * Precedence level: lowest.
 *
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
std::unique_ptr<Expression> GqlParser::parse_or() {
    auto expr = parse_and();
    while (match(TokenType::OR)) {
        auto right = parse_and();
        expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::OR, std::move(expr), std::move(right));
    }
    return expr;
}

/**
 * @brief Parses boolean AND expressions.
 *
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
std::unique_ptr<Expression> GqlParser::parse_and() {
    auto expr = parse_not();
    while (match(TokenType::AND)) {
        auto right = parse_not();
        expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::AND, std::move(expr), std::move(right));
    }
    return expr;
}

/**
 * @brief Parses boolean NOT. Precedence sits BELOW comparisons/predicates but ABOVE AND, so
 *        `NOT a = b` is `NOT (a = b)` and `NOT x IS NULL` is `NOT (x IS NULL)` -- matching ISO GQL/SQL,
 *        not `(NOT a) = b`. (NOT is a boolean operator; it is not part of the unary arithmetic level.)
 *
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
std::unique_ptr<Expression> GqlParser::parse_not() {
    if (match(TokenType::NOT)) {
        return std::make_unique<UnaryOpExpr>(UnaryOpKind::NOT, parse_not());
    }
    return parse_comparison();
}

/**
 * @brief Parses comparison expressions: =, <>, <, <=, >, >=
 *
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
std::unique_ptr<Expression> GqlParser::parse_comparison() {
    auto expr = parse_add_sub();
    while (true) {
        if (match(TokenType::IS)) {
            bool is_not = false;
            if (match(TokenType::NOT)) {
                is_not = true;
            }
            // `x IS [NOT] LABELED <labelExpression>` -- the label side is the same grammar as a pattern's,
            // so AND/OR/NOT compose there exactly as they do inside a pattern.
            if (match(TokenType::LABELED)) {
                // Symbolic label operators only: a trailing `OR`/`AND` here belongs to the enclosing
                // boolean expression, not to the label.
                expr = std::make_unique<IsLabeledExpr>(std::move(expr), parse_label_expression(false), is_not);
                continue;
            }
            // `e IS [NOT] DIRECTED` and `n IS [NOT] (SOURCE | DESTINATION) OF e`. DIRECTED/SOURCE/
            // DESTINATION/OF are not reserved words, so they arrive as identifiers here.
            if (check(TokenType::NAME)) {
                std::string kw = peek().text;
                std::transform(kw.begin(), kw.end(), kw.begin(), [](unsigned char c){ return std::toupper(c); });
                if (kw == "DIRECTED") {
                    advance();
                    expr = std::make_unique<IsDirectedExpr>(std::move(expr), is_not);
                    continue;
                }
                if (kw == "SOURCE" || kw == "DESTINATION") {
                    bool is_source = (kw == "SOURCE");
                    advance();
                    std::string of_tok = check(TokenType::NAME) ? peek().text : "";
                    std::transform(of_tok.begin(), of_tok.end(), of_tok.begin(), [](unsigned char c){ return std::toupper(c); });
                    if (of_tok != "OF") {
                        throw std::runtime_error("Expected 'OF' after IS [NOT] SOURCE/DESTINATION");
                    }
                    advance(); // consume OF
                    if (!check(TokenType::NAME)) {
                        throw std::runtime_error("Expected a relationship variable after 'OF'");
                    }
                    std::string edge_var = peek().text;
                    advance();
                    expr = std::make_unique<IsSourceDestExpr>(std::move(expr),
                        std::make_unique<VariableExpr>(edge_var), is_source, is_not);
                    continue;
                }
            }
            consume(TokenType::NULL_KW, "Expected 'NULL' or 'LABELED' after 'IS [NOT]'");
            expr = std::make_unique<IsNullExpr>(std::move(expr), is_not);
            continue;
        }

        // x IN [a, b, ...] (a literal list) desugars to (x = a OR x = b OR ...), reusing tested EQ/OR
        // execution; an empty list is always false. x IN <expr> (a list-valued right operand, e.g. a
        // collect_list result) becomes an InExpr evaluated as runtime membership.
        if (match(TokenType::IN_KW)) {
            if (check(TokenType::LBRACKET)) {
                advance(); // consume '['
                std::vector<std::unique_ptr<Expression>> items;
                if (!check(TokenType::RBRACKET)) {
                    do {
                        items.push_back(parse_expression());
                    } while (match(TokenType::COMMA));
                }
                consume(TokenType::RBRACKET, "Expected ']' to close 'IN' list");
                if (items.empty()) {
                    expr = std::make_unique<LiteralExpr>(false);
                } else {
                    std::unique_ptr<Expression> disjunction;
                    for (auto& item : items) {
                        auto eq = std::make_unique<BinaryOpExpr>(BinaryOpKind::EQ, expr->clone(), std::move(item));
                        disjunction = disjunction
                            ? std::make_unique<BinaryOpExpr>(BinaryOpKind::OR, std::move(disjunction), std::move(eq))
                            : std::move(eq);
                    }
                    expr = std::move(disjunction);
                }
            } else {
                auto list_expr = parse_add_sub();
                expr = std::make_unique<InExpr>(std::move(expr), std::move(list_expr));
            }
            continue;
        }

        if (check(TokenType::EQ) || check(TokenType::NE) ||
            check(TokenType::LT) || check(TokenType::LE) ||
            check(TokenType::GT) || check(TokenType::GE) ||
            check(TokenType::STARTS_WITH) || check(TokenType::ENDS_WITH) ||
            check(TokenType::CONTAINS)) {
            TokenType op_type = peek().type;
            advance();

            BinaryOpKind op;
            if (op_type == TokenType::EQ) op = BinaryOpKind::EQ;
            else if (op_type == TokenType::NE) op = BinaryOpKind::NE;
            else if (op_type == TokenType::LT) op = BinaryOpKind::LT;
            else if (op_type == TokenType::LE) op = BinaryOpKind::LE;
            else if (op_type == TokenType::GT) op = BinaryOpKind::GT;
            else if (op_type == TokenType::GE) op = BinaryOpKind::GE;
            else if (op_type == TokenType::STARTS_WITH) op = BinaryOpKind::STARTS_WITH;
            else if (op_type == TokenType::ENDS_WITH) op = BinaryOpKind::ENDS_WITH;
            else op = BinaryOpKind::CONTAINS;

            auto right = parse_add_sub();
            expr = std::make_unique<BinaryOpExpr>(op, std::move(expr), std::move(right));
            continue;
        }

        break;
    }
    return expr;
}

/**
 * @brief Parses additive arithmetic expressions: +, -, and ||
 *
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
std::unique_ptr<Expression> GqlParser::parse_add_sub() {
    auto expr = parse_mul_div();
    while (check(TokenType::PLUS) || check(TokenType::MINUS) || check(TokenType::PIPE_PIPE)) {
        TokenType op_type = peek().type;
        advance();
        BinaryOpKind op;
        if (op_type == TokenType::PLUS) op = BinaryOpKind::ADD;
        else if (op_type == TokenType::MINUS) op = BinaryOpKind::SUB;
        else op = BinaryOpKind::CONCAT;
        auto right = parse_mul_div();
        expr = std::make_unique<BinaryOpExpr>(op, std::move(expr), std::move(right));
    }
    return expr;
}

/**
 * @brief Parses multiplicative arithmetic expressions: * and /
 *
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
std::unique_ptr<Expression> GqlParser::parse_mul_div() {
    auto expr = parse_unary();
    while (check(TokenType::STAR) || check(TokenType::SLASH)) {
        TokenType op_type = peek().type;
        advance();
        BinaryOpKind op = (op_type == TokenType::STAR) ? BinaryOpKind::MUL : BinaryOpKind::DIV;
        auto right = parse_unary();
        expr = std::make_unique<BinaryOpExpr>(op, std::move(expr), std::move(right));
    }
    return expr;
}

/**
 * @brief Parses the unary arithmetic prefix: - (negation). Boolean NOT is not here -- it is a looser
 *        operator handled at parse_not.
 *
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
std::unique_ptr<Expression> GqlParser::parse_unary() {
    // Boolean NOT is handled at parse_not (below comparisons, above AND); only arithmetic negation lives
    // at this unary level.
    if (match(TokenType::MINUS)) {
        auto right = parse_unary();
        return std::make_unique<UnaryOpExpr>(UnaryOpKind::NEG, std::move(right));
    }
    auto expr = parse_primary();
    // Postfix operators after a primary: list subscript expr[index] (a '[' at the head of a primary is a
    // list literal, handled in parse_primary; a '[' after a primary is an element access), and a temporal
    // component accessor expr.year/.month/.day/... -- a second '.' following a property lookup, restricted
    // to known temporal field names so an ordinary chained '.' is unaffected.
    auto is_temporal_field = [](const std::string& n) {
        std::string ln;
        for (char c : n) ln += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return ln == "year" || ln == "month" || ln == "day" ||
               ln == "hour" || ln == "minute" || ln == "second";
    };
    while (true) {
        if (check(TokenType::LBRACKET)) {
            advance(); // consume '['
            auto index = parse_expression();
            consume(TokenType::RBRACKET, "Expected ']' after list index");
            expr = std::make_unique<IndexExpr>(std::move(expr), std::move(index));
        } else if (check(TokenType::DOT) && peek(1).type == TokenType::NAME &&
                   is_temporal_field(peek(1).text)) {
            advance(); // '.'
            std::string field;
            for (char c : peek().text) field += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            advance(); // field name
            expr = std::make_unique<TemporalFieldExpr>(std::move(expr), std::move(field));
        } else {
            break;
        }
    }
    return expr;
}

/**
 * @brief Parses the braced body of a subquery predicate (EXISTS { ... } / COUNT { ... }).
 *
 * Handles the MATCH / OPTIONAL MATCH statements and the full-text SEARCH form inside the braces, the
 * openCypher-style bare pattern shorthand, and a trailing WHERE.
 *
 * @param matches Output vector receiving the subquery's MATCH statements.
 * @param sub_where Output receiving the subquery's WHERE expression, or null if absent.
 */
void GqlParser::parse_braced_subquery(std::vector<MatchStatement>& matches,
                                      std::unique_ptr<Expression>& sub_where) {
    consume(TokenType::LBRACE, "Expected '{' to start subquery");
    int sub_match_id = 0;
    {
        while (check(TokenType::MATCH) || (check(TokenType::OPTIONAL) && peek(1).type == TokenType::MATCH)) {
            MatchStatement stmt;
            if (match(TokenType::OPTIONAL)) {
                stmt.is_optional = true;
            }
            consume(TokenType::MATCH, "Expected MATCH");
            if (check(TokenType::NAME) && peek(1).type == TokenType::IN_KW) {
                stmt.is_search = true;
                stmt.search_var = peek().text;
                consume(TokenType::NAME, "Expected variable name before 'IN'");
                consume(TokenType::IN_KW, "Expected 'IN'");
                consume(TokenType::SEARCH, "Expected 'SEARCH'");

                if (match(TokenType::LPAREN)) {
                    do {
                        std::string type_name = peek().text;
                        consume(TokenType::NAME, "Expected type name");
                        consume(TokenType::DOT, "Expected '.'");
                        std::string prop_name = peek().text;
                        consume(TokenType::NAME, "Expected property name");

                        stmt.search_type = type_name;
                        stmt.search_properties.push_back(prop_name);
                    } while (match(TokenType::COMMA));
                    consume(TokenType::RPAREN, "Expected ')' after property list");
                } else {
                    std::string type_name = peek().text;
                    consume(TokenType::NAME, "Expected type name");
                    consume(TokenType::DOT, "Expected '.'");
                    std::string prop_name = peek().text;
                    consume(TokenType::NAME, "Expected property name");

                    stmt.search_type = type_name;
                    stmt.search_properties.push_back(prop_name);
                }

                consume(TokenType::FOR, "Expected 'FOR'");
                stmt.search_string = peek().text;
                consume(TokenType::STRING_LIT, "Expected query string literal after 'FOR'");

                if (match(TokenType::OPTIONS)) {
                    consume(TokenType::LBRACE, "Expected '{' after OPTIONS");
                    if (!check(TokenType::RBRACE)) {
                        do {
                            std::string opt_key = peek().text;
                            consume(TokenType::NAME, "Expected option key identifier");
                            consume(TokenType::COLON, "Expected ':' after option key");

                            std::string opt_val;
                            if (check(TokenType::STRING_LIT)) {
                                opt_val = peek().text;
                                consume(TokenType::STRING_LIT, "Expected string literal");
                            } else if (check(TokenType::TRUE_KW)) {
                                opt_val = "true";
                                consume(TokenType::TRUE_KW, "Expected true");
                            } else if (check(TokenType::FALSE_KW)) {
                                opt_val = "false";
                                consume(TokenType::FALSE_KW, "Expected false");
                            } else if (check(TokenType::NAME)) {
                                opt_val = peek().text;
                                consume(TokenType::NAME, "Expected name identifier");
                            } else {
                                throw std::runtime_error("Expected option value");
                            }
                            stmt.search_options[opt_key] = opt_val;
                        } while (match(TokenType::COMMA));
                    }
                    consume(TokenType::RBRACE, "Expected '}' to close OPTIONS");
                }

                consume(TokenType::YIELD, "Expected 'YIELD'");
                stmt.yield_var = peek().text;
                consume(TokenType::NAME, "Expected variable name to bind search result");
                consume(TokenType::COMMA, "Expected ','");
                stmt.yield_score_var = peek().text;
                consume(TokenType::NAME, "Expected score variable name");
            } else {
                stmt.pattern = parse_path_pattern();
            }
            stmt.id = sub_match_id++;
            matches.push_back(std::move(stmt));
        }
        // openCypher-style bare pattern subquery, e.g. EXISTS { (a)-[:KNOWS]-(b) }: treat the
        // pattern as an implicit MATCH (this is the form the LDBC queries use).
        if (matches.empty() && !check(TokenType::RBRACE) && !check(TokenType::WHERE)) {
            MatchStatement stmt;
            stmt.pattern = parse_path_pattern();
            stmt.id = sub_match_id++;
            matches.push_back(std::move(stmt));
        }
    }
    sub_where = nullptr;
    if (match(TokenType::WHERE)) {
        sub_where = parse_expression();
    }
    consume(TokenType::RBRACE, "Expected '}' after subquery");
}

}  // namespace ragedb::gql

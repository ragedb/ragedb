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

#include "GqlParser.h"
#include <stdexcept>

namespace ragedb::gql {

const Token& GqlParser::peek(size_t offset) const {
    if (pos + offset >= tokens.size()) {
        return tokens.back();
    }
    return tokens[pos + offset];
}

const Token& GqlParser::advance() {
    if (pos < tokens.size()) {
        pos++;
    }
    return tokens[pos - 1];
}

bool GqlParser::check(TokenType type) const {
    return peek().type == type;
}

bool GqlParser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

void GqlParser::consume(TokenType type, const std::string& error_message) {
    if (check(type)) {
        advance();
        return;
    }
    throw std::runtime_error(error_message + " (found: " + peek().text + ")");
}

GqlQuery GqlParser::parse_query() {
    GqlQuery query;

    // Parse MATCH/OPTIONAL MATCH clauses
    while (check(TokenType::MATCH) || check(TokenType::OPTIONAL)) {
        MatchStatement stmt;
        if (match(TokenType::OPTIONAL)) {
            stmt.is_optional = true;
            consume(TokenType::MATCH, "Expected 'MATCH' after 'OPTIONAL'");
        } else {
            consume(TokenType::MATCH, "Expected 'MATCH'");
        }
        stmt.pattern = parse_path_pattern();
        query.matches.push_back(stmt);
    }

    // Parse optional global WHERE clause
    if (match(TokenType::WHERE)) {
        query.where_expr = parse_expression();
    }

    // Parse Write Operations (INSERT, SET, REMOVE, DELETE, DETACH)
    while (check(TokenType::INSERT) || check(TokenType::SET) || check(TokenType::REMOVE) || check(TokenType::DELETE) || check(TokenType::DETACH)) {
        if (match(TokenType::INSERT)) {
            WriteOp op;
            op.type = WriteOp::Type::INSERT;
            op.insert_pattern = parse_path_pattern();
            query.writes.push_back(std::move(op));
        } else if (match(TokenType::SET)) {
            do {
                std::string var = peek().text;
                consume(TokenType::NAME, "Expected variable name in SET");
                consume(TokenType::DOT, "Expected '.' after variable in SET");
                std::string prop = peek().text;
                consume(TokenType::NAME, "Expected property name in SET");
                consume(TokenType::EQ, "Expected '=' after property in SET");
                auto expr = parse_expression();

                WriteOp op;
                op.type = WriteOp::Type::SET;
                op.set_var = var;
                op.set_prop = prop;
                op.set_expr = std::move(expr);
                query.writes.push_back(std::move(op));
            } while (match(TokenType::COMMA));
        } else if (match(TokenType::REMOVE)) {
            do {
                std::string var = peek().text;
                consume(TokenType::NAME, "Expected variable name in REMOVE");
                consume(TokenType::DOT, "Expected '.' after variable in REMOVE");
                std::string prop = peek().text;
                consume(TokenType::NAME, "Expected property name in REMOVE");

                WriteOp op;
                op.type = WriteOp::Type::REMOVE;
                op.remove_var = var;
                op.remove_prop = prop;
                query.writes.push_back(std::move(op));
            } while (match(TokenType::COMMA));
        } else {
            bool detach = false;
            if (match(TokenType::DETACH)) {
                detach = true;
                consume(TokenType::DELETE, "Expected 'DELETE' after 'DETACH'");
            } else {
                consume(TokenType::DELETE, "Expected 'DELETE'");
            }
            do {
                std::string var = peek().text;
                consume(TokenType::NAME, "Expected variable name to delete");

                WriteOp op;
                op.type = WriteOp::Type::DELETE_OP;
                op.delete_var = var;
                op.detach = detach;
                query.writes.push_back(std::move(op));
            } while (match(TokenType::COMMA));
        }
    }

    // Verify we have at least one MATCH or write operation
    if (query.matches.empty() && query.writes.empty()) {
        throw std::runtime_error("Query must contain at least one MATCH or write clause");
    }

    // Parse RETURN clause (optional if we performed writes, otherwise mandatory)
    if (match(TokenType::RETURN)) {
        if (match(TokenType::DISTINCT)) {
            query.distinct = true;
        }

        do {
            ReturnItem item;
            item.expr = parse_expression();
            if (match(TokenType::AS)) {
                std::string alias = peek().text;
                consume(TokenType::NAME, "Expected alias name after 'AS'");
                item.alias = alias;
            }
            query.returns.push_back(std::move(item));
        } while (match(TokenType::COMMA));

        // Parse optional ORDER BY clause
        if (match(TokenType::ORDER_BY)) {
            do {
                SortSpec spec;
                spec.expr = parse_expression();
                if (match(TokenType::ASC)) {
                    spec.ascending = true;
                } else if (match(TokenType::DESC)) {
                    spec.ascending = false;
                } else {
                    spec.ascending = true; // default
                }
                query.order_by.push_back(std::move(spec));
            } while (match(TokenType::COMMA));
        }

        // Parse optional LIMIT clause
        if (match(TokenType::LIMIT)) {
            std::string num_str = peek().text;
            consume(TokenType::NUMBER, "Expected integer limit value");
            query.limit = std::stoull(num_str);
        }
    } else {
        if (query.writes.empty()) {
            throw std::runtime_error("Query must contain either a RETURN clause or at least one write clause");
        }
    }

    consume(TokenType::EOF_TOK, "Expected end of query");
    return query;
}

PathPattern GqlParser::parse_path_pattern() {
    PathPattern pattern;
    pattern.nodes.push_back(parse_node_pattern());

    while (check(TokenType::RIGHT_ARROW) || check(TokenType::LEFT_ARROW) ||
           check(TokenType::DASH_LB) || check(TokenType::LT_DASH_LB) ||
           check(TokenType::MINUS)) {

        PatternEdge edge;
        if (match(TokenType::RIGHT_ARROW)) {
            edge.direction = EdgeDirection::RIGHT;
        } else if (match(TokenType::LEFT_ARROW)) {
            edge.direction = EdgeDirection::LEFT;
        } else if (match(TokenType::MINUS)) {
            edge.direction = EdgeDirection::ANY;
        } else if (match(TokenType::DASH_LB)) {
            // Outgoing or Undirected Edge with detail: -[e:L {p:v}]-> or -[e:L {p:v}]-
            if (check(TokenType::NAME)) {
                edge.variable = peek().text;
                advance();
            }
            if (match(TokenType::COLON) || match(TokenType::IS)) {
                edge.label = peek().text;
                consume(TokenType::NAME, "Expected edge type label after ':' or 'IS'");
            }
            if (check(TokenType::LBRACE)) {
                edge.properties = parse_properties();
            }

            if (match(TokenType::RB_DASH_GT)) {
                edge.direction = EdgeDirection::RIGHT;
            } else {
                consume(TokenType::RB_DASH, "Expected ']-' or ']->' to end relationship description");
                edge.direction = EdgeDirection::ANY;
            }
        } else if (match(TokenType::LT_DASH_LB)) {
            // Incoming Edge with detail: <-[e:L {p:v}]-
            if (check(TokenType::NAME)) {
                edge.variable = peek().text;
                advance();
            }
            if (match(TokenType::COLON) || match(TokenType::IS)) {
                edge.label = peek().text;
                consume(TokenType::NAME, "Expected edge type label after ':' or 'IS'");
            }
            if (check(TokenType::LBRACE)) {
                edge.properties = parse_properties();
            }
            consume(TokenType::RB_DASH, "Expected ']-' to end incoming relationship description");
            edge.direction = EdgeDirection::LEFT;
        }

        pattern.edges.push_back(edge);
        pattern.nodes.push_back(parse_node_pattern());
    }

    return pattern;
}

PatternNode GqlParser::parse_node_pattern() {
    PatternNode node;
    consume(TokenType::LPAREN, "Expected '(' to start node pattern");

    if (check(TokenType::NAME)) {
        node.variable = peek().text;
        advance();
    }
    if (match(TokenType::COLON) || match(TokenType::IS)) {
        node.label = peek().text;
        consume(TokenType::NAME, "Expected node label after ':' or 'IS'");
    }
    if (check(TokenType::LBRACE)) {
        node.properties = parse_properties();
    }

    consume(TokenType::RPAREN, "Expected ')' to end node pattern");
    return node;
}

std::map<std::string, property_type_t> GqlParser::parse_properties() {
    std::map<std::string, property_type_t> props;
    consume(TokenType::LBRACE, "Expected '{' to start property map");
    if (!check(TokenType::RBRACE)) {
        do {
            std::string key = peek().text;
            consume(TokenType::NAME, "Expected property key name");
            consume(TokenType::COLON, "Expected ':' after property key");

            property_type_t value;
            if (match(TokenType::TRUE_KW)) {
                value = true;
            } else if (match(TokenType::FALSE_KW)) {
                value = false;
            } else if (match(TokenType::NULL_KW)) {
                value = std::monostate{};
            } else if (check(TokenType::NUMBER)) {
                value = peek().int_value;
                advance();
            } else if (check(TokenType::FLOAT_LIT)) {
                value = peek().float_value;
                advance();
            } else if (check(TokenType::STRING_LIT)) {
                value = peek().text;
                advance();
            } else {
                throw std::runtime_error("Expected literal value for property map");
            }

            props[key] = value;
        } while (match(TokenType::COMMA));
    }
    consume(TokenType::RBRACE, "Expected '}' to end property map");
    return props;
}

std::unique_ptr<Expression> GqlParser::parse_expression() {
    return parse_or();
}

std::unique_ptr<Expression> GqlParser::parse_or() {
    auto expr = parse_and();
    while (match(TokenType::OR)) {
        auto right = parse_and();
        expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::OR, std::move(expr), std::move(right));
    }
    return expr;
}

std::unique_ptr<Expression> GqlParser::parse_and() {
    auto expr = parse_comparison();
    while (match(TokenType::AND)) {
        auto right = parse_comparison();
        expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::AND, std::move(expr), std::move(right));
    }
    return expr;
}

std::unique_ptr<Expression> GqlParser::parse_comparison() {
    auto expr = parse_add_sub();
    while (check(TokenType::EQ) || check(TokenType::NE) ||
           check(TokenType::LT) || check(TokenType::LE) ||
           check(TokenType::GT) || check(TokenType::GE)) {
        TokenType op_type = peek().type;
        advance();

        BinaryOpKind op;
        if (op_type == TokenType::EQ) op = BinaryOpKind::EQ;
        else if (op_type == TokenType::NE) op = BinaryOpKind::NE;
        else if (op_type == TokenType::LT) op = BinaryOpKind::LT;
        else if (op_type == TokenType::LE) op = BinaryOpKind::LE;
        else if (op_type == TokenType::GT) op = BinaryOpKind::GT;
        else op = BinaryOpKind::GE;

        auto right = parse_add_sub();
        expr = std::make_unique<BinaryOpExpr>(op, std::move(expr), std::move(right));
    }
    return expr;
}

std::unique_ptr<Expression> GqlParser::parse_add_sub() {
    auto expr = parse_mul_div();
    while (check(TokenType::PLUS) || check(TokenType::MINUS)) {
        TokenType op_type = peek().type;
        advance();
        BinaryOpKind op = (op_type == TokenType::PLUS) ? BinaryOpKind::ADD : BinaryOpKind::SUB;
        auto right = parse_mul_div();
        expr = std::make_unique<BinaryOpExpr>(op, std::move(expr), std::move(right));
    }
    return expr;
}

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

std::unique_ptr<Expression> GqlParser::parse_unary() {
    if (match(TokenType::NOT)) {
        auto right = parse_unary();
        return std::make_unique<UnaryOpExpr>(UnaryOpKind::NOT, std::move(right));
    }
    if (match(TokenType::MINUS)) {
        auto right = parse_unary();
        return std::make_unique<UnaryOpExpr>(UnaryOpKind::NEG, std::move(right));
    }
    return parse_primary();
}

std::unique_ptr<Expression> GqlParser::parse_primary() {
    if (match(TokenType::TRUE_KW)) {
        return std::make_unique<LiteralExpr>(true);
    }
    if (match(TokenType::FALSE_KW)) {
        return std::make_unique<LiteralExpr>(false);
    }
    if (match(TokenType::NULL_KW)) {
        return std::make_unique<LiteralExpr>(std::monostate{});
    }
    if (check(TokenType::NUMBER)) {
        int64_t val = peek().int_value;
        advance();
        return std::make_unique<LiteralExpr>(val);
    }
    if (check(TokenType::FLOAT_LIT)) {
        double val = peek().float_value;
        advance();
        return std::make_unique<LiteralExpr>(val);
    }
    if (check(TokenType::STRING_LIT)) {
        std::string val = peek().text;
        advance();
        return std::make_unique<LiteralExpr>(val);
    }
    if (match(TokenType::LPAREN)) {
        auto expr = parse_expression();
        consume(TokenType::RPAREN, "Expected ')' after expression");
        return expr;
    }
    if (check(TokenType::NAME)) {
        std::string var = peek().text;
        advance();
        if (match(TokenType::DOT)) {
            std::string prop = peek().text;
            consume(TokenType::NAME, "Expected property name after '.'");
            return std::make_unique<PropertyLookupExpr>(var, prop);
        }
        return std::make_unique<VariableExpr>(var);
    }

    throw std::runtime_error("Unexpected token in expression: " + peek().text);
}

GqlQuery GqlParser::parse(const std::string& query) {
    auto tokens = GqlLexer::tokenize(query);
    GqlParser parser(tokens);
    return parser.parse_query();
}

} // namespace ragedb::gql

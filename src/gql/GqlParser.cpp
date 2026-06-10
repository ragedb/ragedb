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
#include <algorithm>
#include <cctype>
#include <limits>
#include <memory>

namespace ragedb::gql {

/**
 * @brief Peek at a token in the stream without advancing the cursor.
 * 
 * @param offset The number of tokens ahead of the current position to inspect.
 * @return const Token& The token at the specified offset, or the EOF/last token if out of bounds.
 */
const Token& GqlParser::peek(size_t offset) const {
    if (pos + offset >= tokens.size()) {
        return tokens.back();
    }
    return tokens[pos + offset];
}

/**
 * @brief Consume the current token and advance the stream position.
 * 
 * @return const Token& The token that was current before advancing.
 */
const Token& GqlParser::advance() {
    if (pos < tokens.size()) {
        pos++;
    }
    return tokens[pos - 1];
}

/**
 * @brief Check if the current token matches the specified type.
 * 
 * @param type The token type to compare against.
 * @return true If the current token's type matches.
 * @return false Otherwise.
 */
bool GqlParser::check(TokenType type) const {
    return peek().type == type;
}

/**
 * @brief If the current token matches the specified type, consume it and return true.
 * 
 * @param type The token type to match.
 * @return true If the token was matched and consumed.
 * @return false If the token did not match, leaving the cursor unchanged.
 */
bool GqlParser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

/**
 * @brief Assert that the current token matches the expected type and consume it.
 *        Throws an exception if the token type does not match.
 * 
 * @param type The expected token type.
 * @param error_message The descriptive message to include in the thrown exception.
 * @throws std::runtime_error If the current token type does not match.
 */
void GqlParser::consume(TokenType type, const std::string& error_message) {
    if (check(type)) {
        advance();
        return;
    }
    throw std::runtime_error(error_message + " (found: " + peek().text + ")");
}

static void validate_insert_label_expr(const std::shared_ptr<LabelExpression>& expr) {
    if (!expr) return;
    if (expr->kind != LabelExprKind::LITERAL) {
        throw std::runtime_error("Complex label expressions (AND, OR, NOT) are not allowed in INSERT statements");
    }
}

/**
 * @brief Parses a complete GQL query into a GqlQuery AST object.
 * 
 * Main entry point of the recursive descent parser. Processes MATCH, OPTIONAL MATCH,
 * global WHERE, write operations (INSERT, SET, REMOVE, DELETE, DETACH DELETE),
 * and RETURN clauses with sorting and limits.
 * 
 * @return GqlQuery The constructed query AST object.
 * @throws std::runtime_error If syntax errors are encountered.
 */
GqlQuery GqlParser::parse_single_query() {
    GqlQuery query;

    int match_id_counter = 0;
    // Parse MATCH/OPTIONAL MATCH clauses
    while (check(TokenType::MATCH) || (check(TokenType::OPTIONAL) && peek(1).type == TokenType::MATCH)) {
        MatchStatement stmt;
        if (match(TokenType::OPTIONAL)) {
            stmt.is_optional = true;
        }
        consume(TokenType::MATCH, "Expected MATCH");
        stmt.pattern = parse_path_pattern();
        stmt.id = match_id_counter++;
        query.matches.push_back(std::move(stmt));
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
            for (const auto& node : op.insert_pattern.nodes) {
                validate_insert_label_expr(node.label_expr);
            }
            for (const auto& edge : op.insert_pattern.edges) {
                validate_insert_label_expr(edge.label_expr);
            }
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
    } else {
        if (query.writes.empty()) {
            throw std::runtime_error("Query must contain either a RETURN clause or at least one write clause");
        }
    }

    return query;
}

GqlQuery GqlParser::parse_query() {
    bool explain = false;
    bool profile = false;
    if (match(TokenType::EXPLAIN)) {
        explain = true;
    } else if (match(TokenType::PROFILE)) {
        profile = true;
    }

    if (match(TokenType::CALL)) {
        consume(TokenType::CLEAR, "Expected 'CLEAR' after 'CALL'");
        consume(TokenType::CACHE, "Expected 'CACHE' after 'CLEAR'");
        GqlQuery query;
        query.clear_cache = true;
        query.explain = explain;
        query.profile = profile;
        return query;
    }

    if (check(TokenType::CREATE) || check(TokenType::DROP) || check(TokenType::ALTER) || check(TokenType::SHOW)) {
        GqlQuery query;
        SchemaOperation schema;
        
        if (match(TokenType::CREATE)) {
            if (match(TokenType::INDEX)) {
                std::string type_name = peek().text;
                consume(TokenType::NAME, "Expected type name identifier");
                consume(TokenType::DOT, "Expected '.'");
                std::string property_name = peek().text;
                consume(TokenType::NAME, "Expected property name identifier");
                schema.op = SchemaOperation::Op::CREATE_INDEX;
                schema.name = type_name;
                schema.alter_property_name = property_name;
            } else {
                bool is_node = false;
                if (match(TokenType::NODE)) {
                    is_node = true;
                } else if (match(TokenType::RELATIONSHIP)) {
                    is_node = false;
                } else {
                    throw std::runtime_error("Expected 'NODE', 'RELATIONSHIP', 'REL', or 'INDEX' after 'CREATE'");
                }
                consume(TokenType::TYPE, "Expected 'TYPE' keyword");
                
                std::string type_name = peek().text;
                consume(TokenType::NAME, "Expected type name identifier");
                
                schema.op = is_node ? SchemaOperation::Op::CREATE_NODE_TYPE : SchemaOperation::Op::CREATE_REL_TYPE;
                schema.name = type_name;
                
                // Parse optional properties
                if (match(TokenType::LPAREN)) {
                    do {
                        std::string prop_name = peek().text;
                        consume(TokenType::NAME, "Expected property name identifier");
                        
                        std::string data_type;
                        if (match(TokenType::STRING_KW)) data_type = "string";
                        else if (match(TokenType::INTEGER_KW)) data_type = "integer";
                        else if (match(TokenType::DOUBLE_KW)) data_type = "double";
                        else if (match(TokenType::BOOLEAN_KW)) data_type = "boolean";
                        else if (match(TokenType::STRING_LIST_KW)) data_type = "string_list";
                        else if (match(TokenType::INTEGER_LIST_KW)) data_type = "integer_list";
                        else if (match(TokenType::DOUBLE_LIST_KW)) data_type = "double_list";
                        else if (match(TokenType::BOOLEAN_LIST_KW)) data_type = "boolean_list";
                        else {
                            throw std::runtime_error("Expected datatype (STRING, INTEGER, DOUBLE, BOOLEAN, or list variants) for property '" + prop_name + "'");
                        }
                        
                        schema.properties.push_back({prop_name, data_type});
                    } while (match(TokenType::COMMA));
                    consume(TokenType::RPAREN, "Expected ')' to close property list");
                }
            }
        }
        else if (match(TokenType::DROP)) {
            if (match(TokenType::INDEX)) {
                std::string type_name = peek().text;
                consume(TokenType::NAME, "Expected type name identifier");
                consume(TokenType::DOT, "Expected '.'");
                std::string property_name = peek().text;
                consume(TokenType::NAME, "Expected property name identifier");
                schema.op = SchemaOperation::Op::DROP_INDEX;
                schema.name = type_name;
                schema.alter_property_name = property_name;
            } else {
                bool is_node = false;
                if (match(TokenType::NODE)) {
                    is_node = true;
                } else if (match(TokenType::RELATIONSHIP)) {
                    is_node = false;
                } else {
                    throw std::runtime_error("Expected 'NODE', 'RELATIONSHIP', 'REL', or 'INDEX' after 'DROP'");
                }
                consume(TokenType::TYPE, "Expected 'TYPE' keyword");
                
                std::string type_name = peek().text;
                consume(TokenType::NAME, "Expected type name identifier");
                
                schema.op = is_node ? SchemaOperation::Op::DROP_NODE_TYPE : SchemaOperation::Op::DROP_REL_TYPE;
                schema.name = type_name;
            }
        }
        else if (match(TokenType::ALTER)) {
            bool is_node = false;
            if (match(TokenType::NODE)) {
                is_node = true;
            } else if (match(TokenType::RELATIONSHIP)) {
                is_node = false;
            } else {
                throw std::runtime_error("Expected 'NODE' or 'RELATIONSHIP' / 'REL' after 'ALTER'");
            }
            consume(TokenType::TYPE, "Expected 'TYPE' keyword");
            
            std::string type_name = peek().text;
            consume(TokenType::NAME, "Expected type name identifier");
            
            schema.op = is_node ? SchemaOperation::Op::ALTER_NODE_TYPE : SchemaOperation::Op::ALTER_REL_TYPE;
            schema.name = type_name;
            
            if (match(TokenType::ADD)) {
                schema.alter_op = SchemaOperation::AlterOp::ADD;
                std::string prop_name = peek().text;
                consume(TokenType::NAME, "Expected property name identifier");
                
                std::string data_type;
                if (match(TokenType::STRING_KW)) data_type = "string";
                else if (match(TokenType::INTEGER_KW)) data_type = "integer";
                else if (match(TokenType::DOUBLE_KW)) data_type = "double";
                else if (match(TokenType::BOOLEAN_KW)) data_type = "boolean";
                else if (match(TokenType::STRING_LIST_KW)) data_type = "string_list";
                else if (match(TokenType::INTEGER_LIST_KW)) data_type = "integer_list";
                else if (match(TokenType::DOUBLE_LIST_KW)) data_type = "double_list";
                else if (match(TokenType::BOOLEAN_LIST_KW)) data_type = "boolean_list";
                else {
                    throw std::runtime_error("Expected datatype for property '" + prop_name + "'");
                }
                schema.alter_property_name = prop_name;
                schema.alter_property_type = data_type;
            }
            else if (match(TokenType::DROP)) {
                schema.alter_op = SchemaOperation::AlterOp::DROP;
                std::string prop_name = peek().text;
                consume(TokenType::NAME, "Expected property name identifier");
                schema.alter_property_name = prop_name;
            }
            else {
                throw std::runtime_error("Expected 'ADD' or 'DROP' operation after type name in ALTER TYPE statement");
            }
        }
        else if (match(TokenType::SHOW)) {
            consume(TokenType::INDEXES, "Expected 'INDEXES' after 'SHOW'");
            schema.op = SchemaOperation::Op::SHOW_INDEXES;
            if (match(TokenType::ON)) {
                std::string type_name = peek().text;
                consume(TokenType::NAME, "Expected type name identifier after 'ON'");
                schema.name = type_name;
            } else {
                schema.name = "";
            }
        }
        
        query.schema_op = std::move(schema);
        query.explain = explain;
        query.profile = profile;
        consume(TokenType::EOF_TOK, "Expected end of query");
        return query;
    }

    GqlQuery query = parse_union();

    // Parse optional top-level ORDER BY clause
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

    // Parse optional top-level LIMIT clause
    if (match(TokenType::LIMIT)) {
        std::string num_str = peek().text;
        consume(TokenType::NUMBER, "Expected integer limit value");
        query.limit = std::stoull(num_str);
    }

    query.explain = explain;
    query.profile = profile;

    consume(TokenType::EOF_TOK, "Expected end of query");
    return query;
}

GqlQuery GqlParser::parse_union() {
    GqlQuery query = parse_intersect();
    while (match(TokenType::UNION)) {
        bool all = false;
        if (match(TokenType::ALL_KW)) {
            all = true;
        }
        GqlQuery right = parse_intersect();

        GqlQuery combined;
        combined.kind = all ? QueryKind::UNION_ALL : QueryKind::UNION;
        combined.left = std::make_unique<GqlQuery>(std::move(query));
        combined.right = std::make_unique<GqlQuery>(std::move(right));
        query = std::move(combined);
    }
    return query;
}

GqlQuery GqlParser::parse_intersect() {
    GqlQuery query = parse_single_query();
    while (match(TokenType::INTERSECT)) {
        bool all = false;
        if (match(TokenType::ALL_KW)) {
            all = true;
        }
        GqlQuery right = parse_single_query();

        GqlQuery combined;
        combined.kind = all ? QueryKind::INTERSECT_ALL : QueryKind::INTERSECT;
        combined.left = std::make_unique<GqlQuery>(std::move(query));
        combined.right = std::make_unique<GqlQuery>(std::move(right));
        query = std::move(combined);
    }
    return query;
}

void GqlParser::parse_edge_details(PatternEdge& edge) {
    if (check(TokenType::NAME)) {
        edge.variable = peek().text;
        advance();
    }
    if (match(TokenType::COLON) || match(TokenType::IS)) {
        edge.label_expr = parse_label_expression();
    }
    if (match(TokenType::STAR)) {
        edge.is_variable_length = true;
        edge.min_hops = 1;
        edge.max_hops = std::numeric_limits<uint64_t>::max();
        if (check(TokenType::NUMBER)) {
            uint64_t val = peek().int_value;
            advance();
            if (match(TokenType::DOT)) {
                consume(TokenType::DOT, "Expected '..' for repetition range");
                edge.min_hops = val;
                if (check(TokenType::NUMBER)) {
                    edge.max_hops = peek().int_value;
                    advance();
                }
            } else {
                edge.min_hops = val;
                edge.max_hops = val;
            }
        } else if (match(TokenType::DOT)) {
            consume(TokenType::DOT, "Expected '..' for repetition range");
            edge.min_hops = 1;
            if (check(TokenType::NUMBER)) {
                edge.max_hops = peek().int_value;
                advance();
            }
        }
    }
    if (check(TokenType::LBRACE)) {
        edge.properties = parse_properties();
    }
}

/**
 * @brief Parses a path pattern consisting of alternating nodes and edges.
 * 
 * Path patterns are defined as node-edge-node structures.
 * 
 * @return PathPattern The parsed path pattern AST structure.
 */
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
            parse_edge_details(edge);
            if (match(TokenType::RB_DASH_GT)) {
                edge.direction = EdgeDirection::RIGHT;
            } else {
                consume(TokenType::RB_DASH, "Expected ']-' or ']->' to end relationship description");
                edge.direction = EdgeDirection::ANY;
            }
        } else if (match(TokenType::LT_DASH_LB)) {
            // Incoming Edge with detail: <-[e:L {p:v}]-
            parse_edge_details(edge);
            consume(TokenType::RB_DASH, "Expected ']-' to end incoming relationship description");
            edge.direction = EdgeDirection::LEFT;
        }

        pattern.edges.push_back(edge);
        pattern.nodes.push_back(parse_node_pattern());
    }

    return pattern;
}

/**
 * @brief Parses a node pattern of the form: (variable:Label {prop1: val1, ...})
 * 
 * @return PatternNode The parsed node pattern AST structure.
 */
PatternNode GqlParser::parse_node_pattern() {
    PatternNode node;
    consume(TokenType::LPAREN, "Expected '(' to start node pattern");

    if (check(TokenType::NAME)) {
        node.variable = peek().text;
        advance();
    }
    if (match(TokenType::COLON) || match(TokenType::IS)) {
        node.label_expr = parse_label_expression();
    }
    if (check(TokenType::LBRACE)) {
        node.properties = parse_properties();
    }

    consume(TokenType::RPAREN, "Expected ')' to end node pattern");
    return node;
}

/**
 * @brief Parses property maps in nodes or edges: {name: 'John', age: 30}
 * 
 * @return std::map<std::string, property_type_t> The map of parsed keys to typed literal values.
 */
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
    auto expr = parse_comparison();
    while (match(TokenType::AND)) {
        auto right = parse_comparison();
        expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::AND, std::move(expr), std::move(right));
    }
    return expr;
}

/**
 * @brief Parses comparison expressions: =, <>, <, <=, >, >=
 * 
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
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

/**
 * @brief Parses additive arithmetic expressions: + and -
 * 
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
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
 * @brief Parses unary prefix expressions: NOT and - (negative)
 * 
 * @return std::unique_ptr<Expression> Parsed expression node.
 */
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

/**
 * @brief Parses primary expressions: literals, variables, property lookups, and parenthesized sub-expressions.
 * 
 * Precedence level: highest.
 * 
 * @return std::unique_ptr<Expression> Parsed expression node.
 * @throws std::runtime_error If an unexpected token or syntax issue is encountered.
 */
std::unique_ptr<Expression> GqlParser::parse_primary() {
    if (match(TokenType::EXISTS)) {
        consume(TokenType::LBRACE, "Expected '{' after EXISTS");
        std::vector<MatchStatement> matches;
        while (check(TokenType::MATCH) || (check(TokenType::OPTIONAL) && peek(1).type == TokenType::MATCH)) {
            MatchStatement stmt;
            if (match(TokenType::OPTIONAL)) {
                stmt.is_optional = true;
            }
            consume(TokenType::MATCH, "Expected MATCH");
            stmt.pattern = parse_path_pattern();
            matches.push_back(stmt);
        }
        std::unique_ptr<Expression> sub_where = nullptr;
        if (match(TokenType::WHERE)) {
            sub_where = parse_expression();
        }
        consume(TokenType::RBRACE, "Expected '}' after EXISTS subquery");
        return std::make_unique<ExistsExpr>(std::move(matches), std::move(sub_where));
    }
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
        // Check for aggregate function call: NAME '('
        if (peek(1).type == TokenType::LPAREN) {
            std::string upper_name = var;
            std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), [](unsigned char c){ return std::toupper(c); });
            if (upper_name == "COUNT" || upper_name == "SUM" || upper_name == "AVG" || upper_name == "MIN" || upper_name == "MAX") {
                advance(); // consume function name
                consume(TokenType::LPAREN, "Expected '(' after aggregate function");
                
                std::unique_ptr<Expression> arg = nullptr;
                // Special case for count(*)
                if (upper_name == "COUNT" && check(TokenType::STAR)) {
                    advance(); // consume '*'
                } else {
                    arg = parse_expression();
                }
                consume(TokenType::RPAREN, "Expected ')' after aggregate function argument");
                
                AggregateKind fn;
                if (upper_name == "COUNT") fn = AggregateKind::COUNT;
                else if (upper_name == "SUM") fn = AggregateKind::SUM;
                else if (upper_name == "AVG") fn = AggregateKind::AVG;
                else if (upper_name == "MIN") fn = AggregateKind::MIN;
                else fn = AggregateKind::MAX;
                
                return std::make_unique<AggregateExpr>(fn, std::move(arg));
            }
        }

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

/**
 * @brief Class helper method to tokenize and parse a GQL query string into its AST.
 * 
 * @param query The GQL query string to process.
 * @return GqlQuery The parsed AST query object.
 */
GqlQuery GqlParser::parse(const std::string& query) {
    auto tokens = GqlLexer::tokenize(query);
    GqlParser parser(tokens);
    return parser.parse_query();
}

std::shared_ptr<LabelExpression> GqlParser::parse_label_expression() {
    return parse_label_or();
}

std::shared_ptr<LabelExpression> GqlParser::parse_label_or() {
    auto left = parse_label_and();
    while (check(TokenType::PIPE) || check(TokenType::OR)) {
        advance(); // consume '|' or 'OR'
        auto right = parse_label_and();
        auto node = std::make_shared<LabelExpression>();
        node->kind = LabelExprKind::OR;
        node->left = std::move(left);
        node->right = std::move(right);
        left = std::move(node);
    }
    return left;
}

std::shared_ptr<LabelExpression> GqlParser::parse_label_and() {
    auto left = parse_label_factor();
    while (check(TokenType::AMP) || check(TokenType::AND)) {
        advance(); // consume '&' or 'AND'
        auto right = parse_label_factor();
        auto node = std::make_shared<LabelExpression>();
        node->kind = LabelExprKind::AND;
        node->left = std::move(left);
        node->right = std::move(right);
        left = std::move(node);
    }
    return left;
}

std::shared_ptr<LabelExpression> GqlParser::parse_label_factor() {
    if (match(TokenType::BANG) || match(TokenType::NOT)) {
        auto expr = parse_label_factor();
        auto node = std::make_shared<LabelExpression>();
        node->kind = LabelExprKind::NOT;
        node->expr = std::move(expr);
        return node;
    }
    if (match(TokenType::LPAREN)) {
        auto expr = parse_label_expression();
        consume(TokenType::RPAREN, "Expected ')' to close label expression");
        return expr;
    }
    if (check(TokenType::NAME)) {
        auto text = peek().text;
        advance();
        auto node = std::make_shared<LabelExpression>();
        node->kind = LabelExprKind::LITERAL;
        node->name = text;
        return node;
    }
    throw std::runtime_error("Expected label name or expression in node/edge pattern (found: " + peek().text + ")");
}

} // namespace ragedb::gql

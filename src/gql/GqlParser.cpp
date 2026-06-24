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
    int optional_group_counter = 0;
    // Parse MATCH/OPTIONAL MATCH clauses
    while (check(TokenType::MATCH) || (check(TokenType::OPTIONAL) && peek(1).type == TokenType::MATCH)) {
        MatchStatement stmt;
        if (match(TokenType::OPTIONAL)) {
            stmt.is_optional = true;
        }
        consume(TokenType::MATCH, "Expected MATCH");

        // Parse GQL Match Mode
        if (match(TokenType::DIFFERENT_KW)) {
            if (check(TokenType::NAME) && (peek().text == "EDGES" || peek().text == "edges")) {
                advance();
            } else {
                throw std::runtime_error("Expected 'EDGES' after 'DIFFERENT'");
            }
            stmt.match_mode = MatchMode::DIFFERENT_EDGES;
        } else if (match(TokenType::REPEATABLE_KW)) {
            if (check(TokenType::NAME) && (peek().text == "ELEMENTS" || peek().text == "elements")) {
                advance();
            } else {
                throw std::runtime_error("Expected 'ELEMENTS' after 'REPEATABLE'");
            }
            stmt.match_mode = MatchMode::REPEATABLE_ELEMENTS;
        }

        // Parse GQL Path Mode
        if (match(TokenType::TRAIL_KW)) {
            stmt.path_mode = PathMode::TRAIL;
        } else if (match(TokenType::ACYCLIC_KW)) {
            stmt.path_mode = PathMode::ACYCLIC;
        } else if (match(TokenType::SIMPLE_KW)) {
            stmt.path_mode = PathMode::SIMPLE;
        } else if (match(TokenType::WALK_KW)) {
            stmt.path_mode = PathMode::WALK;
        }

        if (match(TokenType::KHOP)) {
            stmt.is_khop = true;
        }
        // Parse optional path variable assignment (e.g. p = ALL SHORTEST ...)
        if (check(TokenType::NAME) && peek(1).type == TokenType::EQ) {
            stmt.path_variable = peek().text;
            advance(); // Consume variable
            advance(); // Consume '='

            // Parse shortest path selectors:
            // 1. ALL SHORTEST paths or ALL CHEAPEST paths
            if (check(TokenType::ALL_KW) && peek(1).type == TokenType::SHORTEST_KW) {
                stmt.shortest_path_kind = ShortestPathKind::ALL;
                advance();
                advance();
            } else if (check(TokenType::ALL_KW) && peek(1).type == TokenType::CHEAPEST_KW) {
                stmt.shortest_path_kind = ShortestPathKind::ALL_CHEAPEST;
                advance();
                advance();
                if (check(TokenType::NAME) && (peek().text == "PATH" || peek().text == "path" || peek().text == "PATHS" || peek().text == "paths")) {
                    advance();
                }
            // 2. ANY SHORTEST paths or ANY CHEAPEST paths
            } else if (check(TokenType::ANY_KW) && peek(1).type == TokenType::SHORTEST_KW) {
                stmt.shortest_path_kind = ShortestPathKind::ANY;
                advance();
                advance();
            } else if (check(TokenType::ANY_KW) && peek(1).type == TokenType::CHEAPEST_KW) {
                stmt.shortest_path_kind = ShortestPathKind::CHEAPEST;
                advance();
                advance();
                if (check(TokenType::NAME) && (peek().text == "PATH" || peek().text == "path")) {
                    advance();
                }
            // 3. SHORTEST k or SHORTEST k GROUP
            } else if (check(TokenType::SHORTEST_KW)) {
                advance(); // consume SHORTEST
                uint64_t k = 1;
                if (check(TokenType::NUMBER)) {
                    k = peek().int_value;
                    advance();
                }
                stmt.shortest_path_k = k;
                if (match(TokenType::GROUP_KW)) {
                    stmt.shortest_path_kind = ShortestPathKind::K_GROUP;
                } else {
                    stmt.shortest_path_kind = ShortestPathKind::K;
                }
            // 4. CHEAPEST [PATH] or CHEAPEST k [PATH[S]]
            } else if (match(TokenType::CHEAPEST_KW)) {
                uint64_t k = 1;
                if (check(TokenType::NUMBER)) {
                    k = peek().int_value;
                    advance();
                    stmt.shortest_path_kind = ShortestPathKind::CHEAPEST_K;
                    stmt.shortest_path_k = k;
                    if (check(TokenType::NAME) && (peek().text == "PATH" || peek().text == "path" || peek().text == "PATHS" || peek().text == "paths")) {
                        advance();
                    }
                } else {
                    stmt.shortest_path_kind = ShortestPathKind::CHEAPEST;
                    if (check(TokenType::NAME) && (peek().text == "PATH" || peek().text == "path")) {
                        advance();
                    }
                }
            }
        }
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
            stmt.id = match_id_counter++;
            query.matches.push_back(std::move(stmt));
        } else {
            int group_id = stmt.is_optional ? optional_group_counter++ : -1;
            do {
                MatchStatement current_stmt = stmt;
                current_stmt.pattern = parse_path_pattern();
                if (current_stmt.pattern.is_questioned) {
                    current_stmt.is_optional = true;
                    current_stmt.optional_group_id = (group_id >= 0) ? group_id : optional_group_counter++;
                } else {
                    current_stmt.optional_group_id = group_id;
                }
                if (current_stmt.shortest_path_kind == ShortestPathKind::CHEAPEST ||
                    current_stmt.shortest_path_kind == ShortestPathKind::ALL_CHEAPEST ||
                    current_stmt.shortest_path_kind == ShortestPathKind::CHEAPEST_K) {
                    // Backwards compatibility for old-style COST clause placed outside the path pattern
                    if (match(TokenType::COST_KW)) {
                        auto expr = parse_expression();
                        if (!current_stmt.pattern.edges.empty()) {
                            current_stmt.pattern.edges[0].cost_expr = std::move(expr);
                        }
                    }
                }
                current_stmt.id = match_id_counter++;
                query.matches.push_back(std::move(current_stmt));
            } while (match(TokenType::COMMA));
        }
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
    // 1. Detect optional EXPLAIN / PROFILE / NO_SEMANTIC prefix at query start
    bool explain = false;
    bool profile = false;
    bool skip_semantic = false;
    while (true) {
        if (match(TokenType::EXPLAIN)) {
            explain = true;
        } else if (match(TokenType::PROFILE)) {
            profile = true;
        } else if (match(TokenType::NO_SEMANTIC)) {
            skip_semantic = true;
        } else {
            break;
        }
    }

    // 2. Parse special CALL CLEAR CACHE utility command
    if (match(TokenType::CALL)) {
        consume(TokenType::CLEAR, "Expected 'CLEAR' after 'CALL'");
        consume(TokenType::CACHE, "Expected 'CACHE' after 'CLEAR'");
        GqlQuery query;
        query.clear_cache = true;
        query.explain = explain;
        query.profile = profile;
        query.skip_semantic = skip_semantic;
        return query;
    }

    // 3. Parse Schema Operations (CREATE/DROP/ALTER/SHOW)
    if (check(TokenType::CREATE) || check(TokenType::DROP) || check(TokenType::ALTER) || check(TokenType::SHOW)) {
        GqlQuery query;
        SchemaOperation schema;
        
        if (match(TokenType::CREATE)) {
            // CREATE VIEW
            if (check(TokenType::NAME) && (peek().text == "view" || peek().text == "VIEW")) {
                advance(); // consume "VIEW"
                std::string view_name = peek().text;
                consume(TokenType::NAME, "Expected view name identifier");
                
                // Consume optional or keyword 'AS'
                if (check(TokenType::NAME) && (peek().text == "AS" || peek().text == "as")) {
                    advance();
                } else {
                    consume(TokenType::AS, "Expected 'AS' keyword");
                }
                
                // Parse the view definition query and reconstruct its string representation from tokens
                std::string view_query_str;
                size_t start_pos = pos;
                GqlQuery view_query = parse_union();
                
                for (size_t i = start_pos; i < pos; ++i) {
                    if (!view_query_str.empty()) view_query_str += " ";
                    view_query_str += tokens[i].text;
                }
                
                schema.op = SchemaOperation::Op::CREATE_VIEW;
                schema.name = view_name;
                schema.query_string = view_query_str;
            // CREATE CONSTRAINT
            } else if (check(TokenType::NAME) && (peek().text == "CONSTRAINT" || peek().text == "constraint")) {
                advance(); // consume "CONSTRAINT"
                std::string constraint_name = peek().text;
                consume(TokenType::NAME, "Expected constraint name identifier");
                
                // Consume optional or keyword 'AS'
                if (check(TokenType::NAME) && (peek().text == "AS" || peek().text == "as")) {
                    advance();
                } else {
                    consume(TokenType::AS, "Expected 'AS' keyword");
                }
                
                // Parse the constraint query and reconstruct its string representation
                std::string constraint_query_str;
                size_t start_pos = pos;
                GqlQuery constraint_query = parse_union();
                
                for (size_t i = start_pos; i < pos; ++i) {
                    if (!constraint_query_str.empty()) constraint_query_str += " ";
                    constraint_query_str += tokens[i].text;
                }
                
                schema.op = SchemaOperation::Op::CREATE_CONSTRAINT;
                schema.name = constraint_name;
                schema.query_string = constraint_query_str;
            // CREATE FULLTEXT INDEX
            } else if (match(TokenType::FULLTEXT)) {
                consume(TokenType::INDEX, "Expected 'INDEX' after 'FULLTEXT'");
                std::string type_name = peek().text;
                consume(TokenType::NAME, "Expected type name identifier");
                consume(TokenType::DOT, "Expected '.'");
                std::string property_name = peek().text;
                consume(TokenType::NAME, "Expected property name identifier");
                schema.op = SchemaOperation::Op::CREATE_FULLTEXT_INDEX;
                schema.name = type_name;
                schema.alter_property_name = property_name;
            // CREATE INDEX (Standard property index)
            } else if (match(TokenType::INDEX)) {
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
                    throw std::runtime_error("Expected 'NODE', 'RELATIONSHIP', 'REL', 'VIEW', 'CONSTRAINT', or 'INDEX' after 'CREATE'");
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
            if (check(TokenType::NAME) && (peek().text == "VIEW" || peek().text == "view")) {
                advance(); // consume "VIEW"
                std::string view_name = peek().text;
                consume(TokenType::NAME, "Expected view name identifier");
                schema.op = SchemaOperation::Op::DROP_VIEW;
                schema.name = view_name;
            } else if (check(TokenType::NAME) && (peek().text == "CONSTRAINT" || peek().text == "constraint")) {
                advance(); // consume "CONSTRAINT"
                std::string constraint_name = peek().text;
                consume(TokenType::NAME, "Expected constraint name identifier");
                schema.op = SchemaOperation::Op::DROP_CONSTRAINT;
                schema.name = constraint_name;
            } else if (match(TokenType::INDEX)) {
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
                    throw std::runtime_error("Expected 'NODE', 'RELATIONSHIP', 'REL', 'VIEW', 'CONSTRAINT', or 'INDEX' after 'DROP'");
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
        query.skip_semantic = skip_semantic;
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
    query.skip_semantic = skip_semantic;

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
    if (match(TokenType::WHERE)) {
        edge.where_expr = parse_expression();
    }
    if (match(TokenType::COST_KW)) {
        edge.cost_expr = parse_expression();
    }
    if (edge.cost_expr && (!edge.properties.empty() || edge.where_expr)) {
        throw std::runtime_error("COST cannot be used with property specification or inline WHERE within the same edge pattern");
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
    bool is_parenthesized = false;
    if (check(TokenType::LPAREN) && peek(1).type == TokenType::LPAREN) {
        consume(TokenType::LPAREN, "Expected '('");
        is_parenthesized = true;
    }

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
            // Parse repetition suffix if present (e.g. -[e]->+ or -[e]->* or -[e]->{1, 5})
            if (match(TokenType::PLUS)) {
                // '+' denotes 1 or more hops
                edge.is_variable_length = true;
                edge.min_hops = 1;
                edge.max_hops = std::numeric_limits<uint64_t>::max();
            } else if (match(TokenType::STAR)) {
                // '*' denotes 0 or more hops
                edge.is_variable_length = true;
                edge.min_hops = 0;
                edge.max_hops = std::numeric_limits<uint64_t>::max();
            } else if (match(TokenType::LBRACE)) {
                edge.is_variable_length = true;
                if (check(TokenType::NUMBER)) {
                    uint64_t val = peek().int_value;
                    consume(TokenType::NUMBER, "Expected number");
                    if (match(TokenType::COMMA)) {
                        edge.min_hops = val;
                        if (check(TokenType::NUMBER)) {
                            edge.max_hops = peek().int_value;
                            consume(TokenType::NUMBER, "Expected number");
                        } else {
                            edge.max_hops = std::numeric_limits<uint64_t>::max();
                        }
                    } else {
                        edge.min_hops = val;
                        edge.max_hops = val;
                    }
                } else if (match(TokenType::COMMA)) {
                    edge.min_hops = 0;
                    edge.max_hops = peek().int_value;
                    consume(TokenType::NUMBER, "Expected maximum hops");
                } else {
                    throw std::runtime_error("Invalid quantifier format inside '{}'");
                }
                consume(TokenType::RBRACE, "Expected '}'");
            }
        } else if (match(TokenType::LT_DASH_LB)) {
            // Incoming Edge with detail: <-[e:L {p:v}]-
            parse_edge_details(edge);
            consume(TokenType::RB_DASH, "Expected ']-' to end incoming relationship description");
            edge.direction = EdgeDirection::LEFT;
            // Parse repetition suffix if present (e.g. <-[e]-+ or <-[e]-* or <-[e]-{1, 5})
            if (match(TokenType::PLUS)) {
                // '+' denotes 1 or more hops
                edge.is_variable_length = true;
                edge.min_hops = 1;
                edge.max_hops = std::numeric_limits<uint64_t>::max();
            } else if (match(TokenType::STAR)) {
                // '*' denotes 0 or more hops
                edge.is_variable_length = true;
                edge.min_hops = 0;
                edge.max_hops = std::numeric_limits<uint64_t>::max();
            } else if (match(TokenType::LBRACE)) {
                edge.is_variable_length = true;
                if (check(TokenType::NUMBER)) {
                    uint64_t val = peek().int_value;
                    consume(TokenType::NUMBER, "Expected number");
                    if (match(TokenType::COMMA)) {
                        edge.min_hops = val;
                        if (check(TokenType::NUMBER)) {
                            edge.max_hops = peek().int_value;
                            consume(TokenType::NUMBER, "Expected number");
                        } else {
                            edge.max_hops = std::numeric_limits<uint64_t>::max();
                        }
                    } else {
                        edge.min_hops = val;
                        edge.max_hops = val;
                    }
                } else if (match(TokenType::COMMA)) {
                    edge.min_hops = 0;
                    edge.max_hops = peek().int_value;
                    consume(TokenType::NUMBER, "Expected maximum hops");
                } else {
                    throw std::runtime_error("Invalid quantifier format inside '{}'");
                }
                consume(TokenType::RBRACE, "Expected '}'");
            }
        }

        pattern.edges.push_back(edge);
        pattern.nodes.push_back(parse_node_pattern());
    }

    if (is_parenthesized) {
        consume(TokenType::RPAREN, "Expected ')'");
        if (match(TokenType::QUESTION)) {
            pattern.is_questioned = true;
        } else if (match(TokenType::LBRACE)) {
            uint64_t min_hops = 1;
            uint64_t max_hops = 1;
            bool is_range = false;

            if (check(TokenType::NUMBER)) {
                uint64_t val = peek().int_value;
                consume(TokenType::NUMBER, "Expected number");
                if (match(TokenType::COMMA)) {
                    min_hops = val;
                    is_range = true;
                    if (check(TokenType::NUMBER)) {
                        max_hops = peek().int_value;
                        consume(TokenType::NUMBER, "Expected number");
                    } else {
                        max_hops = std::numeric_limits<uint64_t>::max();
                    }
                } else {
                    min_hops = val;
                    max_hops = val;
                }
            } else if (match(TokenType::COMMA)) {
                min_hops = 0;
                max_hops = peek().int_value;
                consume(TokenType::NUMBER, "Expected maximum hops");
                is_range = true;
            } else {
                throw std::runtime_error("Invalid quantifier format inside '{}' after parenthesized path group");
            }
            consume(TokenType::RBRACE, "Expected '}'");

            if (pattern.edges.size() == 1) {
                pattern.edges[0].is_variable_length = true;
                pattern.edges[0].min_hops = min_hops;
                pattern.edges[0].max_hops = max_hops;
            } else if (pattern.edges.size() > 1) {
                if (is_range) {
                    throw std::runtime_error("Range repetitions on multi-edge parenthesized path groups are not supported");
                }
                uint64_t k = min_hops;
                if (k == 0) {
                    pattern.edges.clear();
                    pattern.nodes.resize(1);
                } else if (k > 1) {
                    std::vector<PatternNode> base_nodes = pattern.nodes;
                    std::vector<PatternEdge> base_edges = pattern.edges;
                    size_t L = base_edges.size();

                    for (uint64_t iteration = 1; iteration < k; ++iteration) {
                        for (size_t j = 0; j < L; ++j) {
                            PatternEdge new_edge = base_edges[j];
                            if (!new_edge.variable.empty()) {
                                new_edge.variable += "_" + std::to_string(iteration);
                            }
                            pattern.edges.push_back(std::move(new_edge));

                            PatternNode new_node = base_nodes[j + 1];
                            if (!new_node.variable.empty()) {
                                new_node.variable += "_" + std::to_string(iteration);
                            }
                            pattern.nodes.push_back(std::move(new_node));
                        }
                    }
                }
            }
        }
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
    if (match(TokenType::WHERE)) {
        node.where_expr = parse_expression();
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
            bool is_negative = match(TokenType::MINUS);
            if (check(TokenType::NUMBER)) {
                value = is_negative ? -peek().int_value : peek().int_value;
                advance();
            } else if (check(TokenType::FLOAT_LIT)) {
                value = is_negative ? -peek().float_value : peek().float_value;
                advance();
            } else {
                if (is_negative) {
                    throw std::runtime_error("Expected numeric literal after '-' in property map");
                }
                if (match(TokenType::TRUE_KW)) {
                    value = true;
                } else if (match(TokenType::FALSE_KW)) {
                    value = false;
                } else if (match(TokenType::NULL_KW)) {
                    value = std::monostate{};
                } else if (check(TokenType::STRING_LIT)) {
                    value = peek().text;
                    advance();
                } else {
                    throw std::runtime_error("Expected literal value for property map");
                }
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
    while (true) {
        if (match(TokenType::IS)) {
            bool is_not = false;
            if (match(TokenType::NOT)) {
                is_not = true;
            }
            consume(TokenType::NULL_KW, "Expected 'NULL' after 'IS [NOT]'");
            expr = std::make_unique<IsNullExpr>(std::move(expr), is_not);
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
        int sub_match_id = 0;
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
            TokenType type = peek().type;
            if (type == TokenType::NAME || (type >= TokenType::TRUE_KW && type < TokenType::LPAREN)) {
                advance();
            } else {
                throw std::runtime_error("Expected property name after '.' (found: " + prop + ")");
            }
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
    if (match(TokenType::PERCENT)) {
        auto node = std::make_shared<LabelExpression>();
        node->kind = LabelExprKind::WILDCARD;
        return node;
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

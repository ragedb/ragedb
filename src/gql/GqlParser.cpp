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
#include "GqlParserInternal.h"
#include "GqlValue.h"
#include <stdexcept>
#include <algorithm>
#include <cctype>
#include <limits>
#include <memory>
#include <optional>

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

/**
 * @brief Consume the current token as an identifier and return its text. Accepts NAME plus any
 *        keyword token, so reserved words (e.g. `with`) remain usable as aliases and names.
 *
 * @param error_message The descriptive message to include in the thrown exception.
 * @throws std::runtime_error If the current token is not identifier-like.
 */
std::string GqlParser::consume_identifier(const std::string& error_message) {
    TokenType type = peek().type;
    if (type == TokenType::NAME || (type >= TokenType::TRUE_KW && type < TokenType::LPAREN)) {
        std::string text = peek().text;
        advance();
        return text;
    }
    throw std::runtime_error(error_message + " (found: " + peek().text + ")");
}

/**
 * @brief Parse a comma-separated projection list (RETURN or WITH items) into query.returns.
 *        WITH items pass require_alias_for_expressions: a projected column's name becomes the
 *        binding the next segment resolves, so anything other than a plain variable has no usable
 *        name of its own and must be aliased.
 */
void GqlParser::parse_return_items(GqlQuery& query, bool require_alias_for_expressions) {
    do {
        ReturnItem item;
        item.expr = parse_expression();
        if (match(TokenType::AS)) {
            item.alias = consume_identifier("Expected alias name after 'AS'");
        }
        if (require_alias_for_expressions && !item.alias && item.expr->kind != ExpressionKind::VARIABLE) {
            throw std::runtime_error("WITH items other than a plain variable must be aliased with AS");
        }
        query.returns.push_back(std::move(item));
    } while (match(TokenType::COMMA));
}

/**
 * @brief Names every column the working table carries at the end of a segment: the columns piped in
 *        from the previous segment, the variables this segment's patterns bind, and the columns its
 *        FOR and LET statements add.
 *
 * Used to synthesize the passthrough projection that closes a segment whose ordering, paging or value
 * binding has to take effect before the following statement re-projects. Order is significant only for
 * readability of the resulting plan; duplicates are dropped.
 */
static std::vector<std::string> segment_output_columns(const GqlQuery& query, const GqlQuery* piped) {
    std::vector<std::string> columns;
    std::set<std::string> seen;
    auto add = [&](const std::string& name) {
        if (name.empty() || !seen.insert(name).second) return;
        columns.push_back(name);
    };
    if (piped) {
        for (const auto& item : piped->returns) {
            if (item.alias) {
                add(*item.alias);
            } else if (item.expr && item.expr->kind == ExpressionKind::VARIABLE) {
                add(static_cast<VariableExpr*>(item.expr.get())->name);
            }
        }
    }
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) add(node.variable);
        for (const auto& edge : match.pattern.edges) add(edge.variable);
        add(match.path_variable);
    }
    for (const auto& binding : query.for_bindings) add(binding.variable);
    for (const auto& let : query.let_bindings) {
        if (let.alias) add(*let.alias);
    }
    return columns;
}

/**
 * @brief Parse an optional ISO GQL `GROUP BY <var> (, <var>)*` clause into query.group_by. GROUP is a
 *        keyword but the following BY is not, so it arrives as an identifier. Only fires when both are
 *        present, so a bare GROUP elsewhere (e.g. SHORTEST k GROUP) is untouched.
 */
void GqlParser::parse_group_by(GqlQuery& query) {
    if (!check(TokenType::GROUP_KW)) return;
    Token by = peek(1);
    std::string by_up = by.text;
    std::transform(by_up.begin(), by_up.end(), by_up.begin(), [](unsigned char c){ return std::toupper(c); });
    if (by.type != TokenType::NAME || by_up != "BY") return;
    advance(); // GROUP
    advance(); // BY
    do {
        if (!check(TokenType::NAME)) {
            throw std::runtime_error("Expected a grouping variable after GROUP BY");
        }
        std::string var = peek().text;
        advance();
        query.group_by.push_back(std::make_unique<VariableExpr>(var));
    } while (match(TokenType::COMMA));
}

/**
 * @brief Parse an optional ORDER BY clause (with per-key ASC/DESC) into query.order_by.
 */
void GqlParser::parse_order_by(GqlQuery& query) {
    if (!match(TokenType::ORDER_BY)) return;
    do {
        SortSpec spec;
        spec.expr = parse_expression();
        if (match(TokenType::ASC)) {
            spec.ascending = true;
        } else if (match(TokenType::DESC)) {
            spec.ascending = false;
        } else {
            spec.ascending = true;
        }
        query.order_by.push_back(std::move(spec));
    } while (match(TokenType::COMMA));
}

/**
 * @brief Parse the optional OFFSET and LIMIT page clauses into query.offset / query.limit. Either order is
 *        accepted, so both `OFFSET 10 LIMIT 5` and `LIMIT 5 OFFSET 10` parse.
 */
void GqlParser::parse_limit(GqlQuery& query) {
    for (;;) {
        if (match(TokenType::LIMIT)) {
            std::string num_str = peek().text;
            consume(TokenType::NUMBER, "Expected integer limit value");
            query.limit = std::stoull(num_str);
        } else if (match(TokenType::OFFSET)) {
            std::string num_str = peek().text;
            consume(TokenType::NUMBER, "Expected integer offset value");
            query.offset = std::stoull(num_str);
        } else {
            return;
        }
    }
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
    std::vector<std::shared_ptr<GqlQuery>> with_segments;
    std::unique_ptr<Expression> pending_having;
    int match_id_counter = 0;
    int optional_group_counter = 0;

    while (true) {
    GqlQuery query;
    // A WHERE that followed a preceding WITH filters the rows piped into this segment.
    if (pending_having) {
        query.where_expr = std::move(pending_having);
    }

    // ISO GQL scoped subquery: `CALL (v1, v2, ...) { <subquery incl UNION ALL> }`. This sources the
    // segment: the named outer variables are imported into a nested subquery, which is run and whose
    // result rows are piped into this segment's projection (in place of a MATCH). The '(' immediately
    // after CALL distinguishes it from a procedure call (CALL fts.search / CALL algo.propagate).
    if (check(TokenType::CALL) && peek(1).type == TokenType::LPAREN) {
        advance(); // CALL
        consume(TokenType::LPAREN, "Expected '(' after CALL");
        std::vector<std::string> imports;
        if (!check(TokenType::RPAREN)) {
            do {
                imports.push_back(peek().text);
                consume(TokenType::NAME, "Expected a variable name in the CALL import list");
            } while (match(TokenType::COMMA));
        }
        consume(TokenType::RPAREN, "Expected ')' after the CALL import list");
        consume(TokenType::LBRACE, "Expected '{' to open the CALL subquery");
        GqlQuery sub = parse_union();
        consume(TokenType::RBRACE, "Expected '}' to close the CALL subquery");
        query.call_import_vars = std::move(imports);
        query.call_subquery = std::make_shared<GqlQuery>(std::move(sub));
    }

    // Parse MATCH/OPTIONAL MATCH clauses. A WHERE may sit between MATCH groups
    // (MATCH ... WHERE ... MATCH ... RETURN): after each group we take an optional WHERE and loop
    // back so a following MATCH keeps extending the same segment.
    while (true) {
    while (check(TokenType::MATCH) || (check(TokenType::OPTIONAL) && peek(1).type == TokenType::MATCH) ||
           check(TokenType::CALL)) {
        // CALL fts.search('Type', 'prop', 'query') YIELD node [AS v] [, score [AS s]] -- a full-text search
        // query prefix. Translate to the same search MatchStatement the `<v> IN SEARCH (...)` syntax builds
        // (shared executor path); a follow-on MATCH/RETURN then references the yielded variable.
        if (check(TokenType::CALL)) {
            advance(); // CALL
            // Procedure name namespace.name. `search` lexes as the SEARCH keyword (like the IN SEARCH form),
            // so accept SEARCH or NAME for each part, and compare case-insensitively.
            auto lower = [](std::string s) {
                std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                return s;
            };
            std::string ns = lower(peek().text);
            if (!check(TokenType::NAME) && !check(TokenType::SEARCH))
                throw std::runtime_error("Expected a procedure namespace after CALL");
            advance();
            consume(TokenType::DOT, "Expected '.' in procedure name");
            std::string proc = lower(peek().text);
            if (!check(TokenType::NAME) && !check(TokenType::SEARCH))
                throw std::runtime_error("Expected a procedure name after '.'");
            advance();
            // CALL algo.propagate(seeds, values, relTypes, direction, maxDepth, valueProp, order, cap)
            //   YIELD node [AS a], value [AS b], depth [AS c]
            // A value-propagating first-claim BFS. The arguments are correlated expressions (the seed
            // list and value list are bound upstream), so parse each as a full expression; the executor
            // evaluates them per incoming row. The three output columns are fixed (node, value, depth).
            if (ns == "algo" && proc == "propagate") {
                MatchStatement prop_stmt;
                prop_stmt.is_propagate = true;
                consume(TokenType::LPAREN, "Expected '(' after algo.propagate");
                while (!check(TokenType::RPAREN)) {
                    prop_stmt.propagate_args.push_back(std::shared_ptr<Expression>(parse_expression().release()));
                    if (!match(TokenType::COMMA)) break;
                }
                consume(TokenType::RPAREN, "Expected ')' after algo.propagate arguments");
                consume(TokenType::YIELD, "Expected 'YIELD' after algo.propagate(...)");
                // Fixed output columns node/value/depth, order-independent, each optionally aliased with AS.
                // 'node' lexes as the NODE keyword; 'value' and 'depth' lex as NAME.
                bool got_col = false;
                while (true) {
                    std::string col;
                    if (check(TokenType::NODE)) { col = "node"; advance(); }
                    else if (check(TokenType::NAME)) { col = peek().text; advance(); }
                    else break;
                    std::string lcol = lower(col);
                    std::string bindv = col;
                    if (match(TokenType::AS)) {
                        bindv = peek().text;
                        consume(TokenType::NAME, "Expected alias after AS");
                    }
                    if (lcol == "node") prop_stmt.yield_var = bindv;
                    else if (lcol == "value") prop_stmt.yield_score_var = bindv;
                    else if (lcol == "depth") prop_stmt.yield_depth_var = bindv;
                    else throw std::runtime_error("Unknown algo.propagate YIELD column: " + col);
                    got_col = true;
                    if (!match(TokenType::COMMA)) break;
                }
                if (!got_col)
                    throw std::runtime_error("Expected YIELD columns after algo.propagate(...)");
                prop_stmt.id = match_id_counter++;
                query.matches.push_back(std::move(prop_stmt));
                continue;
            }
            if (ns != "fts" || proc != "search") {
                throw std::runtime_error("Unsupported CALL procedure: " + ns + "." + proc +
                                         " (only fts.search or algo.propagate are supported)");
            }
            MatchStatement search_stmt;
            search_stmt.is_search = true;
            consume(TokenType::LPAREN, "Expected '(' after fts.search");
            search_stmt.search_type = peek().text;
            consume(TokenType::STRING_LIT, "Expected node type name string in fts.search");
            consume(TokenType::COMMA, "Expected ',' after fts.search type");
            search_stmt.search_properties.push_back(peek().text);
            consume(TokenType::STRING_LIT, "Expected property name string in fts.search");
            consume(TokenType::COMMA, "Expected ',' after fts.search property");
            search_stmt.search_string = peek().text;
            consume(TokenType::STRING_LIT, "Expected query string in fts.search");
            consume(TokenType::RPAREN, "Expected ')' after fts.search arguments");
            consume(TokenType::YIELD, "Expected 'YIELD' after fts.search(...)");
            // YIELD node [AS v] [, score [AS s]] -- the node column binds to v (or itself); score is
            // optional. `node` lexes as the NODE keyword, `score` as a NAME, so accept either token here.
            std::string ycol = peek().text;
            if (!check(TokenType::NODE) && !check(TokenType::NAME))
                throw std::runtime_error("Expected a yield column after YIELD");
            advance();
            if (match(TokenType::AS)) {
                search_stmt.yield_var = peek().text;
                consume(TokenType::NAME, "Expected alias after AS");
            } else {
                search_stmt.yield_var = ycol;
            }
            if (match(TokenType::COMMA)) {
                std::string scol = peek().text;
                if (!check(TokenType::NODE) && !check(TokenType::NAME))
                    throw std::runtime_error("Expected a second yield column");
                advance();
                if (match(TokenType::AS)) {
                    search_stmt.yield_score_var = peek().text;
                    consume(TokenType::NAME, "Expected alias after AS");
                } else {
                    search_stmt.yield_score_var = scol;
                }
            }
            search_stmt.id = match_id_counter++;
            query.matches.push_back(std::move(search_stmt));
            continue;
        }
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

            // GQL also allows the path mode after the assignment (e.g. p = TRAIL (...)), not only
            // before it (TRAIL p = ...). The executor honors either position.
            if (match(TokenType::TRAIL_KW)) {
                stmt.path_mode = PathMode::TRAIL;
            } else if (match(TokenType::ACYCLIC_KW)) {
                stmt.path_mode = PathMode::ACYCLIC;
            } else if (match(TokenType::SIMPLE_KW)) {
                stmt.path_mode = PathMode::SIMPLE;
            } else if (match(TokenType::WALK_KW)) {
                stmt.path_mode = PathMode::WALK;
            }

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
                    k = static_cast<uint64_t>(peek().int_value);
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
                    k = static_cast<uint64_t>(peek().int_value);
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
                    current_stmt.shortest_path_kind == ShortestPathKind::CHEAPEST_K ||
                    current_stmt.shortest_path_kind == ShortestPathKind::ANY) {
                    // A COST clause after the pattern (LDBC form: ANY SHORTEST (a)-[r]-{1,}(b) COST r.w)
                    // selects the cheapest-by-weight path; the executor routes ANY+cost through the
                    // weighted search. Also the legacy CHEAPEST-family placement.
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

    // A WHERE after a MATCH group filters those rows; the ISO GQL FILTER statement is its synonym on
    // the working table. AND-combine either with any HAVING carried in from a prior WITH/NEXT and with
    // earlier interleaved predicates in this segment.
    if (match(TokenType::WHERE) || match(TokenType::FILTER_KW)) {
        auto w = parse_expression();
        if (query.where_expr) {
            query.where_expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::AND, std::move(query.where_expr), std::move(w));
        } else {
            query.where_expr = std::move(w);
        }
    } else if (match(TokenType::LET_KW)) {
        // ISO GQL LET adds computed bindings to the working table, usable by the rest of the segment.
        do {
            ReturnItem item;
            item.alias = consume_identifier("Expected variable name after 'LET'");
            consume(TokenType::EQ, "Expected '=' after LET variable");
            item.expr = parse_expression();
            query.let_bindings.push_back(std::move(item));
        } while (match(TokenType::COMMA));
    } else if (match(TokenType::FOR)) {
        // ISO GQL FOR expands a list into one row per element (the standard's UNWIND). Unlike LET, which
        // adds a column, FOR multiplies the working table's rows.
        do {
            ForBinding binding;
            binding.variable = consume_identifier("Expected variable name after 'FOR'");
            consume(TokenType::IN_KW, "Expected 'IN' after FOR variable");
            binding.list_expr = parse_expression();
            query.for_bindings.push_back(std::move(binding));
        } while (match(TokenType::COMMA));
    }
    // A segment is a free sequence of simple statements (MATCH / FILTER / LET / FOR); keep extending it
    // while more of them follow. MATCH ... WHERE ... MATCH ... and FILTER ... MATCH ... both round-trip here.
    if (check(TokenType::MATCH) || (check(TokenType::OPTIONAL) && peek(1).type == TokenType::MATCH) ||
        check(TokenType::WHERE) || check(TokenType::FILTER_KW) || check(TokenType::LET_KW) ||
        check(TokenType::FOR)) {
        continue;
    }
    break;
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

    // Verify we have at least one MATCH or write operation (a continuation segment after WITH may
    // have neither--it operates on the rows piped in from the previous segment). A FOR statement is
    // also a row source in its own right: `FOR x IN [1, 2, 3] RETURN x` matches nothing but still
    // produces rows. A scoped subquery `CALL (imports) { ... }` is likewise a row source for the segment.
    if (query.matches.empty() && query.writes.empty() && with_segments.empty() &&
        query.for_bindings.empty() && !query.call_subquery) {
        throw std::runtime_error("Query must contain at least one MATCH or write clause");
    }

    // ISO GQL standalone ordering/paging statement: `<order by and page statement>` is a primitive query
    // statement, so ORDER BY [OFFSET] [LIMIT] may stand on its own before the RETURN, sorting and paging
    // the working table that the RETURN then re-projects. The table it sorts may come from a preceding
    // NEXT segment, from this segment's own MATCH/FOR, or from its LET bindings.
    if (check(TokenType::ORDER_BY) || check(TokenType::LIMIT) || check(TokenType::OFFSET)) {
        const bool sorts_piped_rows_only = query.matches.empty() && query.let_bindings.empty() &&
                                           query.for_bindings.empty() && !query.call_subquery &&
                                           !with_segments.empty();
        parse_order_by(query);
        parse_limit(query);
        // Sorting rows this segment produced itself, or paging rows a following statement re-projects,
        // is a segment boundary: the ordering has to take effect before the next statement runs. Close
        // the segment with a passthrough projection of everything in scope so the projection pipeline
        // carries every binding forward, and let the RETURN (or MATCH) start a fresh segment. Doing this
        // unconditionally also keeps `ORDER BY ... LIMIT n RETURN count(*)` honest: the page bounds the
        // rows fed to the aggregate rather than the single row it produces.
        const bool match_follows =
            check(TokenType::MATCH) || (check(TokenType::OPTIONAL) && peek(1).type == TokenType::MATCH);
        auto close_segment_with_passthrough = [&]() {
            const GqlQuery* piped = with_segments.empty() ? nullptr : with_segments.back().get();
            for (const auto& col : segment_output_columns(query, piped)) {
                ReturnItem pass;
                pass.expr = std::make_unique<VariableExpr>(col);
                pass.alias = col;
                query.returns.push_back(std::move(pass));
            }
            resolve_order_by_aliases(query);
            with_segments.push_back(std::make_shared<GqlQuery>(std::move(query)));
        };
        if (!sorts_piped_rows_only || match_follows) {
            close_segment_with_passthrough();
            continue;
        }
        // The segment only re-projects rows piped in from the previous one, so push the ORDER BY/LIMIT
        // into that producing segment instead: its streaming top-K then bounds the sort during the
        // traversal rather than materialising the whole intermediate to sort it here. Valid because the
        // order keys are the producer's own output columns; only push when the producer has no
        // ordering/paging of its own to clobber, otherwise close the segment as above.
        auto& producer = with_segments.back();
        if (producer->order_by.empty() && !producer->limit.has_value()) {
            producer->order_by = std::move(query.order_by);
            producer->limit = query.limit;
            query.order_by.clear();
            query.limit.reset();
            resolve_order_by_aliases(*producer);
        } else {
            close_segment_with_passthrough();
            continue;
        }
    }

    // openCypher WITH is not ISO GQL. RageDB is a pure GQL dialect: reject WITH with guidance toward
    // the linear-query equivalents (RETURN ... NEXT for a projection boundary, LET for a value binding,
    // FILTER for a post-projection predicate).
    if (check(TokenType::WITH)) {
        throw std::runtime_error(
            "WITH is not GQL: use 'RETURN ... NEXT' for a projection boundary, 'LET x = ...' for a "
            "value binding, or 'FILTER ...' for a post-projection predicate");
    }

    // Parse RETURN clause (optional if we performed writes, otherwise mandatory)
    if (match(TokenType::RETURN)) {
        if (match(TokenType::DISTINCT)) {
            query.distinct = true;
        }
        parse_return_items(query, false);
        parse_group_by(query);

        // ISO GQL: `RETURN ... [ORDER BY ...] [LIMIT ...] NEXT` is an intermediate projection that
        // feeds the next linear-query statement -- the same pipeline boundary as openCypher WITH.
        // Detect it with a speculative scan past the optional result clauses so a final RETURN leaves
        // any trailing ORDER BY / LIMIT for the (union-aware) top-level parser.
        size_t saved_pos = pos;
        {
            GqlQuery scratch;
            parse_order_by(scratch);
            parse_limit(scratch);
        }
        bool intermediate = check(TokenType::NEXT_KW);
        pos = saved_pos;
        if (intermediate) {
            // A RETURN feeding NEXT projects columns that bind forward, so a non-variable projection
            // needs an alias to name its forwarded column (as the openCypher WITH form required).
            for (const auto& item : query.returns) {
                if (!item.alias && item.expr && item.expr->kind != ExpressionKind::VARIABLE) {
                    throw std::runtime_error("RETURN items before NEXT other than a plain variable must be aliased with AS");
                }
            }
            parse_order_by(query);
            resolve_order_by_aliases(query);
            parse_limit(query);
            consume(TokenType::NEXT_KW, "Expected 'NEXT'");
            // A FILTER/WHERE right after NEXT is a HAVING predicate on the projected rows.
            if (match(TokenType::FILTER_KW) || match(TokenType::WHERE)) {
                pending_having = parse_expression();
            }
            with_segments.push_back(std::make_shared<GqlQuery>(std::move(query)));
            continue;
        }
    } else if (query.writes.empty()) {
        // ISO GQL: a linear query statement composed with NEXT need not end in a result statement --
        // a segment made only of primitive query statements (LET / FILTER / MATCH / FOR) hands its
        // working table to the statement after the NEXT. Close it with a passthrough projection of
        // everything in scope so the projection pipeline carries the bindings, including the columns
        // the LETs just added, forward. Without a NEXT there is nothing to hand the rows to, so an
        // unterminated query is still an error.
        if (!check(TokenType::NEXT_KW)) {
            throw std::runtime_error("Query must contain either a RETURN clause or at least one write clause");
        }
        const GqlQuery* piped = with_segments.empty() ? nullptr : with_segments.back().get();
        for (const auto& col : segment_output_columns(query, piped)) {
            ReturnItem pass;
            pass.expr = std::make_unique<VariableExpr>(col);
            pass.alias = col;
            query.returns.push_back(std::move(pass));
        }
        if (query.returns.empty()) {
            throw std::runtime_error("Query must contain either a RETURN clause or at least one write clause");
        }
        consume(TokenType::NEXT_KW, "Expected 'NEXT'");
        // A FILTER/WHERE right after NEXT is a HAVING predicate on the forwarded rows.
        if (match(TokenType::FILTER_KW) || match(TokenType::WHERE)) {
            pending_having = parse_expression();
        }
        with_segments.push_back(std::make_shared<GqlQuery>(std::move(query)));
        continue;
    }

    query.with_segments = std::move(with_segments);
    return query;
    }  // end while(true) over pipeline segments
}

std::shared_ptr<LabelExpression> GqlParser::parse_label_expression(bool allow_keyword_ops) {
    return parse_label_or(allow_keyword_ops);
}

std::shared_ptr<LabelExpression> GqlParser::parse_label_or(bool allow_keyword_ops) {
    auto left = parse_label_and(allow_keyword_ops);
    while (check(TokenType::PIPE) || (allow_keyword_ops && check(TokenType::OR))) {
        advance(); // consume '|' or 'OR'
        auto right = parse_label_and(allow_keyword_ops);
        auto node = std::make_shared<LabelExpression>();
        node->kind = LabelExprKind::OR;
        node->left = std::move(left);
        node->right = std::move(right);
        left = std::move(node);
    }
    return left;
}

std::shared_ptr<LabelExpression> GqlParser::parse_label_and(bool allow_keyword_ops) {
    auto left = parse_label_factor(allow_keyword_ops);
    while (check(TokenType::AMP) || (allow_keyword_ops && check(TokenType::AND))) {
        advance(); // consume '&' or 'AND'
        auto right = parse_label_factor(allow_keyword_ops);
        auto node = std::make_shared<LabelExpression>();
        node->kind = LabelExprKind::AND;
        node->left = std::move(left);
        node->right = std::move(right);
        left = std::move(node);
    }
    return left;
}

std::shared_ptr<LabelExpression> GqlParser::parse_label_factor(bool allow_keyword_ops) {
    if (match(TokenType::BANG) || (allow_keyword_ops && match(TokenType::NOT))) {
        auto expr = parse_label_factor(allow_keyword_ops);
        auto node = std::make_shared<LabelExpression>();
        node->kind = LabelExprKind::NOT;
        node->expr = std::move(expr);
        return node;
    }
    if (match(TokenType::LPAREN)) {
        // Parenthesised: a ')' delimits it, so the keyword spellings are unambiguous again inside.
        auto expr = parse_label_expression(true);
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

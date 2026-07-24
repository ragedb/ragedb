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

// Top-level query statements: the schema/utility command forms and the set-operation precedence chain
// (parse_query -> parse_union -> parse_intersect -> parse_single_query), the public parse() entry, and the
// ORDER BY alias-scope helper they share with parse_single_query.

#include "GqlParser.h"
#include "GqlParserInternal.h"
#include "GqlValue.h"
#include "GqlLexer.h"
#include <stdexcept>
#include <algorithm>
#include <cctype>
#include <limits>
#include <memory>
#include <optional>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace ragedb::gql {

/**
 * @brief Replace references to the projection's output aliases inside an ORDER BY sort key with
 *        clones of the aliased expressions. ORDER BY is evaluated after the projection, so its
 *        aliases are in scope for the sort keys (as in Cypher and SQL); the executor evaluates
 *        sort keys against pre-projection rows, so the substitution reconstructs that scope.
 *        Aggregation arguments are not descended into (an alias cannot appear there).
 */
static std::unique_ptr<Expression> substitute_return_aliases(
        std::unique_ptr<Expression> expr, const std::map<std::string, const Expression*>& aliases) {
    if (!expr) return expr;
    switch (expr->kind) {
        case ExpressionKind::VARIABLE: {
            auto it = aliases.find(static_cast<const VariableExpr*>(expr.get())->name);
            if (it != aliases.end()) {
                return it->second->clone();
            }
            return expr;
        }
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<UnaryOpExpr*>(expr.get());
            un->expr = substitute_return_aliases(std::move(un->expr), aliases);
            return expr;
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<BinaryOpExpr*>(expr.get());
            bin->left = substitute_return_aliases(std::move(bin->left), aliases);
            bin->right = substitute_return_aliases(std::move(bin->right), aliases);
            return expr;
        }
        case ExpressionKind::FUNCTION_CALL: {
            auto* fc = static_cast<FunctionCallExpr*>(expr.get());
            for (auto& a : fc->args) {
                a = substitute_return_aliases(std::move(a), aliases);
            }
            return expr;
        }
        case ExpressionKind::CASE_WHEN: {
            auto* ce = static_cast<CaseExpr*>(expr.get());
            for (auto& b : ce->branches) {
                b.first = substitute_return_aliases(std::move(b.first), aliases);
                b.second = substitute_return_aliases(std::move(b.second), aliases);
            }
            if (ce->else_expr) {
                ce->else_expr = substitute_return_aliases(std::move(ce->else_expr), aliases);
            }
            return expr;
        }
        case ExpressionKind::IN_LIST: {
            auto* in = static_cast<InExpr*>(expr.get());
            in->value = substitute_return_aliases(std::move(in->value), aliases);
            in->list = substitute_return_aliases(std::move(in->list), aliases);
            return expr;
        }
        case ExpressionKind::CAST: {
            auto* c = static_cast<CastExpr*>(expr.get());
            c->value = substitute_return_aliases(std::move(c->value), aliases);
            return expr;
        }
        case ExpressionKind::IS_LABELED: {
            auto* l = static_cast<IsLabeledExpr*>(expr.get());
            l->value = substitute_return_aliases(std::move(l->value), aliases);
            return expr;
        }
        case ExpressionKind::IS_NULL_CHECK: {
            auto* n = static_cast<IsNullExpr*>(expr.get());
            n->expr = substitute_return_aliases(std::move(n->expr), aliases);
            return expr;
        }
        case ExpressionKind::IS_DIRECTED: {
            auto* d = static_cast<IsDirectedExpr*>(expr.get());
            d->value = substitute_return_aliases(std::move(d->value), aliases);
            return expr;
        }
        case ExpressionKind::IS_SOURCE_DEST: {
            auto* s = static_cast<IsSourceDestExpr*>(expr.get());
            s->value = substitute_return_aliases(std::move(s->value), aliases);
            s->edge = substitute_return_aliases(std::move(s->edge), aliases);
            return expr;
        }
        case ExpressionKind::LIST_LITERAL: {
            auto* le = static_cast<ListExpr*>(expr.get());
            for (auto& element : le->elements) {
                element = substitute_return_aliases(std::move(element), aliases);
            }
            return expr;
        }
        case ExpressionKind::LIST_INDEX: {
            auto* ie = static_cast<IndexExpr*>(expr.get());
            ie->list = substitute_return_aliases(std::move(ie->list), aliases);
            ie->index = substitute_return_aliases(std::move(ie->index), aliases);
            return expr;
        }
        case ExpressionKind::LIST_COMPREHENSION: {
            auto* lc = static_cast<ListComprehensionExpr*>(expr.get());
            lc->list = substitute_return_aliases(std::move(lc->list), aliases);
            if (lc->filter) lc->filter = substitute_return_aliases(std::move(lc->filter), aliases);
            if (lc->projection) lc->projection = substitute_return_aliases(std::move(lc->projection), aliases);
            return expr;
        }
        case ExpressionKind::QUANTIFIED_PREDICATE: {
            auto* qp = static_cast<QuantifiedPredicateExpr*>(expr.get());
            qp->list = substitute_return_aliases(std::move(qp->list), aliases);
            if (qp->predicate) qp->predicate = substitute_return_aliases(std::move(qp->predicate), aliases);
            return expr;
        }
        case ExpressionKind::TEMPORAL_FIELD: {
            auto* tf = static_cast<TemporalFieldExpr*>(expr.get());
            tf->value = substitute_return_aliases(std::move(tf->value), aliases);
            return expr;
        }
        default:
            return expr;
    }
}

void resolve_order_by_aliases(GqlQuery& query) {
    if (query.order_by.empty() || query.returns.empty()) return;
    std::map<std::string, const Expression*> aliases;
    for (const auto& item : query.returns) {
        if (item.alias && item.expr) {
            aliases[*item.alias] = item.expr.get();
        }
    }
    if (aliases.empty()) return;
    for (auto& spec : query.order_by) {
        spec.expr = substitute_return_aliases(std::move(spec.expr), aliases);
    }
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

    // 2. Parse special CALL CLEAR CACHE utility command. Other CALL forms (CALL fts.search(...)) are query
    // prefixes handled in parse_single_query's statement loop, so only intercept CLEAR CACHE here.
    if (check(TokenType::CALL) && peek(1).type == TokenType::CLEAR) {
        advance(); // consume CALL
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

    // Parse optional top-level ORDER BY / LIMIT clauses
    parse_order_by(query);
    if (query.kind == QueryKind::SINGLE) {
        resolve_order_by_aliases(query);
    }
    parse_limit(query);

    query.explain = explain;
    query.profile = profile;
    query.skip_semantic = skip_semantic;

    consume(TokenType::EOF_TOK, "Expected end of query");
    return query;
}

GqlQuery GqlParser::parse_union() {
    // UNION and EXCEPT share precedence (both bind looser than INTERSECT), so a single left-to-right loop
    // handles them. Each accepts the ISO GQL set quantifier ALL | DISTINCT; DISTINCT is the default, so a
    // bare UNION and UNION DISTINCT mean the same thing.
    GqlQuery query = parse_intersect();
    while (check(TokenType::UNION) || check(TokenType::EXCEPT)) {
        const bool is_except = check(TokenType::EXCEPT);
        advance();
        bool all = false;
        if (match(TokenType::ALL_KW)) {
            all = true;
        } else {
            match(TokenType::DISTINCT);   // optional explicit DISTINCT, the default
        }
        GqlQuery right = parse_intersect();

        GqlQuery combined;
        if (is_except) {
            combined.kind = all ? QueryKind::EXCEPT_ALL : QueryKind::EXCEPT;
        } else {
            combined.kind = all ? QueryKind::UNION_ALL : QueryKind::UNION;
        }
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
        } else {
            match(TokenType::DISTINCT);   // optional explicit DISTINCT, the default
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

}  // namespace ragedb::gql

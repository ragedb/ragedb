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

// Graph pattern syntax: path patterns and the node, edge and property-map elements they are built
// from, including the quantifier suffixes and the parenthesized path group.

#include "GqlParser.h"
#include "GqlValue.h"
#include <stdexcept>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace ragedb::gql {

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
            uint64_t val = static_cast<uint64_t>(peek().int_value);
            advance();
            if (match(TokenType::DOT)) {
                consume(TokenType::DOT, "Expected '..' for repetition range");
                edge.min_hops = val;
                if (check(TokenType::NUMBER)) {
                    edge.max_hops = static_cast<uint64_t>(peek().int_value);
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
                edge.max_hops = static_cast<uint64_t>(peek().int_value);
                advance();
            }
        }
    }
    if (check(TokenType::LBRACE)) {
        edge.properties = parse_properties(&edge.property_exprs);
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
                    uint64_t val = static_cast<uint64_t>(peek().int_value);
                    consume(TokenType::NUMBER, "Expected number");
                    if (match(TokenType::COMMA)) {
                        edge.min_hops = val;
                        if (check(TokenType::NUMBER)) {
                            edge.max_hops = static_cast<uint64_t>(peek().int_value);
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
                    edge.max_hops = static_cast<uint64_t>(peek().int_value);
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
                    uint64_t val = static_cast<uint64_t>(peek().int_value);
                    consume(TokenType::NUMBER, "Expected number");
                    if (match(TokenType::COMMA)) {
                        edge.min_hops = val;
                        if (check(TokenType::NUMBER)) {
                            edge.max_hops = static_cast<uint64_t>(peek().int_value);
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
                    edge.max_hops = static_cast<uint64_t>(peek().int_value);
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
                uint64_t val = static_cast<uint64_t>(peek().int_value);
                consume(TokenType::NUMBER, "Expected number");
                if (match(TokenType::COMMA)) {
                    min_hops = val;
                    is_range = true;
                    if (check(TokenType::NUMBER)) {
                        max_hops = static_cast<uint64_t>(peek().int_value);
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
                max_hops = static_cast<uint64_t>(peek().int_value);
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
        node.properties = parse_properties(&node.property_exprs);
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
std::map<std::string, property_type_t> GqlParser::parse_properties(
        std::map<std::string, std::shared_ptr<Expression>>* property_exprs) {
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
            } else if (!is_negative && match(TokenType::TRUE_KW)) {
                value = true;
            } else if (!is_negative && match(TokenType::FALSE_KW)) {
                value = false;
            } else if (!is_negative && match(TokenType::NULL_KW)) {
                value = std::monostate{};
            } else if (!is_negative && check(TokenType::STRING_LIT)) {
                value = peek().text;
                advance();
            } else if (property_exprs) {
                // A property map value is a general value expression, not only a literal: a variable
                // bound by an earlier segment, a LET binding or any computed expression is legal here
                // and is resolved against the current row before the node is looked up. Step back over
                // a consumed '-' so the expression parser sees the whole term.
                if (is_negative) {
                    --pos;
                }
                (*property_exprs)[key] = parse_expression();
                continue;
            } else {
                throw std::runtime_error("Expected literal value for property map");
            }

            props[key] = value;
        } while (match(TokenType::COMMA));
    }
    consume(TokenType::RBRACE, "Expected '}' to end property map");
    return props;
}

} // namespace ragedb::gql

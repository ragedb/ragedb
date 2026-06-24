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

#include "DomainConstraintReasoner.h"
#include "OptimizerUtils.h"
#include "../GqlVirtualCatalog.h"
#include "../GqlParser.h"
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <cmath>
#include <algorithm>
#include <iostream>

namespace ragedb::gql {

namespace {

enum class LogicKind {
    AND,
    OR,
    NOT,
    LEAF
};

struct Predicate {
    std::string variable;
    std::string property;
    BinaryOpKind op; // EQ, LT, LE (canonicalized)
    property_type_t value;
};

struct LogicNode {
    LogicKind kind;
    int literal_id = 0; // Positive for positive leaf literal, negative not used here (NOT node instead)
    std::vector<std::shared_ptr<LogicNode>> children;
};

struct PropertyState {
    Interval interval;
    bool has_allowed_string = false;
    std::string allowed_string;
    std::set<std::string> excluded_strings;
    bool has_bool = false;
    bool bool_val = false;
};

struct Clause {
    std::vector<int> lits;
};

class CNFBuilder {
public:
    int next_var = 1;
    std::vector<Clause> clauses;

    int convert(const std::shared_ptr<LogicNode>& node) {
        if (!node) return 0;
        if (node->kind == LogicKind::LEAF) {
            return node->literal_id;
        }
        int var = next_var++;
        if (node->kind == LogicKind::NOT) {
            int child_var = convert(node->children[0]);
            if (child_var != 0) {
                clauses.push_back(Clause{{var, child_var}});
                clauses.push_back(Clause{{-var, -child_var}});
            }
        } else if (node->kind == LogicKind::AND) {
            std::vector<int> child_vars;
            for (const auto& child : node->children) {
                int cv = convert(child);
                if (cv != 0) child_vars.push_back(cv);
            }
            if (!child_vars.empty()) {
                std::vector<int> big_clause;
                big_clause.push_back(var);
                for (int cv : child_vars) {
                    clauses.push_back(Clause{{-var, cv}});
                    big_clause.push_back(-cv);
                }
                clauses.push_back(Clause{big_clause});
            }
        } else if (node->kind == LogicKind::OR) {
            std::vector<int> child_vars;
            for (const auto& child : node->children) {
                int cv = convert(child);
                if (cv != 0) child_vars.push_back(cv);
            }
            if (!child_vars.empty()) {
                std::vector<int> big_clause;
                big_clause.push_back(-var);
                for (int cv : child_vars) {
                    clauses.push_back(Clause{{var, -cv}});
                    big_clause.push_back(cv);
                }
                clauses.push_back(Clause{big_clause});
            }
        }
        return var;
    }
};

int get_or_create_predicate(const std::string& variable, const std::string& property, BinaryOpKind op, const property_type_t& value, std::vector<Predicate>& predicates) {
    for (size_t i = 0; i < predicates.size(); ++i) {
        if (predicates[i].variable == variable &&
            predicates[i].property == property &&
            predicates[i].op == op &&
            predicates[i].value == value) {
            return static_cast<int>(i + 1);
        }
    }
    predicates.push_back({variable, property, op, value});
    return static_cast<int>(predicates.size());
}

std::shared_ptr<LogicNode> parse_expression_to_logic(const Expression* expr, std::vector<Predicate>& predicates) {
    if (!expr) return nullptr;
    
    if (expr->kind == ExpressionKind::UNARY_OP) {
        const auto* un = static_cast<const UnaryOpExpr*>(expr);
        if (un->op == UnaryOpKind::NOT) {
            if (un->expr && un->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(un->expr.get());
                auto leaf = std::make_shared<LogicNode>();
                leaf->kind = LogicKind::LEAF;
                leaf->literal_id = get_or_create_predicate(pl->variable, pl->property, BinaryOpKind::EQ, false, predicates);
                return leaf;
            }
            auto child = parse_expression_to_logic(un->expr.get(), predicates);
            if (!child) return nullptr;
            auto not_node = std::make_shared<LogicNode>();
            not_node->kind = LogicKind::NOT;
            not_node->children.push_back(child);
            return not_node;
        }
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        const auto* bin = static_cast<const BinaryOpExpr*>(expr);
        if (bin->op == BinaryOpKind::AND) {
            auto left = parse_expression_to_logic(bin->left.get(), predicates);
            auto right = parse_expression_to_logic(bin->right.get(), predicates);
            if (left && right) {
                auto and_node = std::make_shared<LogicNode>();
                and_node->kind = LogicKind::AND;
                and_node->children.push_back(left);
                and_node->children.push_back(right);
                return and_node;
            } else if (left) {
                return left;
            } else if (right) {
                return right;
            }
        } else if (bin->op == BinaryOpKind::OR) {
            auto left = parse_expression_to_logic(bin->left.get(), predicates);
            auto right = parse_expression_to_logic(bin->right.get(), predicates);
            if (left && right) {
                auto or_node = std::make_shared<LogicNode>();
                or_node->kind = LogicKind::OR;
                or_node->children.push_back(left);
                or_node->children.push_back(right);
                return or_node;
            } else if (left) {
                return left;
            } else if (right) {
                return right;
            }
        } else if (bin->op == BinaryOpKind::EQ || bin->op == BinaryOpKind::NE ||
                   bin->op == BinaryOpKind::LT || bin->op == BinaryOpKind::LE ||
                   bin->op == BinaryOpKind::GT || bin->op == BinaryOpKind::GE) {
            const Expression* left = bin->left.get();
            const Expression* right = bin->right.get();
            bool reversed = false;
            double left_val = 0;
            double right_val = 0;
            bool is_left_numeric = get_numeric_value(left, left_val);
            bool is_right_numeric = get_numeric_value(right, right_val);
            
            property_type_t const_val;
            bool has_const = false;
            
            if (is_left_numeric && right->kind == ExpressionKind::PROPERTY_LOOKUP) {
                std::swap(left, right);
                reversed = true;
                const_val = left_val;
                has_const = true;
            } else if (left->kind == ExpressionKind::PROPERTY_LOOKUP && is_right_numeric) {
                const_val = right_val;
                has_const = true;
            } else if (left->kind == ExpressionKind::PROPERTY_LOOKUP && right->kind == ExpressionKind::LITERAL) {
                const auto* lit = static_cast<const LiteralExpr*>(right);
                const_val = lit->value;
                has_const = true;
            } else if (right->kind == ExpressionKind::PROPERTY_LOOKUP && left->kind == ExpressionKind::LITERAL) {
                const auto* lit = static_cast<const LiteralExpr*>(left);
                const_val = lit->value;
                has_const = true;
                std::swap(left, right);
                reversed = true;
            }
            
            if (left->kind == ExpressionKind::PROPERTY_LOOKUP && has_const) {
                const auto* pl = static_cast<const PropertyLookupExpr*>(left);
                auto op = bin->op;
                if (reversed) {
                    if (op == BinaryOpKind::LT) op = BinaryOpKind::GT;
                    else if (op == BinaryOpKind::LE) op = BinaryOpKind::GE;
                    else if (op == BinaryOpKind::GT) op = BinaryOpKind::LT;
                    else if (op == BinaryOpKind::GE) op = BinaryOpKind::LE;
                }
                
                BinaryOpKind base_op = BinaryOpKind::EQ;
                bool needs_negation = false;
                
                if (op == BinaryOpKind::EQ) {
                    base_op = BinaryOpKind::EQ;
                } else if (op == BinaryOpKind::NE) {
                    base_op = BinaryOpKind::EQ;
                    needs_negation = true;
                } else if (op == BinaryOpKind::LT) {
                    base_op = BinaryOpKind::LT;
                } else if (op == BinaryOpKind::LE) {
                    base_op = BinaryOpKind::LE;
                } else if (op == BinaryOpKind::GT) {
                    base_op = BinaryOpKind::LE;
                    needs_negation = true;
                } else if (op == BinaryOpKind::GE) {
                    base_op = BinaryOpKind::LT;
                    needs_negation = true;
                }
                
                auto leaf = std::make_shared<LogicNode>();
                leaf->kind = LogicKind::LEAF;
                leaf->literal_id = get_or_create_predicate(pl->variable, pl->property, base_op, const_val, predicates);
                
                if (needs_negation) {
                    auto not_node = std::make_shared<LogicNode>();
                    not_node->kind = LogicKind::NOT;
                    not_node->children.push_back(leaf);
                    return not_node;
                }
                return leaf;
            }
        }
    } else if (expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
        const auto* pl = static_cast<const PropertyLookupExpr*>(expr);
        auto leaf = std::make_shared<LogicNode>();
        leaf->kind = LogicKind::LEAF;
        leaf->literal_id = get_or_create_predicate(pl->variable, pl->property, BinaryOpKind::EQ, true, predicates);
        return leaf;
    }
    return nullptr;
}

std::shared_ptr<LogicNode> build_inline_logic(const PatternNode& node, std::vector<Predicate>& predicates) {
    std::vector<std::shared_ptr<LogicNode>> children;
    
    for (const auto& [prop, val_type] : node.properties) {
        property_type_t val;
        if (std::holds_alternative<int64_t>(val_type)) val = std::get<int64_t>(val_type);
        else if (std::holds_alternative<double>(val_type)) val = std::get<double>(val_type);
        else if (std::holds_alternative<std::string>(val_type)) val = std::get<std::string>(val_type);
        else if (std::holds_alternative<bool>(val_type)) val = std::get<bool>(val_type);
        else continue;
        
        auto leaf = std::make_shared<LogicNode>();
        leaf->kind = LogicKind::LEAF;
        leaf->literal_id = get_or_create_predicate(node.variable, prop, BinaryOpKind::EQ, val, predicates);
        children.push_back(leaf);
    }
    
    for (const auto& filter : node.property_filters) {
        BinaryOpKind op = BinaryOpKind::EQ;
        bool needs_negation = false;
        if (filter.op == Operation::LT) op = BinaryOpKind::LT;
        else if (filter.op == Operation::LTE) op = BinaryOpKind::LE;
        else if (filter.op == Operation::GT) { op = BinaryOpKind::LE; needs_negation = true; }
        else if (filter.op == Operation::GTE) { op = BinaryOpKind::LT; needs_negation = true; }
        else if (filter.op == Operation::EQ) op = BinaryOpKind::EQ;
        else if (filter.op == Operation::NEQ) { op = BinaryOpKind::EQ; needs_negation = true; }
        else continue;
        
        auto leaf = std::make_shared<LogicNode>();
        leaf->kind = LogicKind::LEAF;
        leaf->literal_id = get_or_create_predicate(node.variable, filter.property, op, filter.value, predicates);
        
        if (needs_negation) {
            auto not_node = std::make_shared<LogicNode>();
            not_node->kind = LogicKind::NOT;
            not_node->children.push_back(leaf);
            children.push_back(not_node);
        } else {
            children.push_back(leaf);
        }
    }
    
    if (children.empty()) return nullptr;
    if (children.size() == 1) return children[0];
    
    auto and_node = std::make_shared<LogicNode>();
    and_node->kind = LogicKind::AND;
    and_node->children = children;
    return and_node;
}

std::shared_ptr<LogicNode> build_inline_edge_logic(const PatternEdge& edge, std::vector<Predicate>& predicates) {
    std::vector<std::shared_ptr<LogicNode>> children;
    
    for (const auto& [prop, val_type] : edge.properties) {
        property_type_t val;
        if (std::holds_alternative<int64_t>(val_type)) val = std::get<int64_t>(val_type);
        else if (std::holds_alternative<double>(val_type)) val = std::get<double>(val_type);
        else if (std::holds_alternative<std::string>(val_type)) val = std::get<std::string>(val_type);
        else if (std::holds_alternative<bool>(val_type)) val = std::get<bool>(val_type);
        else continue;
        
        auto leaf = std::make_shared<LogicNode>();
        leaf->kind = LogicKind::LEAF;
        leaf->literal_id = get_or_create_predicate(edge.variable, prop, BinaryOpKind::EQ, val, predicates);
        children.push_back(leaf);
    }
    
    for (const auto& filter : edge.property_filters) {
        BinaryOpKind op = BinaryOpKind::EQ;
        bool needs_negation = false;
        if (filter.op == Operation::LT) op = BinaryOpKind::LT;
        else if (filter.op == Operation::LTE) op = BinaryOpKind::LE;
        else if (filter.op == Operation::GT) { op = BinaryOpKind::LE; needs_negation = true; }
        else if (filter.op == Operation::GTE) { op = BinaryOpKind::LT; needs_negation = true; }
        else if (filter.op == Operation::EQ) op = BinaryOpKind::EQ;
        else if (filter.op == Operation::NEQ) { op = BinaryOpKind::EQ; needs_negation = true; }
        else continue;
        
        auto leaf = std::make_shared<LogicNode>();
        leaf->kind = LogicKind::LEAF;
        leaf->literal_id = get_or_create_predicate(edge.variable, filter.property, op, filter.value, predicates);
        
        if (needs_negation) {
            auto not_node = std::make_shared<LogicNode>();
            not_node->kind = LogicKind::NOT;
            not_node->children.push_back(leaf);
            children.push_back(not_node);
        } else {
            children.push_back(leaf);
        }
    }
    
    if (children.empty()) return nullptr;
    if (children.size() == 1) return children[0];
    
    auto and_node = std::make_shared<LogicNode>();
    and_node->kind = LogicKind::AND;
    and_node->children = children;
    return and_node;
}

std::shared_ptr<LogicNode> clone_and_rename_logic_tree(const std::shared_ptr<LogicNode>& node, const std::string& c_var, const std::string& q_var, std::vector<Predicate>& predicates) {
    if (!node) return nullptr;
    auto copy = std::make_shared<LogicNode>();
    copy->kind = node->kind;
    if (node->kind == LogicKind::LEAF) {
        int old_id = node->literal_id;
        const auto& old_pred = predicates[old_id - 1];
        std::string new_var = (old_pred.variable == c_var) ? q_var : old_pred.variable;
        copy->literal_id = get_or_create_predicate(new_var, old_pred.property, old_pred.op, old_pred.value, predicates);
    } else {
        for (const auto& child : node->children) {
            copy->children.push_back(clone_and_rename_logic_tree(child, c_var, q_var, predicates));
        }
    }
    return copy;
}

std::shared_ptr<LogicNode> build_constraint_logic(const GqlQuery& c_query, const std::string& c_var, std::vector<Predicate>& predicates) {
    std::vector<std::shared_ptr<LogicNode>> conjuncts;
    if (c_query.where_expr) {
        auto q_logic = parse_expression_to_logic(c_query.where_expr.get(), predicates);
        if (q_logic) conjuncts.push_back(q_logic);
    }
    for (const auto& match : c_query.matches) {
        for (const auto& node : match.pattern.nodes) {
            if (node.variable == c_var) {
                if (node.where_expr) {
                    auto n_logic = parse_expression_to_logic(node.where_expr.get(), predicates);
                    if (n_logic) conjuncts.push_back(n_logic);
                }
                auto inline_logic = build_inline_logic(node, predicates);
                if (inline_logic) conjuncts.push_back(inline_logic);
            }
        }
    }
    if (conjuncts.empty()) return nullptr;
    if (conjuncts.size() == 1) return conjuncts[0];
    
    auto and_node = std::make_shared<LogicNode>();
    and_node->kind = LogicKind::AND;
    and_node->children = conjuncts;
    return and_node;
}

bool update_state(PropertyState& state, BinaryOpKind op, bool assigned_val, const property_type_t& value) {
    if (std::holds_alternative<int64_t>(value) || std::holds_alternative<double>(value)) {
        double val = std::holds_alternative<int64_t>(value) ? std::get<int64_t>(value) : std::get<double>(value);
        Interval limit;
        if (op == BinaryOpKind::LT) {
            if (assigned_val) {
                limit.has_upper = true;
                limit.upper_val = val;
                limit.upper_inclusive = false;
            } else {
                limit.has_lower = true;
                limit.lower_val = val;
                limit.lower_inclusive = true;
            }
        } else if (op == BinaryOpKind::LE) {
            if (assigned_val) {
                limit.has_upper = true;
                limit.upper_val = val;
                limit.upper_inclusive = true;
            } else {
                limit.has_lower = true;
                limit.lower_val = val;
                limit.lower_inclusive = false;
            }
        } else if (op == BinaryOpKind::EQ) {
            if (assigned_val) {
                limit.has_lower = true;
                limit.lower_val = val;
                limit.lower_inclusive = true;
                limit.has_upper = true;
                limit.upper_val = val;
                limit.upper_inclusive = true;
            } else {
                return true; // NEQ on numeric does not easily constrain a single interval, ignore here
            }
        } else {
            return true;
        }
        state.interval.intersect(limit);
        if (state.interval.is_empty()) {
            return false;
        }
    } else if (std::holds_alternative<std::string>(value)) {
        std::string val = std::get<std::string>(value);
        if (op == BinaryOpKind::EQ) {
            if (assigned_val) {
                if (state.has_allowed_string && state.allowed_string != val) {
                    return false;
                }
                if (state.excluded_strings.count(val)) {
                    return false;
                }
                state.has_allowed_string = true;
                state.allowed_string = val;
            } else {
                if (state.has_allowed_string && state.allowed_string == val) {
                    return false;
                }
                state.excluded_strings.insert(val);
            }
        }
    } else if (std::holds_alternative<bool>(value)) {
        bool val = std::get<bool>(value);
        if (op == BinaryOpKind::EQ) {
            bool expected = assigned_val ? val : !val;
            if (state.has_bool && state.bool_val != expected) {
                return false;
            }
            state.has_bool = true;
            state.bool_val = expected;
        }
    }
    return true;
}

bool check_theory_consistency(const std::map<int, bool>& assignment, const std::vector<Predicate>& predicates) {
    std::map<std::pair<std::string, std::string>, PropertyState> states;
    for (const auto& [var, assigned_val] : assignment) {
        if (var >= 1 && var <= static_cast<int>(predicates.size())) {
            const auto& pred = predicates[var - 1];
            auto key = std::make_pair(pred.variable, pred.property);
            if (!update_state(states[key], pred.op, assigned_val, pred.value)) {
                return false;
            }
        }
    }
    return true;
}

bool unit_propagate(const std::vector<Clause>& clauses, std::map<int, bool>& assignment) {
    bool changed = true;
    while (changed) {
        changed = false;
        for (const auto& clause : clauses) {
            int unassigned_count = 0;
            int unit_lit = 0;
            bool satisfied = false;
            for (int lit : clause.lits) {
                int var = std::abs(lit);
                bool sign = (lit > 0);
                auto it = assignment.find(var);
                if (it != assignment.end()) {
                    if (it->second == sign) {
                        satisfied = true;
                        break;
                    }
                } else {
                    unassigned_count++;
                    unit_lit = lit;
                }
            }
            if (satisfied) continue;
            if (unassigned_count == 0) {
                return false;
            }
            if (unassigned_count == 1) {
                int var = std::abs(unit_lit);
                bool sign = (unit_lit > 0);
                assignment[var] = sign;
                changed = true;
            }
        }
    }
    return true;
}

bool dpll(const std::vector<Clause>& clauses, std::map<int, bool>& assignment, const std::vector<Predicate>& predicates, int num_vars) {
    if (!unit_propagate(clauses, assignment)) {
        return false;
    }
    if (!check_theory_consistency(assignment, predicates)) {
        return false;
    }
    
    int next_var = -1;
    for (int i = 1; i <= num_vars; ++i) {
        if (assignment.find(i) == assignment.end()) {
            next_var = i;
            break;
        }
    }
    if (next_var == -1) {
        return true;
    }
    
    {
        auto backup = assignment;
        assignment[next_var] = true;
        if (dpll(clauses, assignment, predicates, num_vars)) return true;
        assignment = backup;
    }
    {
        auto backup = assignment;
        assignment[next_var] = false;
        if (dpll(clauses, assignment, predicates, num_vars)) return true;
        assignment = backup;
    }
    return false;
}

} // namespace

void DomainConstraintReasoner::domain_constraint_reasoning_pass(GqlQuery& query) {
    if (query.kind != QueryKind::SINGLE) return;
    
    std::vector<Predicate> predicates;
    std::vector<std::shared_ptr<LogicNode>> query_conjuncts;
    
    // 1. Gather all query-level and pattern-level filters and inline maps
    if (query.where_expr) {
        auto q_logic = parse_expression_to_logic(query.where_expr.get(), predicates);
        if (q_logic) query_conjuncts.push_back(q_logic);
    }
    
    for (const auto& match : query.matches) {
        for (const auto& node : match.pattern.nodes) {
            if (node.variable.empty()) continue;
            if (node.where_expr) {
                auto n_logic = parse_expression_to_logic(node.where_expr.get(), predicates);
                if (n_logic) query_conjuncts.push_back(n_logic);
            }
            auto inline_logic = build_inline_logic(node, predicates);
            if (inline_logic) query_conjuncts.push_back(inline_logic);
        }
        for (const auto& edge : match.pattern.edges) {
            if (edge.variable.empty()) continue;
            if (edge.where_expr) {
                auto e_logic = parse_expression_to_logic(edge.where_expr.get(), predicates);
                if (e_logic) query_conjuncts.push_back(e_logic);
            }
            auto inline_logic = build_inline_edge_logic(edge, predicates);
            if (inline_logic) query_conjuncts.push_back(inline_logic);
        }
    }
    
    // 2. Load constraints and intersect/negate them based on node labels
    const auto& constraints = GqlVirtualCatalog::local().get_constraints();
    for (const auto& [c_name, c_query_str] : constraints) {
        try {
            auto c_query = GqlParser::parse(c_query_str);
            if (c_query.matches.size() == 1 && c_query.matches[0].pattern.nodes.size() == 1 && c_query.matches[0].pattern.edges.empty()) {
                const auto& c_node = c_query.matches[0].pattern.nodes[0];
                std::string c_label;
                if (c_node.label_expr && c_node.label_expr->kind == LabelExprKind::LITERAL) {
                    c_label = c_node.label_expr->name;
                }
                if (c_label.empty()) continue;
                
                // Inspect all variables in the query to see if their label matches
                for (const auto& match : query.matches) {
                    for (const auto& node : match.pattern.nodes) {
                        if (node.variable.empty()) continue;
                        std::string q_label;
                        if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                            q_label = node.label_expr->name;
                        }
                        
                        if (q_label == c_label) {
                            // Extract logic tree of constraint
                            auto c_logic = build_constraint_logic(c_query, c_node.variable, predicates);
                            if (c_logic) {
                                // Rename constraint variable name to the query variable name
                                auto renamed_c = clone_and_rename_logic_tree(c_logic, c_node.variable, node.variable, predicates);
                                // Negate it: constraint must not happen
                                auto not_c = std::make_shared<LogicNode>();
                                not_c->kind = LogicKind::NOT;
                                not_c->children.push_back(renamed_c);
                                query_conjuncts.push_back(not_c);
                            }
                        }
                    }
                }
            }
        } catch (...) {
            // Ignore constraints that fail to parse
        }
    }
    
    if (query_conjuncts.empty()) return;
    
    // Conjunct everything in a root AND node
    auto root_formula = std::make_shared<LogicNode>();
    root_formula->kind = LogicKind::AND;
    root_formula->children = query_conjuncts;
    
    // Convert logic tree to CNF via Tseitin
    CNFBuilder builder;
    builder.next_var = predicates.size() + 1; // Start Tseitin vars after predicates to prevent clashes
    int root_lit = builder.convert(root_formula);
    if (root_lit != 0) {
        builder.clauses.push_back(Clause{{root_lit}});
    }
    
    if (predicates.empty() && builder.clauses.empty()) return;
    
    // Solve satisfiability using DPLL(T)
    std::map<int, bool> assignment;
    int total_vars = builder.next_var - 1;
    bool is_satisfiable = dpll(builder.clauses, assignment, predicates, total_vars);
    
    if (!is_satisfiable) {
        query.no_op = true;
    }
}

} // namespace ragedb::gql

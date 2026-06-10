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

#include "GqlExecutor.h"
#include "GqlValue.h"
#include "GqlWriter.h"
#include "GqlCatalog.h"
#include "GqlTypechecker.h"
#include "Join.h"
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <set>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <chrono>
#include <seastar/core/when_all.hh>

namespace ragedb::gql {

/**
 * @brief Represents the factorization types for query execution tree nodes.
 *
 * Factorization groups intermediate matching subgraphs to avoid premature Cartesian products.
 */
enum class FactorNodeType {
    FLAT,         ///< Flat vector of GqlRow records.
    PRODUCT,      ///< Cartesian product of child sub-trees (lazily joined).
    PATH_CONCAT,  ///< Path concatenation of nodes/edges.
    UNION         ///< Disjoint union of matching options.
};

struct FactorNode;
using FactorNodePtr = std::shared_ptr<FactorNode>;

/**
 * @brief Comparator for ordering GqlValues when partitioning factorized tree nodes.
 */
struct GqlValueLess {
    bool operator()(const GqlValue& a, const GqlValue& b) const {
        return compare_gql_values(a, b) < 0;
    }
};

// Structures for hashing property_type_t, GqlValue, and std::vector<GqlValue> to support
// accumulator hash joins in GqlExecutor.
struct PropertyHash {
    size_t operator()(const property_type_t& val) const {
        return std::visit([](const auto& arg) -> size_t {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
                return 0;
            } else if constexpr (std::is_same_v<T, bool>) {
                return std::hash<bool>{}(arg);
            } else if constexpr (std::is_same_v<T, int64_t>) {
                return std::hash<int64_t>{}(arg);
            } else if constexpr (std::is_same_v<T, double>) {
                return std::hash<double>{}(arg);
            } else if constexpr (std::is_same_v<T, std::string>) {
                return std::hash<std::string>{}(arg);
            } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
                size_t seed = arg.size();
                for (bool b : arg) {
                    seed ^= std::hash<bool>{}(b) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                }
                return seed;
            } else if constexpr (std::is_same_v<T, std::vector<int64_t>>) {
                size_t seed = arg.size();
                for (int64_t v : arg) {
                    seed ^= std::hash<int64_t>{}(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                }
                return seed;
            } else if constexpr (std::is_same_v<T, std::vector<double>>) {
                size_t seed = arg.size();
                for (double v : arg) {
                    seed ^= std::hash<double>{}(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                }
                return seed;
            } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                size_t seed = arg.size();
                for (const auto& v : arg) {
                    seed ^= std::hash<std::string>{}(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
                }
                return seed;
            } else {
                return 0;
            }
        }, val);
    }
};

struct GqlValueHash {
    size_t operator()(const GqlValue& val) const {
        size_t seed = std::hash<int>{}(static_cast<int>(val.type));
        if (val.type == GqlValue::PROPERTY) {
            seed ^= PropertyHash{}(val.property) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        } else if (val.type == GqlValue::NODE) {
            seed ^= std::hash<uint64_t>{}(val.node->getId()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        } else if (val.type == GqlValue::RELATIONSHIP) {
            seed ^= std::hash<uint64_t>{}(val.relationship->getId()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        } else if (val.type == GqlValue::RELATIONSHIP_LIST) {
            for (const auto& r : *val.relationship_list) {
                seed ^= std::hash<uint64_t>{}(r.getId()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            }
        }
        return seed;
    }
};

struct GqlValueVectorHash {
    size_t operator()(const std::vector<GqlValue>& vec) const {
        size_t seed = vec.size();
        GqlValueHash hasher;
        for (const auto& val : vec) {
            seed ^= hasher(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};

struct GqlValueVectorEqual {
    bool operator()(const std::vector<GqlValue>& a, const std::vector<GqlValue>& b) const {
        if (a.size() != b.size()) return false;
        for (size_t i = 0; i < a.size(); ++i) {
            if (compare_gql_values(a[i], b[i]) != 0) return false;
        }
        return true;
    }
};

/**
 * @brief Joins two lists of flat GqlRows on shared variables.
 *
 * Utilizes an Accumulator Hash Join algorithm when shared variables exist,
 * building a hash table on the smaller side and probing it with the larger side to
 * execute the join in O(|L| + |R|) time. Fallback to Cartesian product if no variables share.
 * Skips internal traversal trackers (prefixed with _n_ and _e_) to prevent false naming collisions.
 *
 * @param left Left side flat rows.
 * @param right Right side flat rows.
 * @return std::vector<GqlRow> The joined output rows.
 */
std::vector<GqlRow> join_flat_rows_variable(const std::vector<GqlRow>& left, const std::vector<GqlRow>& right) {
    if (left.empty() || right.empty()) return {};

    // Identify shared variables between left[0] and right[0]
    std::vector<std::string> shared_vars;
    for (const auto& [var, val] : left[0].bindings) {
        if (var.rfind("_n_", 0) == 0 || var.rfind("_e_", 0) == 0) {
            continue;
        }
        if (right[0].bindings.count(var)) {
            shared_vars.push_back(var);
        }
    }

    // If no shared variables exist, perform standard Cartesian product (nested-loop join)
    if (shared_vars.empty()) {
        std::vector<GqlRow> result;
        result.reserve(left.size() * right.size());
        for (const auto& r1 : left) {
            for (const auto& r2 : right) {
                GqlRow unified = r1;
                for (const auto& [var, val] : r2.bindings) {
                    unified.bindings[var] = val;
                }
                result.push_back(std::move(unified));
            }
        }
        return result;
    }

    // Determine the smaller side for building the hash table (Accumulator Hash Join build side)
    const std::vector<GqlRow>& build_side = (left.size() <= right.size()) ? left : right;
    const std::vector<GqlRow>& probe_side = (left.size() <= right.size()) ? right : left;
    bool left_is_build = (left.size() <= right.size());

    // Build stage: populate the hash table on the build side using the shared variables as keys
    std::unordered_map<std::vector<GqlValue>, std::vector<size_t>, GqlValueVectorHash, GqlValueVectorEqual> hash_table;
    for (size_t i = 0; i < build_side.size(); ++i) {
        std::vector<GqlValue> key;
        key.reserve(shared_vars.size());
        for (const auto& var : shared_vars) {
            auto it = build_side[i].bindings.find(var);
            if (it != build_side[i].bindings.end()) {
                key.push_back(it->second);
            } else {
                key.push_back(GqlValue{}); // Fallback to NIL if a variable is missing
            }
        }
        hash_table[key].push_back(i);
    }

    // Probe stage: stream the probe side and look up matching rows in the build side's hash table
    std::vector<GqlRow> result;
    for (const auto& r_probe : probe_side) {
        std::vector<GqlValue> key;
        key.reserve(shared_vars.size());
        for (const auto& var : shared_vars) {
            auto it = r_probe.bindings.find(var);
            if (it != r_probe.bindings.end()) {
                key.push_back(it->second);
            } else {
                key.push_back(GqlValue{});
            }
        }

        auto it = hash_table.find(key);
        if (it != hash_table.end()) {
            for (size_t idx : it->second) {
                const auto& r_build = build_side[idx];
                
                // Double check compatibility on all shared variables (including fallback/collision checks)
                bool can_unify = true;
                for (const auto& var : shared_vars) {
                    auto it_b = r_build.bindings.find(var);
                    auto it_p = r_probe.bindings.find(var);
                    if (it_b != r_build.bindings.end() && it_p != r_probe.bindings.end()) {
                        if (compare_gql_values(it_b->second, it_p->second) != 0) {
                            can_unify = false;
                            break;
                        }
                    }
                }
                
                if (can_unify) {
                    GqlRow unified = left_is_build ? r_build : r_probe;
                    const auto& right_row = left_is_build ? r_probe : r_build;
                    for (const auto& [var, val] : right_row.bindings) {
                        unified.bindings[var] = val;
                    }
                    result.push_back(std::move(unified));
                }
            }
        }
    }
    return result;
}

/**
 * @brief Node representation in a factorized query tree.
 *
 * Allows lazy merging and evaluation of multi-way patterns.
 */
struct FactorNode {
    FactorNodeType type;
    std::vector<GqlRow> flat_rows;             ///< Populated when type is FLAT.
    std::vector<FactorNodePtr> children;       ///< Populated when type is PRODUCT, PATH_CONCAT, or UNION.

    FactorNode(FactorNodeType t) : type(t) {}
    FactorNode(std::vector<GqlRow> rows) : type(FactorNodeType::FLAT), flat_rows(std::move(rows)) {}

    /**
     * @brief Checks if this node contains no matching patterns or rows.
     */
    bool is_empty() const {
        switch (type) {
            case FactorNodeType::FLAT:
                return flat_rows.empty();
            case FactorNodeType::UNION: {
                for (const auto& child : children) {
                    if (!child->is_empty()) return false;
                }
                return true;
            }
            case FactorNodeType::PRODUCT:
            case FactorNodeType::PATH_CONCAT: {
                for (const auto& child : children) {
                    if (child->is_empty()) return true;
                }
                return false;
            }
        }
        return false;
    }

    /**
     * @brief Returns the set of query variable names bound in this sub-tree.
     */
    std::set<std::string> freevars() const {
        std::set<std::string> vars;
        if (type == FactorNodeType::FLAT) {
            if (!flat_rows.empty()) {
                for (const auto& [k, v] : flat_rows[0].bindings) {
                    if (k.rfind("_n_", 0) != 0 && k.rfind("_e_", 0) != 0) {
                        vars.insert(k);
                    }
                }
            }
        } else {
            for (const auto& child : children) {
                auto child_vars = child->freevars();
                vars.insert(child_vars.begin(), child_vars.end());
            }
        }
        return vars;
    }

    /**
     * @brief Partitions this factorized node into sub-trees grouped by the value of a specific variable.
     *
     * Enables merging sub-trees that share a common variable during natural joins.
     *
     * @param var Variable name to partition by.
     * @return std::map<GqlValue, FactorNodePtr, GqlValueLess> Mapped sub-trees.
     */
    std::map<GqlValue, FactorNodePtr, GqlValueLess> partition(const std::string& var) const {
        std::map<GqlValue, FactorNodePtr, GqlValueLess> partition_map;
        if (type == FactorNodeType::FLAT) {
            std::map<GqlValue, std::vector<GqlRow>, GqlValueLess> groups;
            for (const auto& row : flat_rows) {
                auto it = row.bindings.find(var);
                if (it != row.bindings.end()) {
                    groups[it->second].push_back(row);
                }
            }
            for (auto& [val, group_rows] : groups) {
                partition_map[val] = std::make_shared<FactorNode>(std::move(group_rows));
            }
        } else if (type == FactorNodeType::UNION) {
            std::map<GqlValue, std::vector<FactorNodePtr>, GqlValueLess> groups;
            for (const auto& child : children) {
                auto child_parts = child->partition(var);
                for (auto& [val, part] : child_parts) {
                    groups[val].push_back(part);
                }
            }
            for (auto& [val, group_nodes] : groups) {
                auto union_node = std::make_shared<FactorNode>(FactorNodeType::UNION);
                union_node->children = std::move(group_nodes);
                partition_map[val] = union_node;
            }
        } else if (type == FactorNodeType::PRODUCT || type == FactorNodeType::PATH_CONCAT) {
            int target_idx = -1;
            for (size_t i = 0; i < children.size(); ++i) {
                auto free_vars = children[i]->freevars();
                if (free_vars.count(var)) {
                    target_idx = i;
                    break;
                }
            }
            if (target_idx != -1) {
                auto target_node = children[target_idx];
                auto target_parts = target_node->partition(var);
                for (auto& [val, part] : target_parts) {
                    auto product_node = std::make_shared<FactorNode>(type);
                    product_node->children = children;
                    product_node->children[target_idx] = part;
                    partition_map[val] = product_node;
                }
            }
        }
        return partition_map;
    }

    /**
     * @brief Performs a natural join of this tree and another on a shared variable.
     *
     * Partitions both trees by `join_var` and aligns matching sub-trees under a `PRODUCT` node.
     *
     * @param other The other tree to join.
     * @param join_var The variable to join on.
     * @return FactorNodePtr The combined union tree.
     */
    FactorNodePtr join(const FactorNodePtr& other, const std::string& join_var) const {
        auto left_parts = partition(join_var);
        auto right_parts = other->partition(join_var);

        auto union_node = std::make_shared<FactorNode>(FactorNodeType::UNION);
        for (const auto& [val, left_node] : left_parts) {
            auto it = right_parts.find(val);
            if (it != right_parts.end()) {
                auto product_node = std::make_shared<FactorNode>(FactorNodeType::PRODUCT);
                product_node->children.push_back(left_node);
                product_node->children.push_back(it->second);
                union_node->children.push_back(product_node);
            }
        }
        return union_node;
    }

    /**
     * @brief Flattens this factorized representation into a list of flat GqlRows.
     */
    std::vector<GqlRow> flatten() const {
        if (type == FactorNodeType::FLAT) {
            return flat_rows;
        }
        if (type == FactorNodeType::UNION) {
            auto dom = freevars();
            std::vector<GqlRow> rows;
            for (const auto& child : children) {
                for (auto r : child->flatten()) {
                    for (const auto& var : dom) {
                        if (r.bindings.find(var) == r.bindings.end()) {
                            r.bindings[var] = GqlValue();
                        }
                    }
                    rows.push_back(std::move(r));
                }
            }
            return rows;
        }
        if (type == FactorNodeType::PRODUCT) {
            if (children.empty()) return {};
            std::vector<GqlRow> result = children[0]->flatten();
            for (size_t i = 1; i < children.size(); ++i) {
                result = join_flat_rows_variable(result, children[i]->flatten());
            }
            return result;
        }
        return {};
    }
};

/**
 * @brief Helper structure wrapping a factorized node for use in matching chains.
 */
struct IntermediateResult {
    std::vector<GqlRow> rows;      ///< Flat rows (lazily populated on ensure_flat).
    FactorNodePtr root;            ///< The root factorized node of the query execution tree.

    IntermediateResult() {
        root = std::make_shared<FactorNode>(std::vector<GqlRow>{});
    }

    IntermediateResult(std::vector<GqlRow> r) : rows(r) {
        root = std::make_shared<FactorNode>(std::move(r));
    }

    IntermediateResult(FactorNodePtr node) : root(std::move(node)) {}

    static IntermediateResult empty() {
        return IntermediateResult(std::vector<GqlRow>{});
    }

    /**
     * @brief Recursively flattens the factorized query tree to yield a flat row list.
     */
    void ensure_flat() {
        if (rows.empty() && root) {
            rows = root->flatten();
        }
    }

    FactorNodePtr into_root() const {
        if (root) return root;
        return std::make_shared<FactorNode>(rows);
    }

    bool is_empty() const {
        if (root) return root->is_empty();
        return rows.empty();
    }

    std::set<std::string> freevars() const {
        if (root) return root->freevars();
        std::set<std::string> vars;
        if (!rows.empty()) {
            for (const auto& [k, v] : rows[0].bindings) {
                vars.insert(k);
            }
        }
        return vars;
    }

    /**
     * @brief Joins another intermediate result as a UNION node.
     */
    IntermediateResult union_with(const IntermediateResult& other) const {
        auto union_node = std::make_shared<FactorNode>(FactorNodeType::UNION);
        union_node->children.push_back(into_root());
        union_node->children.push_back(other.into_root());
        return IntermediateResult(union_node);
    }
};

/**
 * @brief Performs natural join optimization on two intermediate results.
 *
 * If results share variables, joins their factor nodes. Otherwise, builds a lazy PRODUCT node.
 *
 * @param left Left operand.
 * @param right Right operand.
 * @param limit Row limit to apply.
 * @return IntermediateResult Joined factorized representation.
 */
IntermediateResult natural_join(IntermediateResult left, IntermediateResult right, size_t limit = 0) {
    auto lk = left.freevars();
    auto rk = right.freevars();
    std::vector<std::string> shared;
    for (const auto& var : lk) {
        if (rk.count(var)) {
            shared.push_back(var);
        }
    }

    if (!shared.empty()) {
        std::string jv = shared[0];
        auto left_root = left.into_root();
        auto right_root = right.into_root();
        auto joined = left_root->join(right_root, jv);
        auto combined = IntermediateResult(joined);
        if (limit > 0) {
            combined.ensure_flat();
            if (combined.rows.size() > limit) {
                combined.rows.resize(limit);
            }
        }
        return combined;
    } else {
        auto factorized = std::make_shared<FactorNode>(FactorNodeType::PRODUCT);
        factorized->children.push_back(left.into_root());
        factorized->children.push_back(right.into_root());
        auto combined = IntermediateResult(factorized);
        if (limit > 0) {
            combined.ensure_flat();
            if (combined.rows.size() > limit) {
                combined.rows.resize(limit);
            }
        }
        return combined;
    }
}

/**
 * @brief Performs a left outer join of two intermediate results.
 *
 * Used for OPTIONAL MATCH chains that share variables. Left rows without matching right rows
 * are padded with null (nil) values.
 *
 * @param left Left side (primary query context).
 * @param right Right side (optional match context).
 * @param bound_vars Set of variables bound in the left context.
 * @param new_vars Set of variables introduced in the right context.
 * @return IntermediateResult Joined factorized representation.
 */
IntermediateResult left_outer_join(
    IntermediateResult left,
    IntermediateResult right,
    const std::set<std::string>& bound_vars,
    const std::set<std::string>& new_vars
) {
    std::vector<std::string> pad_vars;
    for (const auto& v : new_vars) {
        if (!bound_vars.count(v)) {
            pad_vars.push_back(v);
        }
    }

    auto lk = left.freevars();
    auto rk = right.freevars();
    std::vector<std::string> shared;
    for (const auto& var : lk) {
        if (rk.count(var)) {
            shared.push_back(var);
        }
    }

    auto make_pad_node = [&pad_vars]() {
        GqlRow pad_row;
        for (const auto& v : pad_vars) {
            pad_row.bindings[v] = GqlValue();
        }
        return std::make_shared<FactorNode>(std::vector<GqlRow>{pad_row});
    };

    if (!shared.empty()) {
        std::string jv = shared[0];
        auto left_parts = left.into_root()->partition(jv);
        auto right_parts = right.into_root()->partition(jv);

        auto union_node = std::make_shared<FactorNode>(FactorNodeType::UNION);
        for (const auto& [val, left_node] : left_parts) {
            auto it = right_parts.find(val);
            if (it != right_parts.end()) {
                auto product_node = std::make_shared<FactorNode>(FactorNodeType::PRODUCT);
                product_node->children.push_back(left_node);
                product_node->children.push_back(it->second);
                union_node->children.push_back(product_node);
            } else {
                auto product_node = std::make_shared<FactorNode>(FactorNodeType::PRODUCT);
                product_node->children.push_back(left_node);
                product_node->children.push_back(make_pad_node());
                union_node->children.push_back(product_node);
            }
        }
        return IntermediateResult(union_node);
    } else {
        if (right.is_empty()) {
            auto pad_node = make_pad_node();
            auto factorized = std::make_shared<FactorNode>(FactorNodeType::PRODUCT);
            factorized->children.push_back(left.into_root());
            factorized->children.push_back(pad_node);
            return IntermediateResult(factorized);
        } else {
            auto factorized = std::make_shared<FactorNode>(FactorNodeType::PRODUCT);
            factorized->children.push_back(left.into_root());
            factorized->children.push_back(right.into_root());
            return IntermediateResult(factorized);
        }
    }
}

bool matches_label_expr(const std::string& actual_type, const std::shared_ptr<LabelExpression>& expr) {
    if (!expr) return true;
    switch (expr->kind) {
        case LabelExprKind::LITERAL:
            return actual_type == expr->name;
        case LabelExprKind::NOT:
            return !matches_label_expr(actual_type, expr->expr);
        case LabelExprKind::AND:
            return matches_label_expr(actual_type, expr->left) && matches_label_expr(actual_type, expr->right);
        case LabelExprKind::OR:
            return matches_label_expr(actual_type, expr->left) || matches_label_expr(actual_type, expr->right);
    }
    return false;
}

struct ProjectionPruner {
    std::map<std::string, std::set<std::string>> accessed_props;
    std::set<std::string> whole_objects;

    bool should_prune(const std::string& var) const {
        if (var.empty()) return false;
        if (whole_objects.count(var)) return false;
        return true;
    }

    const std::set<std::string>& get_keys(const std::string& var) const {
        static const std::set<std::string> empty_keys;
        auto it = accessed_props.find(var);
        if (it != accessed_props.end()) {
            return it->second;
        }
        return empty_keys;
    }
};

static void collect_accessed_properties(const Expression* expr,
                                 std::map<std::string, std::set<std::string>>& accessed_props,
                                 std::set<std::string>& whole_objects) {
    if (!expr) return;
    switch (expr->kind) {
        case ExpressionKind::VARIABLE: {
            auto* var = static_cast<const VariableExpr*>(expr);
            whole_objects.insert(var->name);
            break;
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* prop_lookup = static_cast<const PropertyLookupExpr*>(expr);
            accessed_props[prop_lookup->variable].insert(prop_lookup->property);
            break;
        }
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<const UnaryOpExpr*>(expr);
            collect_accessed_properties(un->expr.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<const BinaryOpExpr*>(expr);
            collect_accessed_properties(bin->left.get(), accessed_props, whole_objects);
            collect_accessed_properties(bin->right.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<const AggregateExpr*>(expr);
            collect_accessed_properties(agg->expr.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::LITERAL:
        default:
            break;
    }
}


/**
 * @brief Retrieves the starting nodes for a pattern match.
 * 
 * Inspects node pattern label and properties. Utilizes index scans if a property and a label
 * are supplied. Falls back to AllNodes scan either labeled or global if properties are absent.
 * 
 * @param graph The RageDB graph instance.
 * @param node The vertex pattern definition.
 * @return seastar::future<std::vector<Node>> Future wrapping matching start nodes.
 */
/**
 * @brief Retrieves the starting nodes for a pattern match.
 * 
 * Inspects node pattern label and properties. Utilizes index scans if a property and a label
 * are supplied. Falls back to AllNodes scan either labeled or global if properties are absent.
 * 
 * @param graph The RageDB graph instance.
 * @param node The vertex pattern definition.
 * @param limit Optional limit for start node queries.
 * @param pruner The projection properties pruner.
 * @return seastar::future<std::vector<Node>> Future wrapping matching start nodes.
 */
seastar::future<std::vector<Node>> get_start_nodes(ragedb::Graph& graph, const PatternNode& node, size_t limit = 0, const ProjectionPruner& pruner = {}) {
    size_t scan_limit = (limit > 0) ? limit : 100000;
    std::string single_label = "";
    if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
        single_label = node.label_expr->name;
    }

    struct FilterInfo {
        std::string property;
        Operation op;
        property_type_t value;
    };
    std::vector<FilterInfo> all_filters;
    for (const auto& [prop, val] : node.properties) {
        all_filters.push_back({prop, Operation::EQ, val});
    }
    for (const auto& filter : node.property_filters) {
        all_filters.push_back({filter.property, filter.op, filter.value});
    }
    
    seastar::future<std::vector<Node>> fut = seastar::make_ready_future<std::vector<Node>>();
    // Leapfrog Join Optimization (WCOJ): Intersect multi-property search results via their IDs.
    if (all_filters.size() > 1 && !single_label.empty()) {
        std::vector<seastar::future<std::vector<Node>>> futs;
        // Query the shard index concurrently for each property filter constraint.
        for (const auto& filter : all_filters) {
            futs.push_back(graph.shard.local().FindNodesPeered(single_label, filter.property, filter.op, filter.value, 0, scan_limit));
        }
        fut = seastar::when_all_succeed(futs.begin(), futs.end())
        .then([scan_limit](std::vector<std::vector<Node>> node_lists) {
            if (node_lists.empty()) return std::vector<Node>{};
            
            // Gather, sort, and organize node IDs from each index scan for leapfrog intersection.
            std::vector<std::vector<uint64_t>> id_lists;
            for (const auto& list : node_lists) {
                std::vector<uint64_t> ids;
                for (const auto& n : list) {
                    ids.push_back(n.getId());
                }
                std::sort(ids.begin(), ids.end());
                id_lists.push_back(std::move(ids));
            }
            
            // Perform Worst-Case-Optimal Join intersection using the leapfrog join algorithm
            std::vector<uint64_t> intersected_ids = leapfrogJoin(id_lists);
            
            // Filter the original nodes to match only the intersected IDs
            std::unordered_set<uint64_t> valid_ids(intersected_ids.begin(), intersected_ids.end());
            std::vector<Node> result;
            for (const auto& n : node_lists[0]) {
                if (valid_ids.count(n.getId())) {
                    result.push_back(n);
                    if (result.size() >= scan_limit) {
                        break;
                    }
                }
            }
            return result;
        });
    } else if (all_filters.size() == 1 && !single_label.empty()) {
        const auto& filter = all_filters[0];
        fut = graph.shard.local().FindNodesPeered(single_label, filter.property, filter.op, filter.value, 0, scan_limit);
    } else if (!single_label.empty()) {
        fut = graph.shard.local().AllNodesPeered(single_label, 0, scan_limit);
    } else {
        fut = graph.shard.local().AllNodesPeered(0, scan_limit);
    }

    return fut.then([&graph, degree_opt_info = node.degree_opt_info, var = node.variable, pruner](std::vector<Node> result_list) {
        // Optimization: If relationship count optimization is active, retrieve node degrees asynchronously.
        if (degree_opt_info.empty()) {
            if (pruner.should_prune(var)) {
                auto keys = pruner.get_keys(var);
                for (auto& n : result_list) {
                    n.pruneProperties(keys);
                }
            }
            return seastar::make_ready_future<std::vector<Node>>(std::move(result_list));
        }

        // Retrieve node degrees asynchronously and write them as temporary node properties.
        std::vector<seastar::future<>> futs;
        auto shared_list = std::make_shared<std::vector<Node>>(std::move(result_list));

        for (auto& n : *shared_list) {
            for (const auto& info : degree_opt_info) {
                // Initialize the degree future directly using ternary expression to bypass Seastar future's lack of default constructor.
                seastar::future<uint64_t> deg_fut = info.rel_types.empty()
                    ? graph.shard.local().NodeGetDegreePeered(n.getId(), info.direction)
                    : (info.rel_types.size() == 1
                        ? graph.shard.local().NodeGetDegreePeered(n.getId(), info.direction, info.rel_types[0])
                        : graph.shard.local().NodeGetDegreePeered(n.getId(), info.direction, info.rel_types));
                
                Node* node_ptr = &n;
                futs.push_back(deg_fut.then([node_ptr, prop_name = info.property_name](uint64_t deg) {
                    // Store the degree count directly as a node property using public setProperty.
                    node_ptr->setProperty(prop_name, static_cast<int64_t>(deg));
                }));
            }
        }

        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([shared_list, var, pruner]() {
            std::vector<Node> final_list = std::move(*shared_list);
            if (pruner.should_prune(var)) {
                auto keys = pruner.get_keys(var);
                for (auto& n : final_list) {
                    n.pruneProperties(keys);
                }
            }
            return final_list;
        });
    });
}

struct PathHop {
    std::vector<Relationship> rels;
    std::vector<Node> nodes;
};

seastar::future<std::vector<PathHop>> traverse_var_len_async(
    ragedb::Graph& graph,
    uint64_t current_node_id,
    const PatternEdge& edge,
    const PatternNode& next_node,
    uint64_t current_depth,
    std::vector<uint64_t> visited_rel_ids,
    std::vector<Relationship> current_path_rels,
    std::vector<Node> current_path_nodes,
    size_t limit = 0,
    const ProjectionPruner& pruner = {}
) {
    if (current_depth > edge.max_hops) {
        return seastar::make_ready_future<std::vector<PathHop>>();
    }

    std::vector<PathHop> local_results;
    if (current_depth >= edge.min_hops) {
        local_results.push_back({current_path_rels, current_path_nodes});
    }

    if (current_depth == edge.max_hops) {
        return seastar::make_ready_future<std::vector<PathHop>>(std::move(local_results));
    }

    Direction dir = Direction::BOTH;
    if (edge.direction == EdgeDirection::RIGHT) dir = Direction::OUT;
    else if (edge.direction == EdgeDirection::LEFT) dir = Direction::IN;

    std::string edge_type = "";
    if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
        edge_type = edge.label_expr->name;
    }

    auto handle_rels = [&graph, edge, next_node, current_node_id, current_depth, visited_rel_ids = std::move(visited_rel_ids),
           current_path_rels = std::move(current_path_rels), current_path_nodes = std::move(current_path_nodes),
           local_results = std::move(local_results), limit, pruner](std::vector<Relationship> rels) mutable {

        if (pruner.should_prune(edge.variable)) {
            auto keys = pruner.get_keys(edge.variable);
            for (auto& r : rels) {
                r.pruneProperties(keys);
            }
        }

        std::vector<seastar::future<std::vector<PathHop>>> branch_futs;
        for (const auto& rel : rels) {
            // Relationship uniqueness check (edge isomorphic path matching)
            if (std::find(visited_rel_ids.begin(), visited_rel_ids.end(), rel.getId()) != visited_rel_ids.end()) {
                continue;
            }

            // Check relationship type/properties
            if (edge.label_expr && !matches_label_expr(rel.getType(), edge.label_expr)) {
                continue;
            }
            if (!matches_properties(rel.getProperties(), edge.properties) || !matches_filters(rel.getProperties(), edge.property_filters)) {
                continue;
            }

            uint64_t target_id = (rel.getStartingNodeId() == current_node_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

            branch_futs.push_back(
                graph.shard.local().NodeGetPeered(target_id)
                .then([&graph, rel, edge, next_node, current_depth, visited_rel_ids, current_path_rels, current_path_nodes, target_id, limit, pruner](Node target_node) mutable {
                    if (pruner.should_prune(next_node.variable)) {
                        target_node.pruneProperties(pruner.get_keys(next_node.variable));
                    }
                    visited_rel_ids.push_back(rel.getId());
                    current_path_rels.push_back(rel);
                    current_path_nodes.push_back(target_node);

                    return traverse_var_len_async(
                        graph,
                        target_id,
                        edge,
                        next_node,
                        current_depth + 1,
                        std::move(visited_rel_ids),
                        std::move(current_path_rels),
                        std::move(current_path_nodes),
                        limit,
                        pruner
                    );
                })
            );
        }

        if (branch_futs.empty()) {
            return seastar::make_ready_future<std::vector<PathHop>>(std::move(local_results));
        }

        return seastar::when_all_succeed(branch_futs.begin(), branch_futs.end())
        .then([local_results = std::move(local_results), limit](std::vector<std::vector<PathHop>> nested_branches) mutable {
            for (auto& branch_res : nested_branches) {
                local_results.insert(local_results.end(), branch_res.begin(), branch_res.end());
                if (limit > 0 && local_results.size() >= limit) {
                    local_results.resize(limit);
                    break;
                }
            }
            return local_results;
        });
    };

    if (edge_type.empty()) {
        return graph.shard.local().NodeGetRelationshipsPeered(current_node_id, dir).then(std::move(handle_rels));
    } else {
        return graph.shard.local().NodeGetRelationshipsPeered(current_node_id, dir, edge_type).then(std::move(handle_rels));
    }
}

/**
 * @brief Asynchronously traverses a single edge pattern hop from a given row node.
 * 
 * Evaluates edge label, properties, direction (incoming, outgoing, undirected), and matching
 * properties of the target destination node. Binds the traversed edge and target node variables.
 * 
 * @param graph The RageDB graph instance.
 * @param row The current query row (source context).
 * @param edge The pattern edge describing the hop.
 * @param next_node The pattern node that we must reach.
 * @param node_idx The current node index in the traversal chain.
 * @param limit Optional row limit for traversing.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping new rows representing extended paths.
 */
seastar::future<std::vector<GqlRow>> traverse_step(ragedb::Graph& graph, const GqlRow& row, const PatternEdge& edge, const PatternNode& next_node, size_t node_idx, size_t limit = 0, const ProjectionPruner& pruner = {}) {
    auto it = row.bindings.find("_n_" + std::to_string(node_idx));
    if (it == row.bindings.end()) {
        return seastar::make_ready_future<std::vector<GqlRow>>();
    }
    uint64_t src_id = it->second.node->getId();

    if (edge.is_variable_length) {
        return traverse_var_len_async(graph, src_id, edge, next_node, 0, {}, {}, {}, limit, pruner)
        .then([row, edge, next_node, node_idx](std::vector<PathHop> hops) {
            std::vector<GqlRow> out;
            for (const auto& hop : hops) {
                if (hop.nodes.empty()) {
                    continue;
                }
                const Node& final_node = hop.nodes.back();
                if (next_node.label_expr && !matches_label_expr(final_node.getType(), next_node.label_expr)) {
                    continue;
                }
                if (!matches_properties(final_node.getProperties(), next_node.properties) || !matches_filters(final_node.getProperties(), next_node.property_filters)) {
                    continue;
                }

                GqlRow new_row = row;
                new_row.bindings[edge.variable] = GqlValue(hop.rels);
                new_row.bindings["_e_" + std::to_string(node_idx)] = GqlValue(hop.rels);
                new_row.bindings[next_node.variable] = GqlValue(final_node);
                new_row.bindings["_n_" + std::to_string(node_idx + 1)] = GqlValue(final_node);
                out.push_back(new_row);
            }
            return out;
        });
    }

    Direction dir = Direction::BOTH;
    if (edge.direction == EdgeDirection::RIGHT) dir = Direction::OUT;
    else if (edge.direction == EdgeDirection::LEFT) dir = Direction::IN;

    std::string edge_type = "";
    if (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) {
        edge_type = edge.label_expr->name;
    }

    auto handle_rels = [row, edge, next_node, src_id, node_idx, &graph, limit, pruner](std::vector<Relationship> rels) {
        if (pruner.should_prune(edge.variable)) {
            auto keys = pruner.get_keys(edge.variable);
            for (auto& r : rels) {
                r.pruneProperties(keys);
            }
        }
        std::vector<seastar::future<std::optional<GqlRow>>> futs;
        for (const auto& rel : rels) {
            if (edge.label_expr && !matches_label_expr(rel.getType(), edge.label_expr)) {
                continue;
            }
            if (!matches_properties(rel.getProperties(), edge.properties) || !matches_filters(rel.getProperties(), edge.property_filters)) {
                continue;
            }

            uint64_t target_id = (rel.getStartingNodeId() == src_id) ? rel.getEndingNodeId() : rel.getStartingNodeId();

            futs.push_back(
                graph.shard.local().NodeGetPeered(target_id)
                .then([row, rel, edge, next_node, node_idx, pruner](Node target_node) -> std::optional<GqlRow> {
                    if (pruner.should_prune(next_node.variable)) {
                        target_node.pruneProperties(pruner.get_keys(next_node.variable));
                    }
                    if (next_node.label_expr && !matches_label_expr(target_node.getType(), next_node.label_expr)) {
                        return std::nullopt;
                    }
                    if (!matches_properties(target_node.getProperties(), next_node.properties) || !matches_filters(target_node.getProperties(), next_node.property_filters)) {
                        return std::nullopt;
                    }

                    GqlRow new_row = row;
                    new_row.bindings[edge.variable] = GqlValue(rel);
                    new_row.bindings["_e_" + std::to_string(node_idx)] = GqlValue(rel);
                    new_row.bindings[next_node.variable] = GqlValue(target_node);
                    new_row.bindings["_n_" + std::to_string(node_idx + 1)] = GqlValue(target_node);
                    return new_row;
                })
            );
        }
        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([limit](std::vector<std::optional<GqlRow>> opts) {
            std::vector<GqlRow> out;
            for (const auto& opt : opts) {
                if (opt) {
                    out.push_back(*opt);
                    if (limit > 0 && out.size() >= limit) {
                        break;
                    }
                }
            }
            return out;
        });
    };

    if (edge_type.empty()) {
        return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir).then(std::move(handle_rels));
    } else {
        return graph.shard.local().NodeGetRelationshipsPeered(src_id, dir, edge_type).then(std::move(handle_rels));
    }
}

/**
 * @brief Recursively traverses a path pattern, processing one edge-node step at a time.
 * 
 * @param graph The RageDB graph instance.
 * @param prep_pattern The prepared path pattern containing filled variables.
 * @param step_idx Current edge index in the path pattern.
 * @param current_step_rows Rows generated by the previous step.
 * @param limit Optional limit for rows returned.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping the final traversed rows.
 */
seastar::future<std::vector<GqlRow>> traverse_path_pattern_iterative(ragedb::Graph& graph, const PathPattern& prep_pattern, size_t step_idx, std::vector<GqlRow> current_step_rows, size_t limit = 0, const ProjectionPruner& pruner = {}) {
    if (step_idx >= prep_pattern.edges.size()) {
        return seastar::make_ready_future<std::vector<GqlRow>>(std::move(current_step_rows));
    }

    const auto& edge = prep_pattern.edges[step_idx];
    const auto& next_node = prep_pattern.nodes[step_idx + 1];

    std::vector<seastar::future<std::vector<GqlRow>>> futs;
    for (const auto& row : current_step_rows) {
        futs.push_back(traverse_step(graph, row, edge, next_node, step_idx, limit, pruner));
    }

    return seastar::when_all_succeed(futs.begin(), futs.end())
    .then([&graph, prep_pattern, step_idx, limit, pruner](std::vector<std::vector<GqlRow>> nested) {
        std::vector<GqlRow> next_rows;
        for (const auto& vec : nested) {
            next_rows.insert(next_rows.end(), vec.begin(), vec.end());
            if (limit > 0 && next_rows.size() >= limit) {
                next_rows.resize(limit);
                break;
            }
        }
        return traverse_path_pattern_iterative(graph, prep_pattern, step_idx + 1, std::move(next_rows), limit, pruner);
    });
}

/**
 * @brief Entry point for traversing a path pattern.
 * 
 * Generates default variable names for unlabeled/unnamed nodes/edges, resolves start nodes,
 * and initiates the recursive traversal.
 * 
 * @param graph The RageDB graph instance.
 * @param pattern The GQL path pattern to evaluate.
 * @param base_row The base row context.
 * @param limit Optional limit for traversal paths.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping matches.
 */
seastar::future<std::vector<GqlRow>> traverse_path_pattern(ragedb::Graph& graph, const PathPattern& pattern, const GqlRow& base_row, size_t limit = 0, const ProjectionPruner& pruner = {}) {
    // Prep pattern with variables if missing
    PathPattern prep_pattern = pattern;
    for (size_t j = 0; j < prep_pattern.nodes.size(); ++j) {
        if (prep_pattern.nodes[j].variable.empty()) {
            prep_pattern.nodes[j].variable = "_n_" + std::to_string(j) + "_user_empty";
        }
    }
    for (size_t j = 0; j < prep_pattern.edges.size(); ++j) {
        if (prep_pattern.edges[j].variable.empty()) {
            prep_pattern.edges[j].variable = "_e_" + std::to_string(j) + "_user_empty";
        }
    }

    auto start_node_var = prep_pattern.nodes[0].variable;
    auto bound_it = base_row.bindings.find(start_node_var);

    size_t start_node_limit = prep_pattern.edges.empty() ? limit : 0;
    seastar::future<std::vector<Node>> start_nodes_fut = seastar::make_ready_future<std::vector<Node>>();
    if (bound_it != base_row.bindings.end() && bound_it->second.type == GqlValue::NODE) {
        start_nodes_fut = seastar::make_ready_future<std::vector<Node>>(std::vector<Node>{ *bound_it->second.node });
    } else {
        start_nodes_fut = get_start_nodes(graph, prep_pattern.nodes[0], start_node_limit, pruner);
    }

    return start_nodes_fut.then([base_row, prep_pattern, &graph, limit, pruner](std::vector<Node> start_nodes) {
        std::vector<GqlRow> initial_rows;
        for (const auto& node : start_nodes) {
            if (prep_pattern.nodes[0].label_expr && !matches_label_expr(node.getType(), prep_pattern.nodes[0].label_expr)) {
                continue;
            }
            if (!matches_properties(node.getProperties(), prep_pattern.nodes[0].properties) || !matches_filters(node.getProperties(), prep_pattern.nodes[0].property_filters)) {
                continue;
            }
            GqlRow new_row = base_row;
            new_row.bindings[prep_pattern.nodes[0].variable] = GqlValue(node);
            new_row.bindings["_n_0"] = GqlValue(node);
            initial_rows.push_back(new_row);
            if (limit > 0 && prep_pattern.edges.empty() && initial_rows.size() >= limit) {
                break;
            }
        }

        return traverse_path_pattern_iterative(graph, prep_pattern, 0, std::move(initial_rows), limit, pruner);
    });
}

/**
 * @brief Traverses a single GQL MATCH/OPTIONAL MATCH statement.
 * 
 * If OPTIONAL MATCH generates no results, binds all newly introduced variables to null values
 * and retains the incoming row.
 * 
 * @param graph The RageDB graph instance.
 * @param stmt The MatchStatement AST node.
 * @param row The row context.
 * @param limit Optional limit for traversing.
 * @return seastar::future<std::vector<GqlRow>> Future wrapping evaluated rows.
 */
seastar::future<std::vector<GqlRow>> traverse_match_statement(ragedb::Graph& graph, const MatchStatement& stmt, const GqlRow& row, size_t limit = 0, const ProjectionPruner& pruner = {}) {
    return traverse_path_pattern(graph, stmt.pattern, row, limit, pruner)
    .then([row, stmt](std::vector<GqlRow> traversed_rows) {
        if (traversed_rows.empty() && stmt.is_optional) {
            GqlRow opt_row = row;
            for (const auto& node : stmt.pattern.nodes) {
                if (!node.variable.empty() && opt_row.bindings.find(node.variable) == opt_row.bindings.end()) {
                    opt_row.bindings[node.variable] = GqlValue();
                }
            }
            for (const auto& edge : stmt.pattern.edges) {
                if (!edge.variable.empty() && opt_row.bindings.find(edge.variable) == opt_row.bindings.end()) {
                    opt_row.bindings[edge.variable] = GqlValue();
                }
            }
            return std::vector<GqlRow>{opt_row};
        }
        return traversed_rows;
    });
}

/**
 * @brief Recursively executes a chain of sequential MATCH statements.
 * 
 * If a MATCH statement shares no common variables with the accumulated results, it is
 * executed independently, and the results are joined using a factorized lazy join (natural join or outer join),
 * preventing early Cartesian product expansion. Otherwise, it executes relative to the incoming flat rows.
 * 
 * @param graph The RageDB graph instance.
 * @param matches List of match statements to execute.
 * @param match_idx Current match index.
 * @param incoming Accumulated intermediate results.
 * @param limit Optional limit to truncate query paths.
 * @return seastar::future<IntermediateResult> Future wrapping the factorized matched rows.
 */
seastar::future<IntermediateResult> execute_match_chain_factorized(ragedb::Graph& graph, const std::vector<MatchStatement>& matches, size_t match_idx, IntermediateResult incoming, size_t limit = 0, const ProjectionPruner& pruner = {}) {
    if (match_idx >= matches.size()) {
        return seastar::make_ready_future<IntermediateResult>(std::move(incoming));
    }

    const auto& stmt = matches[match_idx];

    // Check if the current MATCH statement shares any query variables with previous MATCH results.
    auto incoming_vars = incoming.freevars();
    bool has_shared = false;
    for (const auto& node : stmt.pattern.nodes) {
        if (!node.variable.empty() && incoming_vars.count(node.variable)) {
            has_shared = true;
            break;
        }
    }
    for (const auto& edge : stmt.pattern.edges) {
        if (!edge.variable.empty() && incoming_vars.count(edge.variable)) {
            has_shared = true;
            break;
        }
    }

    // Optimization: If no variables are shared, execute the MATCH independently and construct a factorized join.
    if (!has_shared && !incoming_vars.empty()) {
        return traverse_path_pattern(graph, stmt.pattern, GqlRow{}, limit, pruner)
        .then([&graph, &matches, match_idx, incoming = std::move(incoming), stmt, limit, pruner](std::vector<GqlRow> pattern_rows) mutable {
            IntermediateResult pattern_res(std::move(pattern_rows));
            IntermediateResult joined;
            if (stmt.is_optional) {
                std::set<std::string> new_vars;
                for (const auto& node : stmt.pattern.nodes) {
                    if (!node.variable.empty()) new_vars.insert(node.variable);
                }
                for (const auto& edge : stmt.pattern.edges) {
                    if (!edge.variable.empty()) new_vars.insert(edge.variable);
                }
                joined = left_outer_join(std::move(incoming), std::move(pattern_res), incoming.freevars(), new_vars);
            } else {
                joined = natural_join(std::move(incoming), std::move(pattern_res), limit);
            }
            return execute_match_chain_factorized(graph, matches, match_idx + 1, std::move(joined), limit, pruner);
        });
    } else {
        // Fallback: Flatten the incoming results and traverse the pattern relative to each row context.
        incoming.ensure_flat();
        if (limit > 0 && incoming.rows.size() > limit) {
            incoming.rows.resize(limit);
        }
        std::vector<seastar::future<std::vector<GqlRow>>> futs;
        for (const auto& row : incoming.rows) {
            futs.push_back(traverse_match_statement(graph, stmt, row, limit, pruner));
        }

        return seastar::when_all_succeed(futs.begin(), futs.end())
        .then([&graph, &matches, match_idx, incoming = std::move(incoming), limit, pruner](std::vector<std::vector<GqlRow>> nested) {
            std::vector<GqlRow> next_rows;
            for (const auto& vec : nested) {
                next_rows.insert(next_rows.end(), vec.begin(), vec.end());
                if (limit > 0 && next_rows.size() >= limit) {
                    next_rows.resize(limit);
                    break;
                }
            }
            return execute_match_chain_factorized(graph, matches, match_idx + 1, IntermediateResult(std::move(next_rows)), limit, pruner);
        });
    }
}

/**
 * @brief Helper to check if an expression contains aggregate functions.
 */
bool has_aggregates(const Expression* expr) {
    if (!expr) return false;
    if (expr->kind == ExpressionKind::AGGREGATION) return true;
    if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<const UnaryOpExpr*>(expr);
        return has_aggregates(un->expr.get());
    }
    if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<const BinaryOpExpr*>(expr);
        return has_aggregates(bin->left.get()) || has_aggregates(bin->right.get());
    }
    return false;
}

/**
 * @brief Recursively collects all pointers to AggregateExpr within an expression tree.
 */
void find_aggregates(const Expression* expr, std::vector<const AggregateExpr*>& aggregates) {
    if (!expr) return;
    if (expr->kind == ExpressionKind::AGGREGATION) {
        aggregates.push_back(static_cast<const AggregateExpr*>(expr));
        return;
    }
    if (expr->kind == ExpressionKind::UNARY_OP) {
        auto* un = static_cast<const UnaryOpExpr*>(expr);
        find_aggregates(un->expr.get(), aggregates);
    } else if (expr->kind == ExpressionKind::BINARY_OP) {
        auto* bin = static_cast<const BinaryOpExpr*>(expr);
        find_aggregates(bin->left.get(), aggregates);
        find_aggregates(bin->right.get(), aggregates);
    }
}

/**
 * @brief Evaluates an expression (including sub-expressions containing aggregates) for an aggregated group.
 * 
 * Uses pre-computed aggregate values for AggregateExpr nodes and falls back to evaluating
 * non-aggregate sub-expressions on the group's representative row.
 */
GqlValue evaluate_group_expression(const GqlRow& representative, const std::map<const AggregateExpr*, GqlValue>& aggregate_results, const Expression* expr) {
    if (!expr) return GqlValue();

    switch (expr->kind) {
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<const AggregateExpr*>(expr);
            auto it = aggregate_results.find(agg);
            if (it != aggregate_results.end()) {
                return it->second;
            }
            return GqlValue();
        }
        case ExpressionKind::LITERAL: {
            auto* lit = static_cast<const LiteralExpr*>(expr);
            return GqlValue(lit->value);
        }
        case ExpressionKind::VARIABLE: {
            auto* var = static_cast<const VariableExpr*>(expr);
            auto it = representative.bindings.find(var->name);
            if (it != representative.bindings.end()) {
                return it->second;
            }
            return GqlValue();
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* prop_lookup = static_cast<const PropertyLookupExpr*>(expr);
            auto it = representative.bindings.find(prop_lookup->variable);
            if (it != representative.bindings.end()) {
                const auto& val = it->second;
                if (val.type == GqlValue::NODE) {
                    return GqlValue(val.node->getProperty(prop_lookup->property));
                } else if (val.type == GqlValue::RELATIONSHIP) {
                    return GqlValue(val.relationship->getProperty(prop_lookup->property));
                }
            }
            return GqlValue();
        }
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<const UnaryOpExpr*>(expr);
            auto val = evaluate_group_expression(representative, aggregate_results, un->expr.get());
            if (un->op == UnaryOpKind::NOT) {
                return GqlValue(!val.is_truthy());
            } else if (un->op == UnaryOpKind::NEG) {
                if (val.type == GqlValue::PROPERTY) {
                    if (std::holds_alternative<int64_t>(val.property)) {
                        return GqlValue(-std::get<int64_t>(val.property));
                    }
                    if (std::holds_alternative<double>(val.property)) {
                        return GqlValue(-std::get<double>(val.property));
                    }
                }
                return GqlValue();
            }
            return GqlValue();
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<const BinaryOpExpr*>(expr);
            if (bin->op == BinaryOpKind::AND) {
                auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
                if (!lhs.is_truthy()) return GqlValue(false);
                auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }
            if (bin->op == BinaryOpKind::OR) {
                auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
                if (lhs.is_truthy()) return GqlValue(true);
                auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());
                return GqlValue(rhs.is_truthy());
            }

            auto lhs = evaluate_group_expression(representative, aggregate_results, bin->left.get());
            auto rhs = evaluate_group_expression(representative, aggregate_results, bin->right.get());

            if (lhs.type == GqlValue::NIL || rhs.type == GqlValue::NIL) {
                return GqlValue();
            }

            if (bin->op == BinaryOpKind::EQ) {
                return GqlValue(compare_gql_values(lhs, rhs) == 0);
            }
            if (bin->op == BinaryOpKind::NE) {
                return GqlValue(compare_gql_values(lhs, rhs) != 0);
            }
            if (bin->op == BinaryOpKind::LT) {
                return GqlValue(compare_gql_values(lhs, rhs) < 0);
            }
            if (bin->op == BinaryOpKind::LE) {
                return GqlValue(compare_gql_values(lhs, rhs) <= 0);
            }
            if (bin->op == BinaryOpKind::GT) {
                return GqlValue(compare_gql_values(lhs, rhs) > 0);
            }
            if (bin->op == BinaryOpKind::GE) {
                return GqlValue(compare_gql_values(lhs, rhs) >= 0);
            }

            if (lhs.type == GqlValue::PROPERTY && rhs.type == GqlValue::PROPERTY) {
                if (std::holds_alternative<int64_t>(lhs.property) && std::holds_alternative<int64_t>(rhs.property)) {
                    int64_t l = std::get<int64_t>(lhs.property);
                    int64_t r = std::get<int64_t>(rhs.property);
                    if (bin->op == BinaryOpKind::ADD) return GqlValue(l + r);
                    if (bin->op == BinaryOpKind::SUB) return GqlValue(l - r);
                    if (bin->op == BinaryOpKind::MUL) return GqlValue(l * r);
                    if (bin->op == BinaryOpKind::DIV) return r != 0 ? GqlValue(l / r) : GqlValue();
                }
                if (std::holds_alternative<double>(lhs.property) || std::holds_alternative<double>(rhs.property)) {
                    double l = std::holds_alternative<double>(lhs.property) ? std::get<double>(lhs.property) : static_cast<double>(std::get<int64_t>(lhs.property));
                    double r = std::holds_alternative<double>(rhs.property) ? std::get<double>(rhs.property) : static_cast<double>(std::get<int64_t>(rhs.property));
                    if (bin->op == BinaryOpKind::ADD) return GqlValue(l + r);
                    if (bin->op == BinaryOpKind::SUB) return GqlValue(l - r);
                    if (bin->op == BinaryOpKind::MUL) return GqlValue(l * r);
                    if (bin->op == BinaryOpKind::DIV) return r != 0.0 ? GqlValue(l / r) : GqlValue();
                }
                if (std::holds_alternative<std::string>(lhs.property) && std::holds_alternative<std::string>(rhs.property)) {
                    if (bin->op == BinaryOpKind::ADD) {
                        return GqlValue(std::get<std::string>(lhs.property) + std::get<std::string>(rhs.property));
                    }
                }
            }
            return GqlValue();
        }
    }
    return GqlValue();
}

/**
 * @brief Represents a grouped set of rows.
 */
struct GqlGroup {
    GqlRow representative;
    std::vector<GqlRow> rows;
};

/**
 * @brief Custom vector comparator for GqlValues to group mapping.
 */
struct GqlValueVectorLess {
    bool operator()(const std::vector<GqlValue>& a, const std::vector<GqlValue>& b) const {
        if (a.size() != b.size()) return a.size() < b.size();
        for (size_t i = 0; i < a.size(); ++i) {
            int cmp = compare_gql_values(a[i], b[i]);
            if (cmp != 0) return cmp < 0;
        }
        return false;
    }
};

/**
 * @brief Helper sorting structure representing a row mapped to its sort keys.
 */
struct RowSortKey {
    std::vector<GqlValue> keys;
    GqlRow row;
};

/**
 * @brief Main execution entry point for the GQL engine.
 * 
 * Performs MATCH chain query paths, applies global WHERE filtering, applies side-effects
 * (writes), sorts the final results via ORDER BY, serializes return projected columns
 * to formatted JSON arrays, and handles limits.
 * 
 * @param graph The RageDB graph instance.
 * @param query_val The parsed GQL Query configuration.
 * @return seastar::future<std::string> Future containing serialized JSON array results.
 */
struct QueryResult {
    std::vector<std::string> column_names;
    std::vector<std::vector<GqlValue>> rows;
};

/**
 * @brief Sorts the combined query results.
 * 
 * If a limit is present, utilizes std::partial_sort to only sort the top-K elements
 * in O(N log K) time, avoiding the overhead of sorting the entire vector.
 */
static void sort_combined_result(QueryResult& res, const std::vector<SortSpec>& order_by, std::optional<size_t> limit = std::nullopt) {
    if (order_by.empty()) return;
    
    auto comp = [&res, &order_by](const std::vector<GqlValue>& a, const std::vector<GqlValue>& b) {
        for (const auto& spec : order_by) {
            size_t col_idx = res.column_names.size();
            if (spec.expr->kind == ExpressionKind::VARIABLE) {
                auto* ve = static_cast<const VariableExpr*>(spec.expr.get());
                for (size_t i = 0; i < res.column_names.size(); ++i) {
                    if (res.column_names[i] == ve->name) {
                        col_idx = i;
                        break;
                    }
                }
            } else if (spec.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                auto* pl = static_cast<const PropertyLookupExpr*>(spec.expr.get());
                std::string col_name = pl->variable + "." + pl->property;
                for (size_t i = 0; i < res.column_names.size(); ++i) {
                    if (res.column_names[i] == col_name) {
                        col_idx = i;
                        break;
                    }
                }
            }
            
            if (col_idx < res.column_names.size()) {
                int cmp = compare_gql_values(a[col_idx], b[col_idx]);
                if (cmp != 0) {
                    return spec.ascending ? (cmp < 0) : (cmp > 0);
                }
            }
        }
        return false;
    };

    // Optimization: If there is a limit, perform a partial sort to keep only the top-K rows.
    if (limit && *limit < res.rows.size()) {
        std::partial_sort(res.rows.begin(), res.rows.begin() + *limit, res.rows.end(), comp);
        res.rows.resize(*limit);
    } else {
        std::stable_sort(res.rows.begin(), res.rows.end(), comp);
    }
}

static seastar::future<QueryResult> execute_query_internal(ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr) {
    if (query_ptr->kind != QueryKind::SINGLE) {
        auto left_ptr = std::shared_ptr<GqlQuery>(query_ptr->left.release());
        auto right_ptr = std::shared_ptr<GqlQuery>(query_ptr->right.release());

        return execute_query_internal(graph, left_ptr)
        .then([&graph, right_ptr, query_ptr](QueryResult left_res) {
            return execute_query_internal(graph, right_ptr)
            .then([left_res = std::move(left_res), query_ptr](QueryResult right_res) {
                if (left_res.column_names.size() != right_res.column_names.size()) {
                    throw std::runtime_error("All subqueries in a GQL Set operation must return the same number of columns");
                }

                QueryResult combined_res;
                combined_res.column_names = left_res.column_names;

                if (query_ptr->kind == QueryKind::UNION_ALL) {
                    combined_res.rows = std::move(left_res.rows);
                    for (auto& r : right_res.rows) {
                        combined_res.rows.push_back(std::move(r));
                    }
                } else if (query_ptr->kind == QueryKind::UNION) {
                    std::set<std::vector<GqlValue>, GqlValueVectorLess> seen;
                    for (auto& r : left_res.rows) {
                        if (seen.insert(r).second) {
                            combined_res.rows.push_back(std::move(r));
                        }
                    }
                    for (auto& r : right_res.rows) {
                        if (seen.insert(r).second) {
                            combined_res.rows.push_back(std::move(r));
                        }
                    }
                } else if (query_ptr->kind == QueryKind::INTERSECT) {
                    std::set<std::vector<GqlValue>, GqlValueVectorLess> left_set;
                    for (const auto& r : left_res.rows) {
                        left_set.insert(r);
                    }
                    std::set<std::vector<GqlValue>, GqlValueVectorLess> seen_intersection;
                    for (auto& r : right_res.rows) {
                        if (left_set.count(r)) {
                            if (seen_intersection.insert(r).second) {
                                combined_res.rows.push_back(std::move(r));
                            }
                        }
                    }
                } else if (query_ptr->kind == QueryKind::INTERSECT_ALL) {
                    std::map<std::vector<GqlValue>, int64_t, GqlValueVectorLess> left_counts;
                    for (const auto& r : left_res.rows) {
                        left_counts[r]++;
                    }
                    for (auto& r : right_res.rows) {
                        auto it = left_counts.find(r);
                        if (it != left_counts.end() && it->second > 0) {
                            combined_res.rows.push_back(std::move(r));
                            it->second--;
                        }
                    }
                }

                return combined_res;
            });
        });
    }

    bool query_contains_aggregates = false;
    for (const auto& item : query_ptr->returns) {
        if (has_aggregates(item.expr.get())) {
            query_contains_aggregates = true;
            break;
        }
    }
    if (!query_contains_aggregates) {
        for (const auto& spec : query_ptr->order_by) {
            if (has_aggregates(spec.expr.get())) {
                query_contains_aggregates = true;
                break;
            }
        }
    }

    size_t limit_val = 0;
    if (query_ptr->limit.has_value() && query_ptr->order_by.empty() && !query_contains_aggregates) {
        limit_val = *query_ptr->limit;
    }

    ProjectionPruner pruner;

    // 1. Analyze returns
    for (const auto& item : query_ptr->returns) {
        collect_accessed_properties(item.expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    // 2. Analyze order_by
    for (const auto& spec : query_ptr->order_by) {
        collect_accessed_properties(spec.expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    // 3. Analyze where_expr
    if (query_ptr->where_expr) {
        collect_accessed_properties(query_ptr->where_expr.get(), pruner.accessed_props, pruner.whole_objects);
    }

    // 4. Analyze writes
    for (const auto& w : query_ptr->writes) {
        if (w.type == WriteOp::Type::INSERT) {
            for (const auto& n : w.insert_pattern.nodes) {
                if (!n.variable.empty()) {
                    pruner.whole_objects.insert(n.variable);
                }
            }
            for (const auto& e : w.insert_pattern.edges) {
                if (!e.variable.empty()) {
                    pruner.whole_objects.insert(e.variable);
                }
            }
        } else if (w.type == WriteOp::Type::SET) {
            if (!w.set_var.empty()) {
                pruner.whole_objects.insert(w.set_var);
            }
            collect_accessed_properties(w.set_expr.get(), pruner.accessed_props, pruner.whole_objects);
        } else if (w.type == WriteOp::Type::REMOVE) {
            if (!w.remove_var.empty()) {
                pruner.whole_objects.insert(w.remove_var);
            }
        } else if (w.type == WriteOp::Type::DELETE_OP) {
            if (!w.delete_var.empty()) {
                pruner.whole_objects.insert(w.delete_var);
            }
        }
    }

    // 5. Collect inline properties and filters from patterns
    for (const auto& match : query_ptr->matches) {
        for (const auto& n : match.pattern.nodes) {
            if (!n.variable.empty()) {
                for (const auto& [prop, val] : n.properties) {
                    pruner.accessed_props[n.variable].insert(prop);
                }
                for (const auto& filter : n.property_filters) {
                    pruner.accessed_props[n.variable].insert(filter.property);
                }
            }
        }
        for (const auto& e : match.pattern.edges) {
            if (!e.variable.empty()) {
                for (const auto& [prop, val] : e.properties) {
                    pruner.accessed_props[e.variable].insert(prop);
                }
                for (const auto& filter : e.property_filters) {
                    pruner.accessed_props[e.variable].insert(filter.property);
                }
            }
        }
    }

    IntermediateResult initial_res(std::vector<GqlRow>{ GqlRow{} });

    return execute_match_chain_factorized(graph, query_ptr->matches, 0, std::move(initial_res), limit_val, pruner)
    .then([&graph, query_ptr](IntermediateResult matched_res) {
        matched_res.ensure_flat();
        std::vector<GqlRow> matched_rows = std::move(matched_res.rows);
        const auto& query = *query_ptr;
        // Filter rows using global WHERE condition
        std::vector<GqlRow> filtered_rows;
        for (auto& row : matched_rows) {
            if (!query.where_expr || evaluate_expression(row, query.where_expr.get()).is_truthy()) {
                filtered_rows.push_back(std::move(row));
            }
        }

        // Execute write operations if any
        if (query.writes.empty()) {
            return seastar::make_ready_future<std::vector<GqlRow>>(std::move(filtered_rows));
        }

        std::vector<seastar::future<GqlRow>> futs;
        for (auto& row : filtered_rows) {
            futs.push_back(execute_writes_for_row(graph, query_ptr, 0, std::move(row)));
        }
        return seastar::when_all_succeed(futs.begin(), futs.end());
    })
    .then([query_ptr](std::vector<GqlRow> written_rows) {
        const auto& query = *query_ptr;
        std::vector<GqlRow> filtered_rows = std::move(written_rows);

        // Detect if there are aggregate expressions anywhere in projected items or order by spec
        bool contains_aggregates = false;
        for (const auto& item : query.returns) {
            if (has_aggregates(item.expr.get())) {
                contains_aggregates = true;
                break;
            }
        }
        if (!contains_aggregates) {
            for (const auto& spec : query.order_by) {
                if (has_aggregates(spec.expr.get())) {
                    contains_aggregates = true;
                    break;
                }
            }
        }

        QueryResult query_res;

        // Resolve column names
        for (size_t i = 0; i < query.returns.size(); ++i) {
            const auto& item = query.returns[i];
            std::string key;
            if (item.alias) {
                key = *item.alias;
            } else {
                if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                    auto* pl = static_cast<const PropertyLookupExpr*>(item.expr.get());
                    key = pl->variable + "." + pl->property;
                } else if (item.expr->kind == ExpressionKind::VARIABLE) {
                    auto* ve = static_cast<const VariableExpr*>(item.expr.get());
                    key = ve->name;
                } else if (item.expr->kind == ExpressionKind::AGGREGATION) {
                    auto* ae = static_cast<const AggregateExpr*>(item.expr.get());
                    std::string fn_name;
                    if (ae->fn_kind == AggregateKind::COUNT) fn_name = "count";
                    else if (ae->fn_kind == AggregateKind::SUM) fn_name = "sum";
                    else if (ae->fn_kind == AggregateKind::AVG) fn_name = "avg";
                    else if (ae->fn_kind == AggregateKind::MIN) fn_name = "min";
                    else fn_name = "max";

                    if (!ae->expr) {
                        key = fn_name + "(*)";
                    } else if (ae->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                        auto* pl = static_cast<const PropertyLookupExpr*>(ae->expr.get());
                        key = fn_name + "(" + pl->variable + "." + pl->property + ")";
                    } else if (ae->expr->kind == ExpressionKind::VARIABLE) {
                        auto* ve = static_cast<const VariableExpr*>(ae->expr.get());
                        key = fn_name + "(" + ve->name + ")";
                    } else {
                        key = fn_name + "(expr)";
                    }
                } else {
                    key = "column_" + std::to_string(i);
                }
            }
            query_res.column_names.push_back(key);
        }

        std::unordered_set<std::string> seen_distinct;

        if (contains_aggregates) {
            std::vector<const AggregateExpr*> aggregate_exprs;
            for (const auto& item : query.returns) {
                find_aggregates(item.expr.get(), aggregate_exprs);
            }
            for (const auto& spec : query.order_by) {
                find_aggregates(spec.expr.get(), aggregate_exprs);
            }

            // Extract and optimize grouping keys.
            // If a node/relationship variable is present as a grouping key, we prune any of its property lookups
            // from the grouping keys list because they are functionally dependent on the variable.
            std::set<std::string> group_variables;
            for (const auto& item : query.returns) {
                if (item.expr && !has_aggregates(item.expr.get())) {
                    if (item.expr->kind == ExpressionKind::VARIABLE) {
                        group_variables.insert(static_cast<const VariableExpr*>(item.expr.get())->name);
                    }
                }
            }

            std::vector<const Expression*> grouping_keys;
            for (const auto& item : query.returns) {
                if (item.expr && !has_aggregates(item.expr.get())) {
                    if (item.expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                        auto* pl = static_cast<const PropertyLookupExpr*>(item.expr.get());
                        if (group_variables.count(pl->variable)) {
                            // Skip functionally dependent property lookup to reduce hashing/comparison overhead.
                            continue;
                        }
                    }
                    grouping_keys.push_back(item.expr.get());
                }
            }

            std::map<std::vector<GqlValue>, GqlGroup, GqlValueVectorLess> groups;
            for (auto& row : filtered_rows) {
                std::vector<GqlValue> key;
                for (const auto* gk : grouping_keys) {
                    key.push_back(evaluate_expression(row, gk));
                }
                auto& group = groups[key];
                if (group.rows.empty()) {
                    group.representative = row;
                }
                group.rows.push_back(std::move(row));
            }

            if (groups.empty() && grouping_keys.empty()) {
                GqlGroup default_group;
                default_group.representative = GqlRow{};
                groups[{}] = default_group;
            }

            struct GroupSortKey {
                std::vector<GqlValue> sort_keys;
                std::vector<GqlValue> projected_row;
            };
            std::vector<GroupSortKey> sorted_groups;

            for (const auto& [key, group] : groups) {
                std::map<const AggregateExpr*, GqlValue> aggregate_results;
                for (const auto* agg : aggregate_exprs) {
                    if (agg->fn_kind == AggregateKind::COUNT) {
                        int64_t count = 0;
                        if (!agg->expr) {
                            count = static_cast<int64_t>(group.rows.size());
                        } else {
                            for (const auto& r : group.rows) {
                                GqlValue v = evaluate_expression(r, agg->expr.get());
                                if (v.type != GqlValue::NIL) {
                                    count++;
                                }
                            }
                        }
                        aggregate_results[agg] = GqlValue(count);
                    } else if (agg->fn_kind == AggregateKind::SUM || agg->fn_kind == AggregateKind::AVG) {
                        int64_t sum_int = 0;
                        double sum_double = 0.0;
                        bool has_double = false;
                        int64_t count = 0;

                        for (const auto& r : group.rows) {
                            GqlValue v = evaluate_expression(r, agg->expr.get());
                            if (v.type == GqlValue::PROPERTY) {
                                if (std::holds_alternative<int64_t>(v.property)) {
                                    if (has_double) sum_double += static_cast<double>(std::get<int64_t>(v.property));
                                    else sum_int += std::get<int64_t>(v.property);
                                    count++;
                                } else if (std::holds_alternative<double>(v.property)) {
                                    if (!has_double) {
                                        sum_double = static_cast<double>(sum_int);
                                        has_double = true;
                                    }
                                    sum_double += std::get<double>(v.property);
                                    count++;
                                }
                            }
                        }

                        if (agg->fn_kind == AggregateKind::SUM) {
                            if (count == 0) {
                                aggregate_results[agg] = GqlValue();
                            } else if (has_double) {
                                aggregate_results[agg] = GqlValue(sum_double);
                            } else {
                                aggregate_results[agg] = GqlValue(sum_int);
                            }
                        } else {
                            if (count == 0) {
                                aggregate_results[agg] = GqlValue();
                            } else if (has_double) {
                                aggregate_results[agg] = GqlValue(sum_double / static_cast<double>(count));
                            } else {
                                aggregate_results[agg] = GqlValue(static_cast<double>(sum_int) / static_cast<double>(count));
                            }
                        }
                    } else if (agg->fn_kind == AggregateKind::MIN) {
                        GqlValue min_val;
                        for (const auto& r : group.rows) {
                            GqlValue v = evaluate_expression(r, agg->expr.get());
                            if (v.type != GqlValue::NIL) {
                                if (min_val.type == GqlValue::NIL || compare_gql_values(v, min_val) < 0) {
                                    min_val = v;
                                }
                            }
                        }
                        aggregate_results[agg] = min_val;
                    } else if (agg->fn_kind == AggregateKind::MAX) {
                        GqlValue max_val;
                        for (const auto& r : group.rows) {
                            GqlValue v = evaluate_expression(r, agg->expr.get());
                            if (v.type != GqlValue::NIL) {
                                if (max_val.type == GqlValue::NIL || compare_gql_values(v, max_val) > 0) {
                                    max_val = v;
                                }
                            }
                        }
                        aggregate_results[agg] = max_val;
                    }
                }

                std::vector<GqlValue> projected;
                for (size_t i = 0; i < query.returns.size(); ++i) {
                    const auto& item = query.returns[i];
                    projected.push_back(evaluate_group_expression(group.representative, aggregate_results, item.expr.get()));
                }

                GroupSortKey gsk;
                gsk.projected_row = std::move(projected);
                for (const auto& spec : query.order_by) {
                    gsk.sort_keys.push_back(evaluate_group_expression(group.representative, aggregate_results, spec.expr.get()));
                }
                sorted_groups.push_back(std::move(gsk));
            }

            // Sort the aggregated groups using Top-K optimization if a limit is present.
            if (!query.order_by.empty()) {
                auto comp = [&query](const GroupSortKey& a, const GroupSortKey& b) {
                    for (size_t i = 0; i < query.order_by.size(); ++i) {
                        int cmp = compare_gql_values(a.sort_keys[i], b.sort_keys[i]);
                        if (cmp != 0) {
                            return query.order_by[i].ascending ? (cmp < 0) : (cmp > 0);
                        }
                    }
                    return false;
                };
                
                // If a limit is present, use std::partial_sort to only sort the top-K aggregate groups.
                if (query.limit && *query.limit < sorted_groups.size()) {
                    std::partial_sort(sorted_groups.begin(), sorted_groups.begin() + *query.limit, sorted_groups.end(), comp);
                    sorted_groups.resize(*query.limit);
                } else {
                    std::stable_sort(sorted_groups.begin(), sorted_groups.end(), comp);
                }
            }

            for (const auto& gsk : sorted_groups) {
                if (query.distinct) {
                    std::string serialized_distinct;
                    for (const auto& v : gsk.projected_row) {
                        serialized_distinct += serialize_gql_value(v) + ",";
                    }
                    if (seen_distinct.insert(serialized_distinct).second) {
                        query_res.rows.push_back(gsk.projected_row);
                    }
                } else {
                    query_res.rows.push_back(gsk.projected_row);
                }
            }
        } else {
            // Sort flat results using Top-K optimization if a limit is present.
            if (!query.order_by.empty()) {
                std::vector<RowSortKey> sorted_keys;
                for (auto& row : filtered_rows) {
                    RowSortKey rk;
                    rk.row = row;
                    for (const auto& spec : query.order_by) {
                        rk.keys.push_back(evaluate_expression(row, spec.expr.get()));
                    }
                    sorted_keys.push_back(std::move(rk));
                }

                auto comp = [&query](const RowSortKey& a, const RowSortKey& b) {
                    for (size_t i = 0; i < query.order_by.size(); ++i) {
                        int cmp = compare_gql_values(a.keys[i], b.keys[i]);
                        if (cmp != 0) {
                            return query.order_by[i].ascending ? (cmp < 0) : (cmp > 0);
                        }
                    }
                    return false;
                };

                // If a limit is present, use std::partial_sort to keep only the top-K keys.
                if (query.limit && *query.limit < sorted_keys.size()) {
                    std::partial_sort(sorted_keys.begin(), sorted_keys.begin() + *query.limit, sorted_keys.end(), comp);
                    sorted_keys.resize(*query.limit);
                } else {
                    std::stable_sort(sorted_keys.begin(), sorted_keys.end(), comp);
                }

                filtered_rows.clear();
                for (auto& rk : sorted_keys) {
                    filtered_rows.push_back(std::move(rk.row));
                }
            }

            for (const auto& row : filtered_rows) {
                std::vector<GqlValue> projected;
                for (size_t i = 0; i < query.returns.size(); ++i) {
                    const auto& item = query.returns[i];
                    projected.push_back(evaluate_expression(row, item.expr.get()));
                }

                if (query.distinct) {
                    std::string serialized_distinct;
                    for (const auto& v : projected) {
                        serialized_distinct += serialize_gql_value(v) + ",";
                    }
                    if (seen_distinct.insert(serialized_distinct).second) {
                        query_res.rows.push_back(std::move(projected));
                    }
                } else {
                    query_res.rows.push_back(std::move(projected));
                }
            }
        }

        if (query.limit && query_res.rows.size() > *query.limit) {
            query_res.rows.resize(*query.limit);
        }

        return seastar::make_ready_future<QueryResult>(std::move(query_res));
    });
}

seastar::future<std::string> GqlExecutor::execute(ragedb::Graph& graph, GqlQuery query_val) {
    GqlTypechecker::typecheck(graph, query_val);

    if (query_val.schema_op.has_value()) {
        return GqlCatalog::execute_schema_op(graph, *query_val.schema_op);
    }

    auto query_ptr = std::make_shared<GqlQuery>(std::move(query_val));

    return execute_query_internal(graph, query_ptr)
    .then([query_ptr](QueryResult result) {
        const auto& query = *query_ptr;

        // Sort the combined result and apply Top-K optimization if limit is present.
        if (!query.order_by.empty()) {
            sort_combined_result(result, query.order_by, query.limit);
        }

        if (query.limit && result.rows.size() > *query.limit) {
            result.rows.resize(*query.limit);
        }

        std::string json_res = "[";
        bool first_row = true;
        for (const auto& row : result.rows) {
            if (!first_row) json_res += ", ";
            json_res += "{";
            bool first_col = true;
            for (size_t i = 0; i < result.column_names.size(); ++i) {
                if (!first_col) json_res += ", ";
                json_res += "\"" + result.column_names[i] + "\": " + serialize_gql_value(row[i]);
                first_col = false;
            }
            json_res += "}";
            first_row = false;
        }
        json_res += "]";

        return seastar::make_ready_future<std::string>(json_res);
    });
}

} // namespace ragedb::gql

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

#include "FactorNode.h"
#include "JoinHelpers.h"
#include <algorithm>

/**
 * @file FactorNode.cpp
 * @brief Logic for creating, partitioning, joining, and flattening factorized nodes.
 * 
 * Example Query utilizing factorized trees (PRODUCT, UNION, FLAT):
 *   MATCH (a)-[:FRIEND]->(b) MATCH (b)-[:KNOWS]->(c) MATCH (b)-[:FRIEND]->(d)
 *   RETURN a, b, c, d
 *   - The results of a-b, b-c, b-d match branches are partitioned on the central variable "b"
 *     using `FactorNode::partition`.
 *   - Branches are combined via `FactorNode::join` into nested sub-trees, avoiding early Cartesian products.
 *   - Finally, `FactorNode::flatten` generates flat GqlRows for projection / output.
 */
namespace ragedb::gql {

bool GqlValueLess::operator()(const GqlValue& a, const GqlValue& b) const {
    return compare_gql_values(a, b) < 0;
}

FactorNode::FactorNode(FactorNodeType t) : type(t) {}
FactorNode::FactorNode(std::vector<GqlRow> rows) : type(FactorNodeType::FLAT), flat_rows(std::move(rows)) {}

/**
 * @brief Checks if this FactorNode represents an empty set of rows.
 */
bool FactorNode::is_empty() const {
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
 * @brief Computes the set of bound user variables in this sub-tree.
 * 
 * Excludes internal path matching trackers starting with "_n_" or "_e_".
 */
std::set<std::string> FactorNode::freevars() const {
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
 * @brief Partitions this factorized node on the value of a given join variable.
 * 
 * Returns a map of GQL values to factorized sub-nodes, enabling factorized joins.
 */
std::map<GqlValue, FactorNodePtr, GqlValueLess> FactorNode::partition(const std::string& var) const {
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
                target_idx = static_cast<int>(i);
                break;
            }
        }
        if (target_idx != -1) {
            auto target_node = children[static_cast<size_t>(target_idx)];
            auto target_parts = target_node->partition(var);
            for (auto& [val, part] : target_parts) {
                auto product_node = std::make_shared<FactorNode>(type);
                product_node->children = children;
                product_node->children[static_cast<size_t>(target_idx)] = part;
                partition_map[val] = product_node;
            }
        }
    }
    return partition_map;
}

/**
 * @brief Joins this factorized node with another factorized node on a shared join variable.
 * 
 * Computes partitions on both sides, intersects the matching values, and builds
 * a UNION of PRODUCT sub-nodes.
 */
FactorNodePtr FactorNode::join(const FactorNodePtr& other, const std::string& join_var) const {
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
 * @brief Flattens this factorized sub-tree back into a standard vector of flat GqlRows.
 */
std::vector<GqlRow> FactorNode::flatten() const {
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

IntermediateResult::IntermediateResult() {
    root = std::make_shared<FactorNode>(std::vector<GqlRow>{});
}

IntermediateResult::IntermediateResult(std::vector<GqlRow> r) : rows(r) {
    root = std::make_shared<FactorNode>(std::move(r));
}

IntermediateResult::IntermediateResult(FactorNodePtr node) : root(std::move(node)) {}

IntermediateResult IntermediateResult::empty() {
    return IntermediateResult(std::vector<GqlRow>{});
}

/**
 * @brief Ensures the flat row representation is populated by flattening the factorized root if necessary.
 */
void IntermediateResult::ensure_flat() {
    if (rows.empty() && root) {
        rows = root->flatten();
    }
}

FactorNodePtr IntermediateResult::into_root() const {
    if (root) return root;
    return std::make_shared<FactorNode>(rows);
}

bool IntermediateResult::is_empty() const {
    if (root) return root->is_empty();
    return rows.empty();
}

std::set<std::string> IntermediateResult::freevars() const {
    if (root) return root->freevars();
    std::set<std::string> vars;
    if (!rows.empty()) {
        for (const auto& [k, v] : rows[0].bindings) {
            vars.insert(k);
        }
    }
    return vars;
}

} // namespace ragedb::gql

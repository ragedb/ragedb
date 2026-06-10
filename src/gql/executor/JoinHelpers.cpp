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

#include "JoinHelpers.h"
#include <variant>
#include <algorithm>

namespace ragedb::gql {

size_t PropertyHash::operator()(const property_type_t& val) const {
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

size_t GqlValueHash::operator()(const GqlValue& val) const {
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

size_t GqlValueVectorHash::operator()(const std::vector<GqlValue>& vec) const {
    size_t seed = vec.size();
    GqlValueHash hasher;
    for (const auto& val : vec) {
        seed ^= hasher(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}

bool GqlValueVectorEqual::operator()(const std::vector<GqlValue>& a, const std::vector<GqlValue>& b) const {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (compare_gql_values(a[i], b[i]) != 0) return false;
    }
    return true;
}

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

IntermediateResult natural_join(IntermediateResult left, IntermediateResult right, size_t limit) {
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
        combined.is_sorted = left.is_sorted;
        if (limit > 0) {
            combined.ensure_flat();
            if (combined.rows.size() > limit) {
                combined.rows.resize(limit);
            }
        }
        return combined;
    }
}

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
            auto combined = IntermediateResult(factorized);
            combined.is_sorted = left.is_sorted;
            return combined;
        } else {
            auto factorized = std::make_shared<FactorNode>(FactorNodeType::PRODUCT);
            factorized->children.push_back(left.into_root());
            factorized->children.push_back(right.into_root());
            auto combined = IntermediateResult(factorized);
            combined.is_sorted = left.is_sorted;
            return combined;
        }
    }
}

} // namespace ragedb::gql

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

#ifndef RAGEDB_FACTOR_NODE_H
#define RAGEDB_FACTOR_NODE_H

#include <vector>
#include <string>
#include <set>
#include <map>
#include <memory>
#include "GqlAst.h"
#include "GqlValue.h"

/**
 * @brief Classes representing factorized execution trees, nesting, and sub-trees.
 * 
 * Example Query utilizing FactorNode / factorized representation:
 *   MATCH (a)-[:FRIEND]->(b) MATCH (b)-[:KNOWS]->(c) MATCH (b)-[:FRIEND]->(d)
 *   RETURN a, b, c, d
 * 
 * Here, "b" is a central variable. Instead of generating a flat Cartesian product
 * of size O(A * C * D), we construct a FactorNode of type PRODUCT containing
 * children for the disjoint paths, reducing intermediate storage and processing cost.
 */
namespace ragedb::gql {

enum class FactorNodeType {
    FLAT,         ///< Flat vector of GqlRow records.
    PRODUCT,      ///< Cartesian product of child sub-trees (lazily joined).
    PATH_CONCAT,  ///< Path concatenation of nodes/edges.
    UNION         ///< Disjoint union of matching options.
};

struct GqlValueLess {
    bool operator()(const GqlValue& a, const GqlValue& b) const;
};

struct FactorNode;
using FactorNodePtr = std::shared_ptr<FactorNode>;

struct FactorNode {
    FactorNodeType type;
    std::vector<GqlRow> flat_rows;
    std::vector<FactorNodePtr> children;

    FactorNode(FactorNodeType t);
    FactorNode(std::vector<GqlRow> rows);

    bool is_empty() const;
    std::set<std::string> freevars() const;
    std::map<GqlValue, FactorNodePtr, GqlValueLess> partition(const std::string& var) const;
    FactorNodePtr join(const FactorNodePtr& other, const std::string& join_var) const;
    std::vector<GqlRow> flatten() const;
};

struct IntermediateResult {
    std::vector<GqlRow> rows;
    FactorNodePtr root;
    bool is_sorted = false;

    IntermediateResult();
    IntermediateResult(std::vector<GqlRow> r);
    IntermediateResult(FactorNodePtr node);

    static IntermediateResult empty();
    void ensure_flat();
    FactorNodePtr into_root() const;
    bool is_empty() const;
    std::set<std::string> freevars() const;
};

} // namespace ragedb::gql

#endif // RAGEDB_FACTOR_NODE_H

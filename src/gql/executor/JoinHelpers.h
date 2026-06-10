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

#ifndef RAGEDB_JOIN_HELPERS_H
#define RAGEDB_JOIN_HELPERS_H

#include <vector>
#include <string>
#include <set>
#include <unordered_map>
#include "GqlValue.h"
#include "FactorNode.h"

/**
 * @brief Helper functions and functors for performing natural joins and left outer joins.
 * 
 * Example Queries utilizing JoinHelpers:
 * 
 * 1. Accumulator Hash Join / natural_join:
 *    MATCH (a)-[:FRIEND]->(b) MATCH (b)-[:KNOWS]->(c)
 *    RETURN a, b, c
 *    Here, "b" is a shared join variable. JoinHelpers hashes the smaller side
 *    on "b" and probes it with the larger side to perform a natural join in O(N+M) time.
 * 
 * 2. Left Outer Join (Unnested Correlated Subqueries):
 *    MATCH (a:Person) WHERE EXISTS { MATCH (a)-[:FRIEND]->(b) WHERE b.age > 30 }
 *    After optimizer unnesting, this translates to:
 *    MATCH (a:Person) OPTIONAL MATCH (a)-[:FRIEND]->(b) WHERE b.age > 30
 *    JoinHelpers performs a left_outer_join on variable "a", keeping "a" even if
 *    no matching friend "b" exists, then validates existence based on whether "b" is bound.
 */
namespace ragedb::gql {

struct PropertyHash {
    size_t operator()(const property_type_t& val) const;
};

struct GqlValueHash {
    size_t operator()(const GqlValue& val) const;
};

struct GqlValueVectorHash {
    size_t operator()(const std::vector<GqlValue>& vec) const;
};

struct GqlValueVectorEqual {
    bool operator()(const std::vector<GqlValue>& a, const std::vector<GqlValue>& b) const;
};

std::vector<GqlRow> join_flat_rows_variable(const std::vector<GqlRow>& left, const std::vector<GqlRow>& right);

IntermediateResult natural_join(IntermediateResult left, IntermediateResult right, size_t limit = 0);

IntermediateResult left_outer_join(
    IntermediateResult left,
    IntermediateResult right,
    const std::set<std::string>& bound_vars,
    const std::set<std::string>& new_vars
);

} // namespace ragedb::gql

#endif // RAGEDB_JOIN_HELPERS_H

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

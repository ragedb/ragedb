/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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


#include "../Shard.h"

namespace ragedb {

    sol::as_table_t<std::vector<uint64_t>> Shard::AllNodeIdsViaLua(uint64_t skip, uint64_t limit) {
        return sol::as_table(AllNodeIdsPeered(skip, limit).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::AllNodeIdsForTypeViaLua(const std::string& type, uint64_t skip, uint64_t limit) {
        return sol::as_table(AllNodeIdsPeered(type, skip, limit).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::AllRelationshipIdsViaLua(uint64_t skip, uint64_t limit) {
        return sol::as_table(AllRelationshipIdsPeered(skip, limit).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::AllRelationshipIdsForTypeViaLua(const std::string& rel_type, uint64_t skip, uint64_t limit) {
        return sol::as_table(AllRelationshipIdsPeered(rel_type, skip, limit).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::AllNodesViaLua(uint64_t skip, uint64_t limit) {
        return sol::as_table(AllNodesPeered(skip, limit).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::AllNodesForTypeViaLua(const std::string& type, uint64_t skip, uint64_t limit) {
        return sol::as_table(AllNodesPeered(type, skip, limit).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::AllRelationshipsViaLua(uint64_t skip, uint64_t limit) {
        return sol::as_table(AllRelationshipsPeered(skip, limit).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::AllRelationshipsForTypeViaLua(const std::string& rel_type, uint64_t skip, uint64_t limit) {
        return sol::as_table(AllRelationshipsPeered(rel_type, skip, limit).get0());
    }

}
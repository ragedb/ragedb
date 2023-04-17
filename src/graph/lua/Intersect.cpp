/*
 * Copyright Max De Marzi. All Rights Reserved.
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

    uint64_t Shard::IntersectIdsCountViaLua(std::vector<uint64_t> ids1, std::vector<uint64_t> ids2) {
        return IntersectIds(ids1, ids2).size();
    }

    uint64_t Shard::IntersectNodesCountViaLua(std::vector<Node> ids1, std::vector<Node> ids2) {

        return IntersectNodesPeered(ids1, ids2).get0().size();
    }

    uint64_t Shard::IntersectRelationshipsCountViaLua(std::vector<Relationship> ids1, std::vector<Relationship> ids2) {
        return IntersectRelationshipsPeered(ids1, ids2).get0().size();
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::IntersectIdsViaLua(std::vector<uint64_t> ids1, std::vector<uint64_t> ids2) {
        return sol::as_table(IntersectIds(ids1, ids2));
    }

    sol::as_table_t<std::vector<Node>> Shard::IntersectNodesViaLua(std::vector<Node> ids1, std::vector<Node> ids2) {

        return sol::as_table(IntersectNodesPeered(ids1, ids2).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::IntersectRelationshipsViaLua(std::vector<Relationship> ids1, std::vector<Relationship> ids2) {
        return sol::as_table(IntersectRelationshipsPeered(ids1, ids2).get0());
    }
}
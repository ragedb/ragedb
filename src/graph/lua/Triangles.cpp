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

    uint64_t Shard::TriangleCount(const std::string& rel_type) {
        uint64_t count = 0;
        auto max = AllNodesCountPeered().get0();
        auto a = AllNodeIdsPeered(uint64_t(0), max).get0();
        auto outs = NodeIdsGetNeighborIdsPeered(a, Direction::OUT, rel_type).get0();
        auto ins = NodeIdsGetNeighborIdsPeered(a, Direction::IN, rel_type).get0();

        for (const auto &node_a : a) {
            auto bs = outs[node_a];
            auto cs = ins[node_a];
            for (const auto &node_b : bs) {
                auto cs2 = outs[node_b];
                count += IntersectIds(cs, cs2).size();
            }
        }

        return count;
    }

    uint64_t Shard::TriangleCount(const std::vector<std::string> &rel_types) {
        uint64_t count = 0;
        auto max = AllNodesCountPeered().get0();
        auto a = AllNodeIdsPeered(uint64_t(0), max).get0();
        auto outs = NodeIdsGetNeighborIdsPeered(a, Direction::OUT, rel_types).get0();
        auto ins = NodeIdsGetNeighborIdsPeered(a, Direction::IN, rel_types).get0();

        for (const auto &node_a : a) {
            auto bs = outs[node_a];
            auto cs = ins[node_a];
            for (const auto &node_b : bs) {
                auto cs2 = outs[node_b];
                count += IntersectIds(cs, cs2).size();
            }
        }

        return count;
    }
}
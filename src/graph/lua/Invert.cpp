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

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::InvertViaLua(const std::map<uint64_t, std::vector<uint64_t>> &map) {
        std::map<uint64_t, std::vector<uint64_t>> inverted;
        for (const auto& [node_id, neighbor_ids] : map) {
            for (const auto& neighbor_id : neighbor_ids) {
                inverted[neighbor_id].emplace_back(node_id);
            }
        }
        return inverted;
    }
}
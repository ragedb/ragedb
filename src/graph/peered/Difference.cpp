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

    std::vector<uint64_t> DifferenceIds(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
        std::vector<uint64_t> first = ids1;
        std::vector<uint64_t> difference = ids2;
        std::sort(first.begin(), first.end());
        std::sort(difference.begin(), difference.end());

        auto end = std::set_difference(first.begin(), first.end(), difference.begin(), difference.end(), difference.begin()); // difference is written in results
        difference.erase(end, difference.end()); // erase redundant elements
        return difference;
    }

    seastar::future<std::vector<uint64_t>> Shard::DifferenceIdsPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
          return seastar::make_ready_future<std::vector<uint64_t>>(DifferenceIds(ids1, ids2));
    }

    seastar::future<uint64_t> Shard::DifferenceIdsCountPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
          return seastar::make_ready_future<uint64_t>(DifferenceIds(ids1, ids2).size());
    }

    seastar::future<std::vector<Node>> Shard::DifferenceNodesPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
        return NodesGetPeered(DifferenceIds(ids1, ids2));
    }

    seastar::future<std::vector<Relationship>> Shard::DifferenceRelationshipsPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
        return RelationshipsGetPeered(DifferenceIds(ids1, ids2));
    }
}






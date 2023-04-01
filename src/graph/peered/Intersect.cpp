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

    std::vector<uint64_t> Shard::IntersectIds(const std::vector<uint64_t> &sorted_ids, const std::vector<uint64_t> &sorted_ids2) const {
        std::vector<uint64_t> intersection;
        intersection.reserve(std::min(sorted_ids.size(), sorted_ids2.size()));
        std::set_intersection(sorted_ids.begin(), sorted_ids.end(), sorted_ids2.begin(), sorted_ids2.end(),
          std::back_inserter(intersection));
        return intersection;
    }

    std::vector<uint64_t> IntersectUnsortedIds(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
        std::vector<uint64_t> first(ids1);
        std::vector<uint64_t> second(ids2);
        std::vector<uint64_t> intersection;
        std::sort(first.begin(), first.end());
        std::sort(second.begin(), second.end());

        std::set_intersection(first.begin(), first.end(), second.begin(), second.end(), std::inserter(intersection, intersection.begin()));
        return intersection;
    }

    seastar::future<std::vector<uint64_t>> Shard::IntersectIdsPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
          return seastar::make_ready_future<std::vector<uint64_t>>(IntersectIds(ids1, ids2));
    }

    seastar::future<uint64_t> Shard::IntersectIdsCountPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
          return seastar::make_ready_future<uint64_t>(IntersectIds(ids1, ids2).size());
    }

    seastar::future<std::vector<Node>> Shard::IntersectNodesPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
        return NodesGetPeered(IntersectIds(ids1, ids2));
    }

    seastar::future<std::vector<Relationship>> Shard::IntersectRelationshipsPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
        return RelationshipsGetPeered(IntersectIds(ids1, ids2));
    }
}






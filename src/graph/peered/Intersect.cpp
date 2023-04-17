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

    size_t Shard::IntersectIdsCount(const uint64_t *A, const size_t lenA, const uint64_t *B, const size_t lenB) const {
        size_t count = 0;
        if (lenA == 0 || lenB == 0) {
            return 0;
        }

        const uint64_t *endA = A + lenA;
        const uint64_t *endB = B + lenB;

        while (1) {
            while (*A < *B) {
            SKIP_FIRST_COMPARE:
                if (++A == endA) {
                    return count;
                }
            }
            while (*A > *B) {
                if (++B == endB) {
                    return count;
                }
            }
            if (*A == *B) {
                count++;
                if (++A == endA || ++B == endB)
                    return count;
            } else {
                goto SKIP_FIRST_COMPARE;
            }
        }
    }

    size_t Shard::IntersectIdsCount(const std::vector<uint64_t> &ids1, const std::vector<uint64_t> &ids2) const {
        std::vector<uint64_t> first(ids1);
        std::vector<uint64_t> second(ids2);
        std::sort(first.begin(), first.end());
        std::sort(second.begin(), second.end());
        return IntersectIdsCount(first.data(), first.size(), second.data(), second.size());
    }

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
          return seastar::make_ready_future<std::vector<uint64_t>>(IntersectUnsortedIds(ids1, ids2));
    }

    seastar::future<uint64_t> Shard::IntersectIdsCountPeered(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
          return seastar::make_ready_future<uint64_t>(IntersectIdsCount(ids1, ids2));
    }

    seastar::future<std::vector<Node>> Shard::IntersectNodesPeered(const std::vector<Node>& nodes1, const std::vector<Node>& nodes2) {
          std::vector<uint64_t> ids1, ids2;
          std::vector<Node> result;
          ids1.reserve(nodes1.size());
          ids2.reserve(nodes2.size());
          result.reserve(std::min(nodes1.size(), nodes2.size()));
          for (const auto & node : nodes1) {
            ids1.emplace_back(node.getId());
          }
          for (const auto & node : nodes2) {
            ids2.emplace_back(node.getId());
          }
          std::unordered_map<uint64_t, Node> nodes;
          if (ids1.size() > ids2.size()) {
            for (const auto node : nodes2) {
                nodes.insert({node.getId(), node});
            }
          } else {
            for (const auto node : nodes1) {
                nodes.insert({node.getId(), node});
            }
          }
          for (const auto id : IntersectUnsortedIds(ids1, ids2)) {
            result.emplace_back(nodes[id]);
          }
          return seastar::make_ready_future<std::vector<Node>>(result);
    }

    seastar::future<std::vector<Relationship>> Shard::IntersectRelationshipsPeered(const std::vector<Relationship>& rels1, const std::vector<Relationship>& rels2) {
          std::vector<uint64_t> ids1, ids2;
          std::vector<Relationship> result;
          ids1.reserve(rels1.size());
          ids2.reserve(rels2.size());
          result.reserve(std::min(rels1.size(), rels2.size()));
          for (const auto & node : rels1) {
            ids1.emplace_back(node.getId());
          }
          for (const auto & node : rels2) {
            ids2.emplace_back(node.getId());
          }
          std::unordered_map<uint64_t, Relationship> rels;
          if (ids1.size() > ids2.size()) {
            for (const auto node : rels2) {
                rels.insert({node.getId(), node});
            }
          } else {
            for (const auto node : rels1) {
                rels.insert({node.getId(), node});
            }
          }
          for (const auto id : IntersectUnsortedIds(ids1, ids2)) {
            result.emplace_back(rels[id]);
          }
          return seastar::make_ready_future<std::vector<Relationship>>(result);
    }
}






/*
 * Copyright Max De Marzi. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") {

}

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

    std::vector<uint64_t> Shard::NodeGetNeighborIds(const std::string &type, const std::string &key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetNeighborIds(id);
    }
    
    std::vector<uint64_t> Shard::NodeGetNeighborIds(const std::string &type, const std::string &key, Direction direction) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetNeighborIds(id, direction);
    }
    
    std::vector<uint64_t> Shard::NodeGetNeighborIds(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetNeighborIds(id, direction, rel_type);
    }
    
    std::vector<uint64_t> Shard::NodeGetNeighborIds(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetNeighborIds(id, direction, rel_types);
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(const std::string &type, const std::string &key, const std::vector<uint64_t>& ids) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetNeighborIds(id, ids);
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(const std::string &type, const std::string &key, Direction direction, const std::vector<uint64_t>& ids) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetNeighborIds(id, direction, ids);
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type, const std::vector<uint64_t>& ids) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetNeighborIds(id, direction, rel_type, ids);
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types, const std::vector<uint64_t>& ids) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetNeighborIds(id, direction, rel_types, ids);
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(const std::string &type, const std::string &key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetDistinctNeighborIds(id);
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(const std::string &type, const std::string &key, Direction direction) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetDistinctNeighborIds(id, direction);
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetDistinctNeighborIds(id, direction, rel_type);
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetDistinctNeighborIds(id, direction, rel_types);
    }

    seastar::future<roaring::Roaring64Map> Shard::NodeIdsGetNeighborIds(const std::vector<uint64_t>& ids) {
        roaring::Roaring64Map combined;
        for (const auto& id : ids) {
            if (ValidNodeId(id)) {
                uint64_t internal_id = externalToInternal(id);
                uint16_t node_type_id = externalToTypeId(id);
                for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                    auto out_ids = group.node_ids();
                    combined.addMany(out_ids.size(), out_ids.data());
                    co_await seastar::coroutine::maybe_yield();
                }
                for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                    auto in_ids = group.node_ids();
                    combined.addMany(in_ids.size(), in_ids.data());
                    co_await seastar::coroutine::maybe_yield();
                }

            }
            co_await seastar::coroutine::maybe_yield();
        }
        co_return combined;
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }
    
        uint64_t internal_id = externalToInternal(id);
        uint16_t node_type_id = externalToTypeId(id);
        std::vector<uint64_t> ids;
        for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            auto out_ids = group.node_ids();
            std::ranges::copy(out_ids, std::back_inserter(ids));
        }

        for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            auto in_ids = group.node_ids();
            std::ranges::copy(in_ids, std::back_inserter(ids));
        }

        return ids;
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(uint64_t id) {
        if (!ValidNodeId(id)) {
            return boost::container::flat_multimap<uint64_t, uint64_t>();
        }

        uint64_t internal_id = externalToInternal(id);
        uint16_t node_type_id = externalToTypeId(id);
        boost::container::flat_multimap<uint64_t, uint64_t> ids;
        for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            auto out_ids = group.link_map;
            ids.insert(out_ids.begin(), out_ids.end());
        }

        for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            auto in_ids = group.link_map;
            ids.insert(in_ids.begin(), in_ids.end());
        }

        return ids;
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(uint64_t id, const std::vector<uint64_t>& allowed_ids) {
        if (!ValidNodeId(id)) {
            return boost::container::flat_multimap<uint64_t, uint64_t>();
        }

        uint64_t internal_id = externalToInternal(id);
        uint16_t node_type_id = externalToTypeId(id);
        boost::container::flat_multimap<uint64_t, uint64_t> ids;
        for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            auto out_ids = group.link_map;
            for (const auto allowed_id : allowed_ids) {
                if (out_ids.contains(allowed_id)) {
                    auto range = out_ids.equal_range(allowed_id);
                    for (auto it = range.first; it != range.second; it++) {
                        ids.insert({ it->first, it->second });
                    }
                }
            }
        }

        for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            auto in_ids = group.link_map;
            for (const auto allowed_id : allowed_ids) {
                if (in_ids.contains(allowed_id)) {
                    auto range = in_ids.equal_range(allowed_id);
                    for (auto it = range.first; it != range.second; it++) {
                        ids.insert({ it->first, it->second });
                    }
                }
            }
        }

        return ids;
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id, const std::vector<uint64_t>& sorted_ids) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }

        uint64_t internal_id = externalToInternal(id);
        uint16_t node_type_id = externalToTypeId(id);
        std::vector<uint64_t> ids;
        for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            auto out_ids = group.node_ids();
            std::ranges::copy(out_ids, std::back_inserter(ids));
        }

        for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            auto in_ids = group.node_ids();
            std::ranges::copy(in_ids, std::back_inserter(ids));
        }
        std::sort(ids.begin(), ids.end());
        return IntersectIds(ids, sorted_ids);
    }

    seastar::future<roaring::Roaring64Map> Shard::NodeIdsGetNeighborIds(const std::vector<uint64_t>& ids, Direction direction) {
        roaring::Roaring64Map combined;
        for (const auto& id : ids) {
            if (ValidNodeId(id)) {
                uint64_t internal_id = externalToInternal(id);
                uint16_t node_type_id = externalToTypeId(id);


                // Use the two ifs to handle ALL for a direction
                if (direction != Direction::IN) {
                    for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                        auto out_ids = group.node_ids();
                        combined.addMany(out_ids.size(), out_ids.data());
                        co_await seastar::coroutine::maybe_yield();
                    }
                }
                // Use the two ifs to handle ALL for a direction
                if (direction != Direction::OUT) {
                    for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                        auto in_ids = group.node_ids();
                        combined.addMany(in_ids.size(), in_ids.data());
                        co_await seastar::coroutine::maybe_yield();
                    }
                }

            }
            co_await seastar::coroutine::maybe_yield();
        }
        co_return combined;
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id, Direction direction) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }
    
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<uint64_t> ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                auto out_ids = group.node_ids();
                std::ranges::copy(out_ids, std::back_inserter(ids));
            }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
            for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                auto in_ids = group.node_ids();
                std::ranges::copy(in_ids, std::back_inserter(ids));
            }
        }
        return ids;
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(uint64_t id, Direction direction) {
        if (!ValidNodeId(id)) {
            return boost::container::flat_multimap<uint64_t, uint64_t>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        boost::container::flat_multimap<uint64_t, uint64_t> ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                auto out_ids = group.link_map;
                ids.insert(out_ids.begin(), out_ids.end());
            }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
            for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                auto in_ids = group.link_map;
                ids.insert(in_ids.begin(), in_ids.end());
            }
        }
        return ids;
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id, Direction direction, const std::vector<uint64_t>& sorted_ids) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<uint64_t > ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            for (const auto &group : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                auto out_ids = group.node_ids();
                std::ranges::copy(out_ids, std::back_inserter(ids));
            }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
            for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                auto in_ids = group.node_ids();
                std::ranges::copy(in_ids, std::back_inserter(ids));
            }
        }
        std::sort(ids.begin(), ids.end());
        return IntersectIds(ids, sorted_ids);
    }

    seastar::future<roaring::Roaring64Map> Shard::NodeIdsGetNeighborIds(const std::vector<uint64_t>& ids, Direction direction, const std::string &rel_type) {
        roaring::Roaring64Map combined;
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        for (const auto& id : ids) {
            if (ValidNodeId(id)) {
                uint64_t internal_id = externalToInternal(id);
                uint16_t node_type_id = externalToTypeId(id);

                // Use the two ifs to handle ALL for a direction
                if (direction != Direction::IN) {
                    auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                      [type_id](const Group &g) { return g.rel_type_id == type_id; });

                    if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                        auto out_ids = out_group->node_ids();
                        combined.addMany(out_ids.size(), out_ids.data());
                        co_await seastar::coroutine::maybe_yield();
                    }
                }

                if (direction != Direction::OUT) {
                    auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                      [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                        auto in_ids = in_group->node_ids();
                        combined.addMany(in_ids.size(), in_ids.data());
                        co_await seastar::coroutine::maybe_yield();
                    }
                }
            }
            co_await seastar::coroutine::maybe_yield();
        }
        co_return combined;
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id, Direction direction, const std::string &rel_type) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::vector<uint64_t > ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id](const Group &g) { return g.rel_type_id == type_id; });
    
            if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                ids.reserve(out_group->size());
                std::ranges::copy(out_group->node_ids(), std::back_inserter(ids));
            }
        }
    
        if (direction != Direction::OUT) {
            auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );
    
            if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                ids.reserve(ids.size() + in_group->size());
                std::ranges::copy(in_group->node_ids(), std::back_inserter(ids));
            }
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(uint64_t id, Direction direction, const std::string &rel_type) {
        if (!ValidNodeId(id)) {
            return boost::container::flat_multimap<uint64_t, uint64_t>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        boost::container::flat_multimap<uint64_t, uint64_t> ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::BOTH) {
            if (direction == Direction::OUT) {
                auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                  [type_id](const Group &g) { return g.rel_type_id == type_id; });

                if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                    return out_group->link_map;
                }
            }

            auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                return in_group->link_map;
            }

        }
        if (direction != Direction::IN) {
            auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id](const Group &g) { return g.rel_type_id == type_id; });

            if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                auto out_ids = out_group->link_map;
                ids.reserve(out_group->size());
                ids.insert(out_ids.begin(), out_ids.end());
            }
        }

        if (direction != Direction::OUT) {
            auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                auto in_ids = in_group->link_map;
                ids.reserve(ids.size() + in_ids.size());
                ids.insert(in_ids.begin(), in_ids.end());
            }
        }
        return ids;
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id, Direction direction, const std::string &rel_type, const std::vector<uint64_t>& sorted_ids) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::vector<uint64_t > ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id](const Group &g) { return g.rel_type_id == type_id; });

            if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                ids.reserve(out_group->size());
                std::ranges::copy(out_group->node_ids(), std::back_inserter(ids));
            }
        }

        if (direction != Direction::OUT) {
            auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                ids.reserve(ids.size() + in_group->size());
                std::ranges::copy(in_group->node_ids(), std::back_inserter(ids));
            }
        }
        std::sort(ids.begin(), ids.end());
        return IntersectIds(ids, sorted_ids);
    }

    seastar::future<roaring::Roaring64Map> Shard::NodeIdsGetNeighborIds(const std::vector<uint64_t>& ids, Direction direction, const std::vector<std::string> &rel_types) {
        roaring::Roaring64Map combined;
        for (const auto& id : ids) {
            if (ValidNodeId(id)) {
                uint64_t internal_id = externalToInternal(id);
                uint16_t node_type_id = externalToTypeId(id);

                // Use the two ifs to handle ALL for a direction
                if (direction != Direction::IN) {
                    for (const auto &rel_type : rel_types) {
                        uint16_t type_id = relationship_types.getTypeId(rel_type);
                        if (type_id == 0) {
                            continue;
                        }
                        auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                          [type_id](const Group &g) { return g.rel_type_id == type_id; });

                        if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                            auto out_ids = out_group->node_ids();
                            combined.addMany(out_ids.size(), out_ids.data());
                            co_await seastar::coroutine::maybe_yield();
                        }
                    }
                }

                if (direction != Direction::OUT) {
                    for (const auto &rel_type : rel_types) {
                        uint16_t type_id = relationship_types.getTypeId(rel_type);
                        if (type_id == 0) {
                            continue;
                        }
                        auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                          [type_id](const Group &g) { return g.rel_type_id == type_id; });

                        if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                            auto in_ids = in_group->node_ids();
                            combined.addMany(in_ids.size(), in_ids.data());
                            co_await seastar::coroutine::maybe_yield();
                        }
                    }
                }
            }
            co_await seastar::coroutine::maybe_yield();
        }
        co_return combined;
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<uint64_t> ids;
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            // For each requested type sum up the values
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id == 0) {
                    continue;
                }
                auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );
    
                if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                    std::ranges::copy(out_group->node_ids(), std::back_inserter(ids));
                }
            }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
            // For each requested type sum up the values
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id == 0) {
                    continue;
                }
                auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );
    
                if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                    std::ranges::copy(in_group->node_ids(), std::back_inserter(ids));
                }
    
            }
        }
        return ids;
    }

    boost::container::flat_multimap<uint64_t, uint64_t> Shard::NodeGetDistinctNeighborIds(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        if (!ValidNodeId(id)) {
            return boost::container::flat_multimap<uint64_t, uint64_t>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        boost::container::flat_multimap<uint64_t, uint64_t> ids;
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            // For each requested type sum up the values
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id == 0) {
                    continue;
                }
                auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                    auto out_ids = out_group->link_map;
                    ids.reserve(ids.size() + out_ids.size());
                    ids.insert(out_ids.begin(), out_ids.end());
                }
            }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
            // For each requested type sum up the values
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id == 0) {
                    continue;
                }
                auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                    auto in_ids = in_group->link_map;
                    ids.reserve(ids.size() + in_ids.size());
                    ids.insert(in_ids.begin(), in_ids.end());
                }

            }
        }
        return ids;
    }

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id, Direction direction, const std::vector<std::string> &rel_types, const std::vector<uint64_t>& sorted_ids) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<uint64_t> ids;
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            // For each requested type sum up the values
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id == 0) {
                    continue;
                }
                auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                    std::ranges::copy(out_group->node_ids(), std::back_inserter(ids));
                }
            }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
            // For each requested type sum up the values
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id == 0) {
                    continue;
                }
                auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                    std::ranges::copy(in_group->node_ids(), std::back_inserter(ids));
                }

            }
        }
        std::sort(ids.begin(), ids.end());
        return IntersectIds(ids, sorted_ids);
    }
}
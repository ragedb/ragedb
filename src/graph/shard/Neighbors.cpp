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

    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }
    
        uint64_t internal_id = externalToInternal(id);
        uint16_t type_id = externalToTypeId(id);
        std::vector<uint64_t> ids;
        auto out_ids = node_types.getOutgoingRelationships(type_id).at(internal_id).at(internal_id).node_ids();
        std::ranges::copy(out_ids, std::back_inserter(ids));

        auto in_ids = node_types.getIncomingRelationships(type_id).at(internal_id).at(internal_id).node_ids();
        std::ranges::copy(in_ids, std::back_inserter(ids));

        return ids;
    }
    
    std::vector<uint64_t> Shard::NodeGetNeighborIds(uint64_t id, Direction direction) {
        if (!ValidNodeId(id)) {
            return std::vector<uint64_t>();
        }
    
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<uint64_t > ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            auto out_ids = node_types.getOutgoingRelationships(node_type_id).at(internal_id).at(internal_id).node_ids();
            std::ranges::copy(out_ids, std::back_inserter(ids));
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
            auto in_ids = node_types.getIncomingRelationships(node_type_id).at(internal_id).at(internal_id).node_ids();
            std::ranges::copy(in_ids, std::back_inserter(ids));
        }
        return ids;
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
                ids.reserve(out_group->links.size());
                std::ranges::copy(out_group->node_ids(), std::back_inserter(ids));
            }
        }
    
        if (direction != Direction::OUT) {
            auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );
    
            if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                ids.reserve(ids.size() + in_group->links.size());
                std::ranges::copy(in_group->node_ids(), std::back_inserter(ids));
            }
        }
        return ids;
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

}
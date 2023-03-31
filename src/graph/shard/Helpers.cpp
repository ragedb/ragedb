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

    bool Shard::NodeRemoveDeleteIncoming(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>>&grouped_relationships) {
        for (const auto& [rel_type_id, ids] : grouped_relationships) {
            for (const auto& node_id : ids) {
                uint64_t internal_id = externalToInternal(node_id);
                uint16_t node_type_id = externalToTypeId(node_id);

                std::ranges::for_each(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                  [rel_type_id = rel_type_id, id] (Group& g) {
                      if (g.rel_type_id == rel_type_id) {
                          g.link_map.erase(id);
                      }
                  });
            }
        }

        return true;
    }

    std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> Shard::NodeRemoveGetIncoming(uint64_t external_id) {
        uint64_t internal_id = externalToInternal(external_id);
        uint16_t node_type_id = externalToTypeId(external_id);
        std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> relationships_to_delete;
        for (uint16_t i = 0; i < cpus; i++) {
            relationships_to_delete.try_emplace(i, std::map<uint16_t, std::vector<uint64_t>>());
        }

        // Go through all the outgoing relationships and return the counterparts that I do not own
        for (const auto &types : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            // Get the Relationship Type of the list
            uint16_t rel_type = types.rel_type_id;

            for (Link link : types.links()) {
                std::map<uint16_t, std::vector<uint64_t>> node_ids;
                for (uint16_t i = 0; i < cpus; i++) {
                    node_ids.try_emplace(i, std::vector<uint64_t>());
                }

                // Remove relationship from other node that I own
                if (uint16_t node_shard_id = CalculateShardId(link.node_id);
                    node_shard_id != shard_id) {
                    node_ids.at(node_shard_id).push_back(link.node_id);
                }

                for (uint16_t i = 0; i < cpus; i++) {
                    if(!node_ids.at(i).empty()) {
                        relationships_to_delete[i].try_emplace(rel_type, node_ids.at(i));
                    }
                }
            }
        }
        for (uint16_t i = 0; i < cpus; i++) {
            if(relationships_to_delete[i].empty()) {
                relationships_to_delete.erase(i);
            }
        }

        return relationships_to_delete;
    }

    std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> Shard::NodeRemoveGetOutgoing(uint64_t external_id) {
        uint64_t internal_id = externalToInternal(external_id);
        uint16_t node_type_id = externalToTypeId(external_id);
        std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> relationships_to_delete;
        for (uint16_t i = 0; i < cpus; i++) {
            relationships_to_delete.try_emplace(i, std::map<uint16_t, std::vector<uint64_t>>());
        }

        // Go through all the incoming relationships and return the counterparts that I do not own
        for (const auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            // Get the Relationship Type of the list
            uint16_t rel_type = group.rel_type_id;

            for (Link link : group.links()) {
                std::map<uint16_t, std::vector<uint64_t>> node_ids;
                for (uint16_t i = 0; i < cpus; i++) {
                    node_ids.try_emplace(i, std::vector<uint64_t>());
                }

                // Remove relationship from other node that I own

                if (uint16_t node_shard_id = CalculateShardId(link.node_id);
                    node_shard_id != shard_id) {
                    node_ids.at(node_shard_id).push_back(link.node_id);
                }

                for (uint16_t i = 0; i < cpus; i++) {
                    if(!node_ids.at(i).empty()) {
                        relationships_to_delete[i].try_emplace(rel_type, node_ids.at(i));
                    }
                }

            }
        }
        for (uint16_t i = 0; i < cpus; i++) {
            if(relationships_to_delete[i].empty()) {
                relationships_to_delete.erase(i);
            }
        }

        return relationships_to_delete;
    }

    bool Shard::NodeRemoveDeleteOutgoing(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>> &grouped_relationships) {
        for (const auto& [rel_type_id, ids] : grouped_relationships) {
            for (const auto& node_id : ids) {
                uint64_t internal_id = externalToInternal(node_id);
                uint16_t node_type_id = externalToTypeId(node_id);

                std::ranges::for_each(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                  [rel_type_id = rel_type_id, id, this] (Group& g) {
                      // Look in the relationship chain for any relationships of the node to be removed and delete them.
                      if (g.rel_type_id == rel_type_id) {
                          for (auto itr = g.link_map.find(id); itr != g.link_map.end(); itr++) {
                              auto node = *itr;
                              uint64_t other_internal_id = externalToInternal(node.second);
                              // Update the relationship type counts
                              relationship_types.removeId(rel_type_id, other_internal_id);
                              relationship_types.setStartingNodeId(rel_type_id, other_internal_id, 0);
                              relationship_types.setEndingNodeId(rel_type_id, other_internal_id, 0);
                              relationship_types.deleteProperties(rel_type_id, other_internal_id);
                          }
                          g.link_map.erase(id);
                      }
                  });
            }
        }

        return true;
    }
}

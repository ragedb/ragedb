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
        for (const auto& rel_type_node_ids : grouped_relationships) {
            uint16_t rel_type_id = rel_type_node_ids.first;
            for (auto node_id : rel_type_node_ids.second) {
                uint64_t internal_id = externalToInternal(node_id);
                uint16_t node_type_id = externalToTypeId(node_id);

                auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                     [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

                if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                    group->links.erase(std::remove_if(std::begin(group->links), std::end(group->links), [id](Link entry) {
                        return entry.node_id == id;
                    }), std::end(group->links));
                }
            }
        }

        return true;
    }

    std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> Shard::NodeRemoveGetIncoming(uint64_t external_id) {
        uint64_t internal_id = externalToInternal(external_id);
        uint16_t node_type_id = externalToTypeId(external_id);
        std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> relationships_to_delete;
        for (int i = 0; i < cpus; i++) {
            relationships_to_delete.insert({i, std::map<uint16_t, std::vector<uint64_t>>() });
        }

        // Go through all the outgoing relationships and return the counterparts that I do not own
        for (auto &types : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            // Get the Relationship Type of the list
            uint16_t rel_type = types.rel_type_id;

            for (Link link : types.links) {
                std::map<uint16_t, std::vector<uint64_t>> node_ids;
                for (int i = 0; i < cpus; i++) {
                    node_ids.insert({ i, std::vector<uint64_t>() });
                }

                // Remove relationship from other node that I own
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                if ( node_shard_id != shard_id) {
                    node_ids.at(node_shard_id).push_back(link.node_id);
                }

                for (int i = 0; i < cpus; i++) {
                    if(!node_ids.at(i).empty()) {
                        relationships_to_delete[i].insert({ rel_type, node_ids.at(i) });
                    }
                }
            }
        }
        for (int i = 0; i < cpus; i++) {
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
        for (int i = 0; i < cpus; i++) {
            relationships_to_delete.insert({i, std::map<uint16_t, std::vector<uint64_t>>() });
        }

        // Go through all the incoming relationships and return the counterparts that I do not own
        for (auto &group : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            // Get the Relationship Type of the list
            uint16_t rel_type = group.rel_type_id;

            for (Link link : group.links) {
                std::map<uint16_t, std::vector<uint64_t>> node_ids;
                for (int i = 0; i < cpus; i++) {
                    node_ids.insert({ i, std::vector<uint64_t>() });
                }

                // Remove relationship from other node that I own
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                if ( node_shard_id != shard_id) {
                    node_ids.at(node_shard_id).push_back(link.node_id);
                }

                for (int i = 0; i < cpus; i++) {
                    if(!node_ids.at(i).empty()) {
                        relationships_to_delete[i].insert({ rel_type, node_ids.at(i) });
                    }
                }

            }
        }
        for (int i = 0; i < cpus; i++) {
            if(relationships_to_delete[i].empty()) {
                relationships_to_delete.erase(i);
            }
        }

        return relationships_to_delete;
    }

    bool Shard::NodeRemoveDeleteOutgoing(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>> &grouped_relationships) {
        for (const auto& rel_type_node_ids : grouped_relationships) {
            uint16_t rel_type_id = rel_type_node_ids.first;
            for (auto node_id : rel_type_node_ids.second) {
                uint64_t internal_id = externalToInternal(node_id);
                uint16_t node_type_id = externalToTypeId(node_id);

                auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                     [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

                if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                    // Look in the relationship chain for any relationships of the node to be removed and delete them.
                    group->links.erase(std::remove_if(std::begin(group->links), std::end(group->links), [id, rel_type_id, this](Link entry) {
                        if (entry.node_id == id) {
                            uint64_t internal_id = externalToInternal(entry.rel_id);
                            // Update the relationship type counts
                            relationship_types.removeId(rel_type_id, internal_id);
                            relationship_types.setStartingNodeId(rel_type_id, internal_id, 0);
                            relationship_types.setEndingNodeId(rel_type_id, internal_id, 0);
                            relationship_types.deleteProperties(rel_type_id, internal_id);
                            return true;
                        }

                        return false;
                    }), std::end(group->links));
                }

            }
        }

        return true;
    }
}

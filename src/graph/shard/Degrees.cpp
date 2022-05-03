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

    uint64_t Shard::NodeGetDegree(const std::string &type, const std::string &key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetDegree(id);
    }

    uint64_t Shard::NodeGetDegree(const std::string &type, const std::string &key, Direction direction) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetDegree(id, direction);
    }

    uint64_t Shard::NodeGetDegree(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetDegree(id, direction, rel_type);
    }

    uint64_t Shard::NodeGetDegree(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetDegree(id, direction, rel_types);
    }

    uint64_t Shard::NodeGetDegree(uint64_t id) {
        if (ValidNodeId(id)) {
            uint64_t internal_id = externalToInternal(id);
            uint16_t node_type_id = externalToTypeId(id);
            return node_types.getOutgoingRelationships(node_type_id).at(internal_id).size() + node_types.getIncomingRelationships(node_type_id).at(internal_id).size();
        }
        return 0;
    }

    uint64_t Shard::NodeGetDegree(uint64_t id, Direction direction) {
        if (ValidNodeId(id)) {
            uint64_t internal_id = externalToInternal(id);
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t count = 0;
            // Use the two ifs to handle ALL for a direction
            if (direction != Direction::IN) {
                // For each type sum up the values
                for (const auto &[key, value] : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                    count += value.size();
                }
            }
            if (direction != Direction::OUT) {
                // For each type sum up the values
                for (const auto &[key, value] : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                    count += value.size();
                }
            }
            return count;
        }

        return 0;
    }

    uint64_t Shard::NodeGetDegree(uint64_t id, Direction direction, const std::string &rel_type) {
        if (ValidNodeId(id)) {
            uint64_t internal_id = externalToInternal(id);
            uint16_t node_type_id = externalToTypeId(id);
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {
                uint64_t count = 0;
                // Use the two ifs to handle ALL for a direction
                if (direction != Direction::IN) {
                    auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                        count += group->links.size();
                    }
                }
                if (direction != Direction::OUT) {
                    auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                        count += group->links.size();
                    }
                }
                return count;
            }
        }

        return 0;
    }

    uint64_t Shard::NodeGetDegree(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        if (ValidNodeId(id)) {
            uint64_t internal_id = externalToInternal(id);
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t count = 0;
            // Use the two ifs to handle ALL for a direction
            if (direction != Direction::IN) {
                // For each requested type sum up the values
                for (const auto &rel_type : rel_types) {
                    uint16_t type_id = relationship_types.getTypeId(rel_type);
                    if (type_id > 0) {
                        auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                             [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                        if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                            count += group->links.size();
                        }
                    }
                }
            }
            if (direction != Direction::OUT) {
                // For each requested type sum up the values
                for (const auto &rel_type : rel_types) {
                    uint16_t type_id = relationship_types.getTypeId(rel_type);
                    if (type_id > 0) {
                        auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                             [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                        if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                            count += group->links.size();
                        }
                    }
                }
            }
            return count;
        }

        return 0;
    }

}
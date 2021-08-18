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

    std::map<uint16_t, uint64_t> Shard::NodeCounts() {
        return node_types.getCounts();
    }

    uint64_t Shard::NodeCount(const std::string &type) {
        uint16_t type_id = node_types.getTypeId(type);
        return NodeCount(type_id);
    }

    uint64_t Shard::NodeCount(uint16_t type_id) {
        return node_types.getCount(type_id);
    }

    std::vector<uint64_t> Shard::AllNodeIds(uint64_t skip, uint64_t limit ) {
        return node_types.getIds(skip, limit);
    }

    std::vector<uint64_t> Shard::AllNodeIds(const std::string& type, uint64_t skip, uint64_t limit) {
        return AllNodeIds(node_types.getTypeId(type), skip, limit);
    }

    std::vector<uint64_t> Shard::AllNodeIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
        return node_types.getIds(type_id, skip, limit);
    }

    std::vector<Node> Shard::AllNodes(uint64_t skip, uint64_t limit) {
        return node_types.getNodes(skip, limit);
    }

    std::vector<Node> Shard::AllNodes(const std::string& type, uint64_t skip, uint64_t limit) {
        uint16_t type_id = node_types.getTypeId(type);
        return AllNodes(type_id, skip, limit);
    }

    std::vector<Node> Shard::AllNodes(uint16_t type_id, uint64_t skip, uint64_t limit) {
        return node_types.getNodes(type_id, skip, limit);
    }

    std::vector<uint64_t> Shard::AllRelationshipIds(uint64_t skip, uint64_t limit) {
        return relationship_types.getIds(skip, limit);
    }

    std::vector<uint64_t> Shard::AllRelationshipIds(const std::string& rel_type, uint64_t skip, uint64_t limit) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        return AllRelationshipIds(type_id, skip, limit);
    }

    std::vector<uint64_t> Shard::AllRelationshipIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
        return relationship_types.getIds(type_id, skip, limit);
    }

    std::vector<Relationship> Shard::AllRelationships(uint64_t skip, uint64_t limit) {
        return relationship_types.getRelationships(skip, limit);
    }

    std::vector<Relationship> Shard::AllRelationships(const std::string& rel_type, uint64_t skip, uint64_t limit) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        return AllRelationships(type_id, skip, limit);
    }

    std::vector<Relationship> Shard::AllRelationships(uint16_t type_id, uint64_t skip, uint64_t limit) {
        return relationship_types.getRelationships(type_id, skip, limit);
    }

    std::map<uint16_t, uint64_t> Shard::AllRelationshipIdCounts() {
        return relationship_types.getCounts();
    }

    uint64_t Shard::AllRelationshipIdCounts(const std::string &type) {
        uint16_t type_id = relationship_types.getTypeId(type);
        return AllRelationshipIdCounts(type_id);
    }

    uint64_t Shard::AllRelationshipIdCounts(uint16_t type_id) {
        return relationship_types.getCount(type_id);
    }

}
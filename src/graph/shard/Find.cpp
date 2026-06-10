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

    uint64_t Shard::FindNodeCount(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
        uint16_t type_id = node_types.getTypeId(type);
        auto type_it = node_indexes.find(type_id);
        if (type_it != node_indexes.end() && type_it->second.find(property) != type_it->second.end()) {
            return NodeIndexLookup(type_id, property, operation, value).size();
        }
        return node_types.findCount(type_id, property, operation, value);
    }

    uint64_t Shard::FindRelationshipCount(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
        uint16_t type_id = relationship_types.getTypeId(type);
        auto type_it = relationship_indexes.find(type_id);
        if (type_it != relationship_indexes.end() && type_it->second.find(property) != type_it->second.end()) {
            return RelationshipIndexLookup(type_id, property, operation, value).size();
        }
        return relationship_types.findCount(type_id, property, operation, value);
    }

    std::vector<uint64_t> Shard::FindNodeIds(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
        uint16_t type_id = node_types.getTypeId(type);
        auto type_it = node_indexes.find(type_id);
        if (type_it != node_indexes.end() && type_it->second.find(property) != type_it->second.end()) {
            std::vector<uint64_t> ids = NodeIndexLookup(type_id, property, operation, value);
            if (skip >= ids.size()) {
                return {};
            }
            auto start = ids.begin() + skip;
            auto end = ids.end();
            if (ids.size() - skip > limit) {
                end = start + limit;
            }
            return std::vector<uint64_t>(start, end);
        }
        return node_types.findIds(type_id, property, operation, value, skip, limit);
    }

    std::vector<uint64_t> Shard::FindRelationshipIds(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
        uint16_t type_id = relationship_types.getTypeId(type);
        auto type_it = relationship_indexes.find(type_id);
        if (type_it != relationship_indexes.end() && type_it->second.find(property) != type_it->second.end()) {
            std::vector<uint64_t> ids = RelationshipIndexLookup(type_id, property, operation, value);
            if (skip >= ids.size()) {
                return {};
            }
            auto start = ids.begin() + skip;
            auto end = ids.end();
            if (ids.size() - skip > limit) {
                end = start + limit;
            }
            return std::vector<uint64_t>(start, end);
        }
        return relationship_types.findIds(type_id, property, operation, value, skip, limit);
    }

    std::vector<Node> Shard::FindNodes(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
        uint16_t type_id = node_types.getTypeId(type);
        auto type_it = node_indexes.find(type_id);
        if (type_it != node_indexes.end() && type_it->second.find(property) != type_it->second.end()) {
            std::vector<uint64_t> ids = NodeIndexLookup(type_id, property, operation, value);
            if (skip >= ids.size()) {
                return {};
            }
            auto start = ids.begin() + skip;
            auto end = ids.end();
            if (ids.size() - skip > limit) {
                end = start + limit;
            }
            std::vector<uint64_t> sliced(start, end);
            std::vector<Node> nodes;
            nodes.reserve(sliced.size());
            for (uint64_t external_id : sliced) {
                nodes.push_back(node_types.getNode(external_id));
            }
            return nodes;
        }
        return node_types.findNodes(type_id, property, operation, value, skip, limit);
    }

    std::vector<Relationship> Shard::FindRelationships(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
        uint16_t type_id = relationship_types.getTypeId(type);
        auto type_it = relationship_indexes.find(type_id);
        if (type_it != relationship_indexes.end() && type_it->second.find(property) != type_it->second.end()) {
            std::vector<uint64_t> ids = RelationshipIndexLookup(type_id, property, operation, value);
            if (skip >= ids.size()) {
                return {};
            }
            auto start = ids.begin() + skip;
            auto end = ids.end();
            if (ids.size() - skip > limit) {
                end = start + limit;
            }
            std::vector<uint64_t> sliced(start, end);
            std::vector<Relationship> relationships;
            relationships.reserve(sliced.size());
            for (uint64_t external_id : sliced) {
                relationships.push_back(relationship_types.getRelationship(external_id));
            }
            return relationships;
        }
        return relationship_types.findRelationships(type_id, property, operation, value, skip, limit);
    }

}

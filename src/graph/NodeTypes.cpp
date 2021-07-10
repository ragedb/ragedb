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

#include <tsl/sparse_map.h>
#include "NodeTypes.h"


namespace ragedb {

    NodeTypes::NodeTypes() : type_to_id(), id_to_type() {
        // start with empty blank type 0
        type_to_id.emplace("", 0);
        id_to_type.emplace_back();
        node_keys.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        nodes.emplace_back(std::vector<Node>());
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
        ids.emplace_back(Roaring64Map());
        deleted_ids.emplace_back(Roaring64Map());
    }

    uint16_t NodeTypes::getTypeId(const std::string &type) {
        auto type_search = type_to_id.find(type);
        if (type_search != type_to_id.end()) {
            return type_search->second;
        }
        return 0;
    }

    uint16_t NodeTypes::insertOrGetTypeId(const std::string &type) {
        // Get
        auto type_search = type_to_id.find(type);
        if (type_search != type_to_id.end()) {
            return type_search->second;
        }
        // Insert
        uint16_t type_id = type_to_id.size();
        type_to_id.emplace(type, type_id);
        id_to_type.emplace_back(type);
        node_keys.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        nodes.emplace_back(std::vector<Node>());
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
        ids.emplace_back(Roaring64Map());
        deleted_ids.emplace_back(Roaring64Map());
        return type_id;
    }

    std::string NodeTypes::getType(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return id_to_type[type_id];
        }
        // If not found return empty
        return id_to_type[0];
    }

    bool NodeTypes::addId(uint16_t type_id, uint64_t id) {
        if (ValidTypeId(type_id)) {
            ids[type_id].add(id);
            deleted_ids[type_id].remove(id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool NodeTypes::removeId(uint16_t type_id, uint64_t id) {
        if (ValidTypeId(type_id)) {
            ids[type_id].remove(id);
            deleted_ids[type_id].add(id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool NodeTypes::containsId(uint16_t type_id, uint64_t id) {
        if (ValidTypeId(type_id)) {
            return ids[type_id].contains(id);
        }
        // If not valid return false
        return false;
    }

    Roaring64Map NodeTypes::getIds() const {
        Roaring64Map allIds;
        for (int i=1; i < type_to_id.size(); i++) {
            allIds.operator|=(ids[i]);
        }
        return allIds;
    }

    Roaring64Map NodeTypes::getIds(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return ids[type_id];
        }
        return ids[0];
    }

    Roaring64Map NodeTypes::getDeletedIds() const {
        Roaring64Map allIds;
        for (int i=1; i < type_to_id.size(); i++) {
            allIds.operator|=(deleted_ids[i]);
        }
        return allIds;
    }

    Roaring64Map NodeTypes::getDeletedIds(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return deleted_ids[type_id];
        }
        return deleted_ids[0];
    }

    bool NodeTypes::ValidTypeId(uint16_t type_id) const {
        // TypeId must be greater than zero
        return (type_id > 0 && type_id < id_to_type.size());
    }

    uint64_t NodeTypes::getCount(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return ids[type_id].cardinality();
        }
        // If not valid return 0
        return 0;
    }

    uint16_t NodeTypes::getSize() const {
        return id_to_type.size() - 1;
    }

    std::set<std::string> NodeTypes::getTypes() {
        // Skip the empty type
        return {id_to_type.begin() + 1, id_to_type.end()};
    }

    std::set<uint16_t> NodeTypes::getTypeIds() {
        std::set<uint16_t> type_ids;
        for (int i=1; i < type_to_id.size(); i++) {
            type_ids.insert(i);
        }

        return type_ids;
    }

    std::map<uint16_t,uint64_t> NodeTypes::getCounts() {
        std::map<uint16_t,uint64_t> counts;
        for (int i=1; i < type_to_id.size(); i++) {
            counts.insert({i, ids[i].cardinality()});
        }

        return counts;
    }

    bool NodeTypes::addTypeId(const std::string& type, uint16_t type_id) {
        auto type_search = type_to_id.find(type);
        if (type_search != type_to_id.end()) {
            // Type already exists
            return false;
        } else {
            if (ValidTypeId(type_id)) {
                // Id already exists
                return false;
            }
            type_to_id.emplace(type, type_id);
            id_to_type.emplace_back(type);
            node_keys.emplace_back(tsl::sparse_map<std::string, uint64_t>());
            nodes.emplace_back(std::vector<Node>());
            outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
            incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
            ids.emplace_back(Roaring64Map());
            deleted_ids.emplace_back(Roaring64Map());
            return true;
        }
    }

    tsl::sparse_map<std::string, uint64_t> NodeTypes::getNodeKeys(uint16_t type_id) {
        return node_keys[type_id];
    }

    std::vector<Node> NodeTypes::getNodes(uint16_t type_id) {
        return nodes[type_id];
    }

    std::vector<std::vector<Group>> NodeTypes::getOutgoingRelationships(uint16_t type_id) {
        return outgoing_relationships[type_id];
    }

    std::vector<std::vector<Group>> NodeTypes::getIncomingRelationships(uint16_t type_id) {
        return incoming_relationships[type_id];
    }

}
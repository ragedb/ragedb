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
#include <iostream>
#include <simdjson.h>
#include "NodeTypes.h"
#include "Shard.h"


namespace ragedb {

    static const unsigned int SHARD_BITS = 10U;
    static const unsigned int SHARD_MASK = 0x00000000000003FFU;
    static const unsigned int TYPE_BITS = 16U;
    static const unsigned int TYPE_MASK = 0x0000000003FFFFFFU;

    NodeTypes::NodeTypes() : type_to_id(), id_to_type(), shard_id(seastar::this_shard_id()) {
        // start with empty blank type 0
        type_to_id.emplace("", 0);
        id_to_type.emplace_back();
        key_to_node_id.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        keys.emplace_back(std::vector<std::string>());
        node_properties.emplace_back();
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
        ids.emplace_back(Roaring64Map());
        deleted_ids.emplace_back(Roaring64Map());
    }

    void NodeTypes::Clear() {
        type_to_id.clear();
        id_to_type.clear();
        id_to_type.shrink_to_fit();
        key_to_node_id.clear();
        key_to_node_id.shrink_to_fit();
        keys.clear();
        keys.shrink_to_fit();
        node_properties.clear();
        node_properties.shrink_to_fit();
        outgoing_relationships.clear();
        outgoing_relationships.shrink_to_fit();
        incoming_relationships.clear();
        incoming_relationships.shrink_to_fit();
        ids.clear();
        ids.shrink_to_fit();
        deleted_ids.clear();
        deleted_ids.shrink_to_fit();

        // start with empty blank type 0
        type_to_id.emplace("", 0);
        id_to_type.emplace_back();
        key_to_node_id.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        keys.emplace_back(std::vector<std::string>());
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
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
        key_to_node_id.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        keys.emplace_back(std::vector<std::string>());
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
        // ids are internal ids, we need to switch to external ids
        for (int type_id=1; type_id < id_to_type.size(); type_id++) {
            for (Roaring64MapSetBitForwardIterator iterator = ids[type_id].begin(); iterator != ids[type_id].end(); iterator++) {
              allIds.add(internalToExternal(type_id, iterator.operator*()));
            }
        }
        return allIds;
    }

    Roaring64Map NodeTypes::getIds(uint16_t type_id) {
        Roaring64Map allIds;
        if (ValidTypeId(type_id)) {
            for (Roaring64MapSetBitForwardIterator iterator = ids[type_id].begin(); iterator != ids[type_id].end(); iterator++) {
                allIds.add(internalToExternal(type_id, iterator.operator*()));
            }
        }
        return allIds;
    }

    Roaring64Map NodeTypes::getDeletedIds() const {

        Roaring64Map allIds;
        // ids are internal ids, we need to switch to external ids
        for (int type_id=1; type_id < id_to_type.size(); type_id++) {
            for (Roaring64MapSetBitForwardIterator iterator = deleted_ids[type_id].begin(); iterator != deleted_ids[type_id].end(); iterator++) {
                allIds.add(internalToExternal(type_id, iterator.operator*()));
            }
        }

        return allIds;
    }

    Roaring64Map NodeTypes::getDeletedIds(uint16_t type_id) {
        Roaring64Map allIds;
        if (ValidTypeId(type_id)) {
            for (Roaring64MapSetBitForwardIterator iterator = deleted_ids[type_id].begin(); iterator != deleted_ids[type_id].end(); iterator++) {
                allIds.add(internalToExternal(type_id, iterator.operator*()));
            }
        }
        return allIds;
    }

    bool NodeTypes::ValidTypeId(uint16_t type_id) const {
        // TypeId must be greater than zero
        return (type_id > 0 && type_id < id_to_type.size());
    }

    bool NodeTypes::ValidNodeId(uint16_t type_id, uint64_t internal_id) {
        // If the type is valid, is the internal id within the vector size and is it not deleted?
        if (ValidTypeId(type_id)) {
            return key_to_node_id[type_id].size() > internal_id && !deleted_ids[type_id].contains(internal_id);
        }
        return false;
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
            key_to_node_id.emplace_back(tsl::sparse_map<std::string, uint64_t>());
            keys.emplace_back(std::vector<std::string>());
            outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
            incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
            ids.emplace_back(Roaring64Map());
            deleted_ids.emplace_back(Roaring64Map());
            return true;
        }
    }

    tsl::sparse_map<std::string, uint64_t> &NodeTypes::getKeysToNodeId(uint16_t node_type_id) {
        return key_to_node_id[node_type_id];
    }

    uint64_t NodeTypes::getNodeId(uint16_t type_id, const std::string& key) {
        if(ValidTypeId(type_id)) {
            auto key_search = key_to_node_id[type_id].find(key);
            if (key_search != std::end(key_to_node_id[type_id])) {
                return key_search->second;
            }
        }
        return 0;
    }

    uint64_t NodeTypes::getNodeId(const std::string& type, const std::string& key) {
        uint16_t type_id = getTypeId(type);
        if(ValidTypeId(type_id)) {
            auto key_search = key_to_node_id[type_id].find(key);
            if (key_search != std::end(key_to_node_id[type_id])) {
                return key_search->second;
            }
        }
        return 0;
    }

    std::string NodeTypes::getNodeKey(uint16_t node_type_id, uint64_t internal_id) {
        if(ValidTypeId(node_type_id)) {
            return keys[node_type_id][internal_id];
        }
        return id_to_type[0];
    };

    std::map<std::string, std::any> NodeTypes::getNodeProperties(uint16_t node_type_id, uint64_t internal_id) {
        if(ValidTypeId(node_type_id)) {
            node_properties[node_type_id].getProperties(internal_id);
        }
        return std::map<std::string, std::any>();
    }

    Node NodeTypes::getNode(uint16_t node_type_id, uint64_t internal_id, uint64_t external_id) {
        return Node(external_id, getType(node_type_id), getKeys(node_type_id)[internal_id], getNodeProperties(node_type_id, internal_id));
    }

    Properties &NodeTypes::getNodeTypeProperties(uint16_t type_id) {
        return node_properties[type_id];
    }

    bool NodeTypes::setProperties(uint16_t type_id, uint64_t internal_id, const std::string& properties) {
        if (!properties.empty()) {
            // Get the properties
            simdjson::dom::object object;
            simdjson::error_code error = parser.parse(properties).get(object);

            if (!error) {
                // Add the node properties
                for (auto[key, value] : object) {
                    auto property = static_cast<std::string>(key);
                    switch (value.type()) {
                        case simdjson::dom::element_type::INT64:
                            node_properties[type_id].setIntegerProperty(property, internal_id, int64_t(value));
                            break;
                        case simdjson::dom::element_type::UINT64:
                            // Unsigned Integer Values are not allowed, convert to signed
                            node_properties[type_id].setIntegerProperty(property, internal_id, static_cast<std::make_signed_t<uint64_t>>(value));
                            break;
                        case simdjson::dom::element_type::DOUBLE:
                            node_properties[type_id].setDoubleProperty(property, internal_id, double(value));
                            break;
                        case simdjson::dom::element_type::STRING:
                            node_properties[type_id].setStringProperty(property, internal_id, std::string(value));
                            break;
                        case simdjson::dom::element_type::BOOL:
                            node_properties[type_id].setBooleanProperty(property, internal_id, bool(value));
                            break;
                        case simdjson::dom::element_type::NULL_VALUE:
                            // Null Values are not allowed, just ignore them
                            break;
                        case simdjson::dom::element_type::OBJECT: {
                            // TODO: Add support for nested properties
                            break;
                        }
                        case simdjson::dom::element_type::ARRAY: {
                            auto array = simdjson::dom::array(value);
                            if (array.size() > 0) {
                                simdjson::dom::element first = array.at(0);
                                std::vector<int64_t> int_vector;
                                std::vector<double> double_vector;
                                std::vector<std::string> string_vector;
                                std::vector<bool> bool_vector;
                                switch (first.type()) {
                                    case simdjson::dom::element_type::ARRAY:
                                        break;
                                    case simdjson::dom::element_type::OBJECT:
                                        break;
                                    case simdjson::dom::element_type::INT64:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            int_vector.emplace_back(int64_t(child));
                                        }
                                        node_properties[type_id].setListOfIntegerProperty(property, internal_id, int_vector);
                                        break;
                                    case simdjson::dom::element_type::UINT64:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            int_vector.emplace_back(static_cast<std::make_signed_t<uint64_t>>(child));
                                        }
                                        node_properties[type_id].setListOfIntegerProperty(property, internal_id, int_vector);
                                        break;
                                    case simdjson::dom::element_type::DOUBLE:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            double_vector.emplace_back(double(child));
                                        }
                                        node_properties[type_id].setListOfDoubleProperty(property, internal_id, double_vector);
                                        break;
                                    case simdjson::dom::element_type::STRING:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            string_vector.emplace_back(child);
                                        }
                                        node_properties[type_id].setListOfStringProperty(property, internal_id, string_vector);
                                        break;
                                    case simdjson::dom::element_type::BOOL:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            bool_vector.emplace_back(bool(child));
                                        }
                                        node_properties[type_id].setListOfBooleanProperty(property, internal_id, bool_vector);
                                        break;
                                    case simdjson::dom::element_type::NULL_VALUE:
                                        // Null Values are not allowed, just ignore them
                                        break;
                                }
                            }
                        }
                    }
                }
                return true;
            }
        }
        return false;
    }

    std::vector<std::string>& NodeTypes::getKeys(uint16_t node_type_id) {
        return keys[node_type_id];
    }

    std::vector<std::vector<Group>>& NodeTypes::getOutgoingRelationships(uint16_t node_type_id) {
        return outgoing_relationships[node_type_id];
    }

    std::vector<std::vector<Group>>& NodeTypes::getIncomingRelationships(uint16_t node_type_id) {
        return incoming_relationships[node_type_id];
    }

    uint64_t NodeTypes::internalToExternal(uint16_t type_id, uint64_t internal_id) const {
        return (((internal_id << TYPE_BITS) + type_id) << SHARD_BITS) + shard_id;
    }

}
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
#include <utility>
#include <simdjson.h>
#include "NodeTypes.h"
#include "Shard.h"


namespace ragedb {

    static const unsigned int SHARD_BITS = 10U;
    static const unsigned int SHARD_MASK = 0x00000000000003FFU;
    static const unsigned int TYPE_BITS = 16U;
    static const unsigned int TYPE_MASK = 0x0000000003FFFFFFU;

    static const std::any tombstone_any = std::any();

    NodeTypes::NodeTypes() : type_to_id(), id_to_type(), shard_id(seastar::this_shard_id()) {
        // start with empty blank type 0
        type_to_id.emplace("", 0);
        id_to_type.emplace_back();
        key_to_node_id.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        keys.emplace_back(std::vector<std::string>());
        properties.emplace_back();
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
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
        properties.clear();
        properties.shrink_to_fit();
        outgoing_relationships.clear();
        outgoing_relationships.shrink_to_fit();
        incoming_relationships.clear();
        incoming_relationships.shrink_to_fit();
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

    uint64_t NodeTypes::internalToExternal(uint16_t type_id, uint64_t internal_id) const {
        return (((internal_id << TYPE_BITS) + type_id) << SHARD_BITS) + shard_id;
    }

    uint64_t NodeTypes::externalToInternal(uint64_t id) {
        return (id >> (TYPE_BITS + SHARD_BITS));
    }

    uint16_t NodeTypes::externalToTypeId(uint64_t id) {
        return (id & TYPE_MASK ) >> SHARD_BITS;
    }

    bool NodeTypes::addTypeId(const std::string& type, uint16_t type_id) {
        auto type_search = type_to_id.find(type);
        if (type_search != type_to_id.end()) {
            // Type already exists
            return false;
        }
        if (ValidTypeId(type_id)) {
            // Id already exists
            return false;
        }
        type_to_id.emplace(type, type_id);
        id_to_type.emplace_back(type);
        key_to_node_id.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        keys.emplace_back(std::vector<std::string>());
        properties.emplace_back(Properties());
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
        deleted_ids.emplace_back(Roaring64Map());
        return true;
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
        properties.emplace_back(Properties());
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
        deleted_ids.emplace_back(Roaring64Map());
        return type_id;
    }

    std::string NodeTypes::getType(const std::string &type) {
        uint16_t type_id = getTypeId(type);
        return getType(type_id);
    }

    std::string NodeTypes::getType(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return id_to_type[type_id];
        }
        // If not found return empty
        return id_to_type[0];
    }

    bool NodeTypes::deleteTypeId(const std::string &type) {
        // TODO: Recycle type links
        uint16_t type_id = getTypeId(type);
        if (ValidTypeId(type_id)) {
            // Before calling this method, all nodes of this type should already be deleted
            // It needs to be handled at the Shard level because we don't own the
            // incoming relationship chains on multiple cores
            if (getCount(type_id) == 0) {
                type_to_id[type] = 0;
                id_to_type[type_id] = "";
                key_to_node_id[type_id].clear();
                keys[type_id].clear();
                properties[type_id].clear();
                outgoing_relationships[type_id].clear();
                incoming_relationships[type_id].clear();
                deleted_ids[type_id].clear();
                return true;
            }
        }
        return false;
    }

    bool NodeTypes::addId(uint16_t type_id, uint64_t internal_id) {
        if (ValidTypeId(type_id)) {
            deleted_ids[type_id].remove(internal_id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool NodeTypes::removeId(uint16_t type_id, uint64_t internal_id) {
        if (ValidTypeId(type_id)) {
            deleted_ids[type_id].add(internal_id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool NodeTypes::containsId(uint16_t type_id, uint64_t internal_id) {
        if (ValidTypeId(type_id)) {
            if (ValidNodeId(type_id, internal_id)) {
                return !deleted_ids[type_id].contains(internal_id);
            }
        }
        // If not valid return false
        return false;
    }

    std::vector<uint64_t> NodeTypes::getIds(uint64_t skip, uint64_t limit) const {
        std::vector<uint64_t> allIds;
        int current = 1;
        // links are internal links, we need to switch to external links
        for (int type_id=1; type_id < id_to_type.size(); type_id++) {
            uint64_t max_id = key_to_node_id[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip) {
                        allIds.emplace_back(internalToExternal(type_id, internal_id));
                    }
                    current++;
                }
            } else {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip) {
                        if (!deleted_ids[type_id].contains(internal_id)) {
                            allIds.emplace_back(internalToExternal(type_id, internal_id));
                        }
                    }
                    current++;
                }
            }
        }
        return allIds;
    }

    std::vector<uint64_t>  NodeTypes::getIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
        std::vector<uint64_t>  allIds;
        if (ValidTypeId(type_id)) {
            int current = 1;
            uint64_t max_id = key_to_node_id[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip) {
                        allIds.emplace_back(internalToExternal(type_id, internal_id));
                    }
                    current++;
                }
            } else {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip) {
                        if (!deleted_ids[type_id].contains(internal_id)) {
                            allIds.emplace_back(internalToExternal(type_id, internal_id));
                        }
                    }
                    current++;
                }
            }
        }
        return allIds;
    }

    std::vector<Node> NodeTypes::getNodes(uint64_t skip, uint64_t limit) {
        std::vector<Node> allNodes;
        int current = 1;
        // links are internal links, we need to switch to external links
        for (int type_id=1; type_id < id_to_type.size(); type_id++) {
            uint64_t max_id = key_to_node_id[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allNodes;
                    }
                    if (current > skip) {
                        allNodes.emplace_back(getNode(type_id, internal_id));
                    }
                    current++;
                }
            } else {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allNodes;
                    }
                    if (current > skip) {
                        if (!deleted_ids[type_id].contains(internal_id)) {
                            allNodes.emplace_back(getNode(type_id, internal_id));
                        }
                    }
                    current++;
                }
            }
        }
        return allNodes;
    }

    std::vector<Node> NodeTypes::getNodes(uint16_t type_id, uint64_t skip, uint64_t limit) {
        std::vector<Node>  allNodes;
        if (ValidTypeId(type_id)) {
            int current = 1;
            uint64_t max_id = key_to_node_id[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allNodes;
                    }
                    if (current > skip) {
                        allNodes.emplace_back(getNode(type_id, internal_id));
                    }
                    current++;
                }
            } else {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allNodes;
                    }
                    if (current > skip) {
                        if (!deleted_ids[type_id].contains(internal_id)) {
                            allNodes.emplace_back(getNode(type_id, internal_id));
                        }
                    }
                    current++;
                }
            }
        }
        return allNodes;
    }

    std::vector<uint64_t>  NodeTypes::getDeletedIds() const {
        std::vector<uint64_t>  allIds;
        // links are internal links, we need to switch to external links
        for (int type_id=1; type_id < id_to_type.size(); type_id++) {
            for (Roaring64MapSetBitForwardIterator iterator = deleted_ids[type_id].begin(); iterator != deleted_ids[type_id].end(); iterator++) {
                allIds.emplace_back(internalToExternal(type_id, iterator.operator*()));
            }
        }

        return allIds;
    }

    bool  NodeTypes::hasDeleted(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return !deleted_ids[type_id].isEmpty();
        }
        return false;
    }

    uint64_t NodeTypes::getDeletedIdsMinimum(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return deleted_ids[type_id].minimum();
        }
        return 0;
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
            return key_to_node_id[type_id].size() - deleted_ids[type_id].cardinality();
        }
        // If not valid return 0
        return 0;
    }

    uint64_t NodeTypes::getDeletedCount(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return deleted_ids[type_id].cardinality();
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

    bool NodeTypes::deleteTypeProperty(uint16_t type_id, const std::string &property) {
        if (ValidTypeId(type_id) ) {
            properties[type_id].removePropertyType(property);
        }

        return false;
    }

    std::map<uint16_t,uint64_t> NodeTypes::getCounts() {
        std::map<uint16_t,uint64_t> counts;
        for (int type_id=1; type_id < type_to_id.size(); type_id++) {
            counts.insert({type_id, key_to_node_id[type_id].size() - deleted_ids[type_id].cardinality()});
        }

        return counts;
    }

    tsl::sparse_map<std::string, uint64_t> &NodeTypes::getKeysToNodeId(uint16_t type_id) {
        return key_to_node_id[type_id];
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

    std::string NodeTypes::getNodeKey(uint16_t type_id, uint64_t internal_id) {
        if(ValidTypeId(type_id)) {
            return keys[type_id][internal_id];
        }
        return id_to_type[0];
    }

    std::map<std::string, std::any> NodeTypes::getNodeProperties(uint16_t type_id, uint64_t internal_id) {
        if(ValidTypeId(type_id)) {
            return properties[type_id].getProperties(internal_id);
        }
        return std::map<std::string, std::any>();
    }

    Node NodeTypes::getNode(uint64_t external_id) {
        return getNode(externalToTypeId(external_id), externalToInternal(external_id), external_id);
    }

    Node NodeTypes::getNode(uint16_t type_id, uint64_t internal_id) {
        return Node( internalToExternal(type_id, internal_id), getType(type_id), getKeys(type_id)[internal_id], getNodeProperties(type_id, internal_id));
    }

    Node NodeTypes::getNode(uint16_t type_id, uint64_t internal_id, uint64_t external_id) {
        return Node( external_id, getType(type_id), getKeys(type_id)[internal_id], getNodeProperties(type_id, internal_id));
    }

    std::any NodeTypes::getNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property) {
        if(ValidTypeId(type_id)) {
            if (ValidNodeId(type_id, internal_id)) {
                return properties[type_id].getProperty(property, internal_id);
            }
        }
        return tombstone_any;
    }

    std::any NodeTypes::getNodeProperty(uint64_t external_id, const std::string &property) {
        return getNodeProperty(externalToTypeId(external_id), externalToInternal(external_id), property);
    }

    Properties &NodeTypes::getNodeTypeProperties(uint16_t type_id) {
        return properties[type_id];
    }

    bool NodeTypes::setNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property, std::any value) {
        // find out what data_type property is supposed to be, cast value to that.

        switch (properties[type_id].getPropertyTypeId(property)) {
            case Properties::getBooleanPropertyType(): {
                return properties[type_id].setBooleanProperty(property, internal_id, std::any_cast<bool>(value));
            }
            case Properties::getIntegerPropertyType(): {
                return properties[type_id].setIntegerProperty(property, internal_id, std::any_cast<int64_t>(value));
            }
            case Properties::getDoublePropertyType(): {
                return properties[type_id].setDoubleProperty(property, internal_id, std::any_cast<double>(value));
            }
            case Properties::getStringPropertyType(): {
                if (value.type() != typeid(std::string)) {
                    return properties[type_id].setStringProperty(property, internal_id, std::string(std::any_cast<const char*>(value)));
                }
                return properties[type_id].setStringProperty(property, internal_id, std::any_cast<std::string>(value));
            }
            case Properties::getBooleanListPropertyType(): {
                return properties[type_id].setListOfBooleanProperty(property, internal_id, std::any_cast<std::vector<bool>>(value));
            }
            case Properties::getIntegerListPropertyType(): {
                return properties[type_id].setListOfIntegerProperty(property, internal_id, std::any_cast<std::vector<int64_t>>(value));
            }
            case Properties::getDoubleListPropertyType(): {
                return properties[type_id].setListOfDoubleProperty(property, internal_id, std::any_cast<std::vector<double>>(value));
            }
            case Properties::getStringListPropertyType(): {
                return properties[type_id].setListOfStringProperty(property, internal_id, std::any_cast<std::vector<std::string>>(value));
            }
        }
        return false;
    }

    bool NodeTypes::setNodeProperty(uint64_t external_id, const std::string &property, std::any value) {
        return setNodeProperty(externalToTypeId(external_id), externalToInternal(external_id), property, std::move(value));
    }

    bool NodeTypes::setNodePropertyFromJson(uint16_t type_id, uint64_t internal_id, const std::string &property, const std::string &json) {
        if (!json.empty()) {
            simdjson::dom::element value;
            simdjson::error_code error = parser.parse(json).get(value);
            if (error == 0U) {
                uint16_t data_type_id = properties[type_id].getPropertyTypeId(property);
                switch (data_type_id) {
                    case Properties::getBooleanPropertyType(): {
                        return properties[type_id].setBooleanProperty(property, internal_id, bool(value));
                    }
                    case Properties::getIntegerPropertyType(): {
                        return properties[type_id].setIntegerProperty(property, internal_id,
                                                                      static_cast<std::make_signed_t<uint64_t>>(value));
                    }
                    case Properties::getDoublePropertyType(): {
                        return properties[type_id].setDoubleProperty(property, internal_id, double(value));
                    }
                    case Properties::getStringPropertyType(): {
                        return properties[type_id].setStringProperty(property, internal_id, std::string(value));
                    }
                    case Properties::getBooleanListPropertyType(): {
                        std::vector<bool> bool_vector;
                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                            bool_vector.emplace_back(bool(child));
                        }
                        return properties[type_id].setListOfBooleanProperty(property, internal_id, bool_vector);
                    }
                    case Properties::getIntegerListPropertyType(): {
                        std::vector<int64_t> int_vector;
                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                            int_vector.emplace_back(int64_t(child));
                        }
                        return properties[type_id].setListOfIntegerProperty(property, internal_id, int_vector);
                    }
                    case Properties::getDoubleListPropertyType(): {
                        std::vector<double> double_vector;
                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                            double_vector.emplace_back(double(child));
                        }
                        return properties[type_id].setListOfDoubleProperty(property, internal_id, double_vector);
                    }
                    case Properties::getStringListPropertyType(): {
                        std::vector<std::string> string_vector;
                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                            string_vector.emplace_back(child);
                        }
                        return properties[type_id].setListOfStringProperty(property, internal_id, string_vector);
                    }
                    default: {
                        return false;
                    }
                }
            }
        }
        return false;
    }

    bool NodeTypes::setNodePropertyFromJson(uint64_t external_id, const std::string &property, const std::string &value) {
        return setNodePropertyFromJson(externalToTypeId(external_id), externalToInternal(external_id), property, value);
    }

    bool NodeTypes::setPropertiesFromJSON(uint16_t type_id, uint64_t internal_id, const std::string& json) {
        if (!json.empty()) {
            // Get the properties
            simdjson::dom::object object;
            simdjson::error_code error = parser.parse(json).get(object);

            if (!error) {
                // Add the node properties
                for (auto[key, value] : object) {
                    auto property = static_cast<std::string>(key);
                    switch (value.type()) {
                        case simdjson::dom::element_type::INT64:
                            properties[type_id].setIntegerProperty(property, internal_id, int64_t(value));
                            break;
                        case simdjson::dom::element_type::UINT64:
                            // Unsigned Integer Values are not allowed, convert to signed
                            properties[type_id].setIntegerProperty(property, internal_id, static_cast<std::make_signed_t<uint64_t>>(value));
                            break;
                        case simdjson::dom::element_type::DOUBLE:
                            properties[type_id].setDoubleProperty(property, internal_id, double(value));
                            break;
                        case simdjson::dom::element_type::STRING:
                            properties[type_id].setStringProperty(property, internal_id, std::string(value));
                            break;
                        case simdjson::dom::element_type::BOOL:
                            properties[type_id].setBooleanProperty(property, internal_id, bool(value));
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
                                        properties[type_id].setListOfIntegerProperty(property, internal_id, int_vector);
                                        break;
                                    case simdjson::dom::element_type::UINT64:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            int_vector.emplace_back(static_cast<std::make_signed_t<uint64_t>>(child));
                                        }
                                        properties[type_id].setListOfIntegerProperty(property, internal_id, int_vector);
                                        break;
                                    case simdjson::dom::element_type::DOUBLE:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            double_vector.emplace_back(double(child));
                                        }
                                        properties[type_id].setListOfDoubleProperty(property, internal_id, double_vector);
                                        break;
                                    case simdjson::dom::element_type::STRING:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            string_vector.emplace_back(child);
                                        }
                                        properties[type_id].setListOfStringProperty(property, internal_id, string_vector);
                                        break;
                                    case simdjson::dom::element_type::BOOL:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            bool_vector.emplace_back(bool(child));
                                        }
                                        properties[type_id].setListOfBooleanProperty(property, internal_id, bool_vector);
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

    bool NodeTypes::deleteNodeProperty(uint64_t external_id, const std::string &property) {
        return deleteNodeProperty(externalToTypeId(external_id), externalToInternal(external_id), property);
    }

    bool NodeTypes::deleteNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property) {
        return properties[type_id].deleteProperty(property, internal_id);
    }

    bool NodeTypes::deleteProperties(uint16_t type_id, uint64_t internal_id) {
        return properties[type_id].deleteProperties(internal_id);
    }

    std::vector<std::string>& NodeTypes::getKeys(uint16_t type_id) {
        return keys[type_id];
    }

    std::vector<std::vector<Group>>& NodeTypes::getOutgoingRelationships(uint16_t type_id) {
        return outgoing_relationships[type_id];
    }

    std::vector<std::vector<Group>>& NodeTypes::getIncomingRelationships(uint16_t type_id) {
        return incoming_relationships[type_id];
    }

}
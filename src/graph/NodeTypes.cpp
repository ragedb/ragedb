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

#include <utility>
#include "NodeTypes.h"
#include "Shard.h"
#include "types/Date.h"


namespace ragedb {

    NodeTypes::NodeTypes() : type_to_id(), id_to_type(), shard_id(seastar::this_shard_id()) {
        // start with empty blank type 0
        type_to_id.emplace("", 0);
        id_to_type.emplace_back();
        key_to_node_id.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        keys.emplace_back(std::vector<std::string>());
        properties.emplace_back();
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
        deleted_ids.emplace_back(roaring::Roaring64Map());
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
        properties.emplace_back();
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
        deleted_ids.emplace_back(roaring::Roaring64Map());
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
        deleted_ids.emplace_back(roaring::Roaring64Map());
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
        auto type_id = type_to_id.size();
        type_to_id.emplace(type, type_id);
        id_to_type.emplace_back(type);
        key_to_node_id.emplace_back(tsl::sparse_map<std::string, uint64_t>());
        keys.emplace_back(std::vector<std::string>());
        properties.emplace_back(Properties());
        outgoing_relationships.emplace_back(std::vector<std::vector<Group>>());
        incoming_relationships.emplace_back(std::vector<std::vector<Group>>());
        deleted_ids.emplace_back(roaring::Roaring64Map());
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
                id_to_type[type_id].clear();
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
            properties[type_id].deleteProperties(internal_id);
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
        // ids are internal ids, we need to switch to external ids
        for (size_t type_id=1; type_id < id_to_type.size(); type_id++) {
            uint64_t max_id = key_to_node_id[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip) {
                        allIds.emplace_back(Shard::internalToExternal(type_id, internal_id));
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
                            allIds.emplace_back(Shard::internalToExternal(type_id, internal_id));
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

            for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                if (current > (skip + limit)) {
                    return allIds;
                }
                if (current > skip) {
                    if (!deleted_ids[type_id].contains(internal_id)) {
                        allIds.emplace_back(Shard::internalToExternal(type_id, internal_id));
                    }
                }
                current++;
            }
        }
        return allIds;
    }

    std::vector<Node> NodeTypes::getNodes(uint64_t skip, uint64_t limit) {
        std::vector<Node> allNodes;
        int current = 1;
        // links are internal links, we need to switch to external links
        for (size_t type_id=1; type_id < id_to_type.size(); type_id++) {
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
        for (size_t type_id=1; type_id < id_to_type.size(); type_id++) {
            for (roaring::Roaring64MapSetBitForwardIterator iterator = deleted_ids[type_id].begin(); iterator != deleted_ids[type_id].end(); ++iterator) {
                allIds.emplace_back(Shard::internalToExternal(type_id, *iterator));
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

    roaring::Roaring64Map& NodeTypes::getDeletedMap(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return deleted_ids[type_id];
        }
        return deleted_ids[0];
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
        for (auto [key, value]: type_to_id) {
          type_ids.insert(value);
        }
        type_ids.erase(0);
        return type_ids;
    }

    bool NodeTypes::deleteTypeProperty(uint16_t type_id, const std::string &property) {
        if (ValidTypeId(type_id) ) {
            return properties[type_id].removePropertyType(property);
        }
        return false;
    }

    std::map<uint16_t,uint64_t> NodeTypes::getCounts() {
        std::map<uint16_t,uint64_t> counts;
        for (size_t type_id=1; type_id < type_to_id.size(); type_id++) {
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
          return getNodeId(type_id, key);
    }

    std::string NodeTypes::getNodeKey(uint16_t type_id, uint64_t internal_id) {
        if(ValidTypeId(type_id)) {
            return keys[type_id][internal_id];
        }
        return id_to_type[0];
    }

    std::map<std::string, property_type_t, std::less<>> NodeTypes::getNodeProperties(uint16_t type_id, uint64_t internal_id) {
        if(ValidTypeId(type_id)) {
            return properties[type_id].getProperties(internal_id);
        }
        return std::map<std::string, property_type_t, std::less<>>();
    }

    Node NodeTypes::getNode(uint64_t external_id) {
        return getNode(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), external_id);
    }

    Node NodeTypes::getNode(uint16_t type_id, uint64_t internal_id) {
        return Node( Shard::internalToExternal(type_id, internal_id), getType(type_id), getKeys(type_id)[internal_id], getNodeProperties(type_id, internal_id));
    }

    Node NodeTypes::getNode(uint16_t type_id, uint64_t internal_id, uint64_t external_id) {
        return Node( external_id, getType(type_id), getKeys(type_id)[internal_id], getNodeProperties(type_id, internal_id));
    }

    property_type_t NodeTypes::getNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property) {
        if(ValidTypeId(type_id)) {
            if (ValidNodeId(type_id, internal_id)) {
                return properties[type_id].getProperty(property, internal_id);
            }
        }
        return property_type_t();
    }

    property_type_t NodeTypes::getNodeProperty(uint64_t external_id, const std::string &property) {
        return getNodeProperty(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), property);
    }

    Properties &NodeTypes::getProperties(uint16_t type_id) {
        return properties[type_id];
    }

    bool NodeTypes::setNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property, const property_type_t& value) {
        // find out what data_type property is supposed to be, cast value to that.

        switch (properties[type_id].getPropertyTypeId(property)) {
            case Properties::boolean_type: {
                return properties[type_id].setBooleanProperty(property, internal_id, get<bool>(value));
            }
            case Properties::integer_type: {
                return properties[type_id].setIntegerProperty(property, internal_id, get<int64_t>(value));
            }
            case Properties::double_type: {
                return properties[type_id].setDoubleProperty(property, internal_id, get<double>(value));
            }
            case Properties::string_type: {
                return properties[type_id].setStringProperty(property, internal_id, get<std::string>(value));
            }
            case Properties::date_type: {
              // Date are stored as doubles
              return properties[type_id].setDateProperty(property, internal_id, get<double>(value));
            }
            case Properties::boolean_list_type: {
                return properties[type_id].setListOfBooleanProperty(property, internal_id, get<std::vector<bool>>(value));
            }
            case Properties::integer_list_type: {
                return properties[type_id].setListOfIntegerProperty(property, internal_id, get<std::vector<int64_t>>(value));
            }
            case Properties::double_list_type: {
                return properties[type_id].setListOfDoubleProperty(property, internal_id, get<std::vector<double>>(value));
            }
            case Properties::string_list_type: {
                return properties[type_id].setListOfStringProperty(property, internal_id, get<std::vector<std::string>>(value));
            }
            case Properties::date_list_type: {
              // Lists of Dates are stored as Lists of Doubles
              return properties[type_id].setListOfDateProperty(property, internal_id, get<std::vector<double>>(value));
            }

        }
        return false;
    }

    bool NodeTypes::setNodeProperty(uint64_t external_id, const std::string &property, const property_type_t& value) {
        return setNodeProperty(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), property, value);
    }

    bool NodeTypes::setProperty(uint16_t data_type_id, simdjson::dom::element value, uint16_t type_id, const std::string &property, uint64_t internal_id) {
      // We are going to check that the property type matches the value type
      // and handle some conversions. If conversions fail, we do not enter a value for that property
      switch (data_type_id) {
        case Properties::boolean_type: {
          // For booleans only allow bool property types
          if (value.is_bool()) {
            return properties[type_id].setBooleanProperty(property, internal_id, bool(value));
          }
          break;
        }

        case Properties::integer_type: {
          if (value.is_int64()) {
            return properties[type_id].setIntegerProperty(property, internal_id, int64_t(value));
          }
          if (value.is_uint64()) {
            // Unsigned Integer Values are not allowed, convert to signed
            return properties[type_id].setIntegerProperty(property, internal_id, static_cast<std::make_signed_t<uint64_t>>(value));
          }
          break;
        }

        case Properties::double_type: {
          switch (value.type()) {
            case simdjson::dom::element_type::INT64:
              return properties[type_id].setDoubleProperty(property, internal_id, static_cast<double>(int64_t(value)));
            case simdjson::dom::element_type::UINT64:
              // Unsigned Integer Values are not allowed, convert to signed
              return properties[type_id].setDoubleProperty(property, internal_id, static_cast<double>(static_cast<std::make_signed_t<uint64_t>>(value)));
            case simdjson::dom::element_type::DOUBLE:
              return properties[type_id].setDoubleProperty(property, internal_id, double(value));
            default:
              break;
          }
        }

        case Properties::string_type: {
          if (value.is_string()) {
            return properties[type_id].setStringProperty(property, internal_id, std::string(value));
          }
          break;
        }

        case Properties::date_type: {
          switch (value.type()) {
            case simdjson::dom::element_type::INT64:
              return properties[type_id].setDateProperty(property, internal_id, static_cast<double>(int64_t(value)));
            case simdjson::dom::element_type::UINT64:
              // Unsigned Integer Values are not allowed, convert to signed
              return properties[type_id].setDateProperty(property, internal_id, static_cast<double>(static_cast<std::make_signed_t<uint64_t>>(value)));
            case simdjson::dom::element_type::DOUBLE:
              return properties[type_id].setDateProperty(property, internal_id, double(value));
            case simdjson::dom::element_type::STRING:
              return properties[type_id].setDateProperty(property, internal_id, Date::convert(std::string(value)));
            default:
              break;
          }
        }

        case Properties::boolean_list_type: {
          if (value.type() == simdjson::dom::element_type::ARRAY) {
            auto array = simdjson::dom::array(value);
              std::vector<bool> bool_vector;
              for (simdjson::dom::element child : simdjson::dom::array(value)) {
                if (child.is_bool()) {
                  bool_vector.emplace_back(bool(child));
                }
              }
              return properties[type_id].setListOfBooleanProperty(property, internal_id, bool_vector);
          }
          break;
        }

        case Properties::integer_list_type: {
          if (value.type() == simdjson::dom::element_type::ARRAY) {
            auto array = simdjson::dom::array(value);
            std::vector<int64_t> int_vector;
            for (simdjson::dom::element child : simdjson::dom::array(value)) {
              if (child.is_int64()) {
                int_vector.emplace_back(int64_t(child));
              }
              if (child.is_uint64()) {
                // Unsigned Integer Values are not allowed, convert to signed
                int_vector.emplace_back(static_cast<std::make_signed_t<uint64_t>>(child));
              }
            }
            return properties[type_id].setListOfIntegerProperty(property, internal_id, int_vector);
          }
          break;
        }

        case Properties::double_list_type: {
          if (value.type() == simdjson::dom::element_type::ARRAY) {
            auto array = simdjson::dom::array(value);
            std::vector<double> double_vector;
            for (simdjson::dom::element child : simdjson::dom::array(value)) {
              switch (child.type()) {
                case simdjson::dom::element_type::INT64:
                  double_vector.emplace_back(static_cast<double>(int64_t(value)));
                case simdjson::dom::element_type::UINT64:
                  // Unsigned Integer Values are not allowed, convert to signed
                  double_vector.emplace_back( static_cast<double>(static_cast<std::make_signed_t<uint64_t>>(value)));
                case simdjson::dom::element_type::DOUBLE:
                  double_vector.emplace_back( double(value));
                default:
                  break;
              }
            }
            return properties[type_id].setListOfDoubleProperty(property, internal_id, double_vector);
          }
          break;
        }

        case Properties::string_list_type: {
          if (value.type() == simdjson::dom::element_type::ARRAY) {
            auto array = simdjson::dom::array(value);
            std::vector<std::string> string_vector;
            for (simdjson::dom::element child : simdjson::dom::array(value)) {
              if (value.is_string()) {
                string_vector.emplace_back(std::string(child));
              }
            }
            return properties[type_id].setListOfStringProperty(property, internal_id, string_vector);
          }
          break;
        }

        case Properties::date_list_type: {
          if (value.type() == simdjson::dom::element_type::ARRAY) {
            std::vector<double> double_vector;
            for (simdjson::dom::element child : simdjson::dom::array(value)) {
              switch (child.type()) {
              case simdjson::dom::element_type::INT64:
                double_vector.emplace_back(static_cast<double>(int64_t(value)));
              case simdjson::dom::element_type::UINT64:
                // Unsigned Integer Values are not allowed, convert to signed
                double_vector.emplace_back(static_cast<double>(static_cast<std::make_signed_t<uint64_t>>(value)));
              case simdjson::dom::element_type::DOUBLE:
                double_vector.emplace_back(double(value));
              case simdjson::dom::element_type::STRING:
                double_vector.emplace_back(Date::convert(std::string(value)));
              default:
                break;
              }
            }
            return properties[type_id].setListOfDateProperty(property, internal_id, double_vector);
          }
        }
        default: {
          break;
        }
      }

      // The property value given was invalid for the type, set the property as deleted and return false
      properties[type_id].deleteProperty(property, internal_id);
      return false;
    }

    bool NodeTypes::setNodePropertyFromJson(uint16_t type_id, uint64_t internal_id, const std::string &property, const std::string &json) {
        if (!json.empty()) {
            simdjson::dom::element value;
            simdjson::error_code error = parser.parse(json).get(value);
            if (error == 0U) {
                uint16_t data_type_id = properties[type_id].getPropertyTypeId(property);
                return setProperty(data_type_id, value, type_id, property, internal_id);
            }
        }
        return false;
    }

    bool NodeTypes::setNodePropertyFromJson(uint64_t external_id, const std::string &property, const std::string &value) {
        return setNodePropertyFromJson(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), property, value);
    }

    bool NodeTypes::setPropertiesFromJSON(uint16_t type_id, uint64_t internal_id, const std::string& json) {
        if (!json.empty()) {
            // Get the properties
            simdjson::dom::object object;
            simdjson::error_code error = parser.parse(json).get(object);
            if (!error) {
                // Add the node properties
                int valid = 0;
                for (auto[key, value] : object) {
                    auto property = static_cast<std::string>(key);
                    const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
                    // If the property type exists at all
                    if (property_type_id > 0) {
                      // True is 1, so if we set all the properties correctly then valid will equal the object size
                        valid += setProperty(property_type_id, value, type_id, property, internal_id);
                    }
                }
                return valid == object.size();
            }
        }
        return false;
    }

    bool NodeTypes::deleteNodeProperty(uint64_t external_id, const std::string &property) {
        return deleteNodeProperty(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), property);
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
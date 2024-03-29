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

#include <simdjson.h>
#include "RelationshipTypes.h"
#include "Properties.h"
#include "Shard.h"
#include "types/Date.h"

namespace ragedb {

    RelationshipTypes::RelationshipTypes() {
        // start with empty blank type
        type_to_id.try_emplace("", 0);
        id_to_type.emplace_back("");
        properties.emplace_back();
        starting_node_ids.emplace_back();
        ending_node_ids.emplace_back();
        deleted_ids.emplace_back();
    }

    void RelationshipTypes::Clear() {
        type_to_id.clear();
        id_to_type.clear();
        id_to_type.shrink_to_fit();
        starting_node_ids.clear();
        starting_node_ids.shrink_to_fit();
        ending_node_ids.clear();
        ending_node_ids.shrink_to_fit();
        properties.clear();
        properties.shrink_to_fit();
        deleted_ids.clear();

        // start with empty blank type
        type_to_id.try_emplace("", 0);
        id_to_type.emplace_back("");
        properties.emplace_back();
        starting_node_ids.emplace_back();
        ending_node_ids.emplace_back();
        deleted_ids.emplace_back();
    }

    bool RelationshipTypes::addTypeId(const std::string &type, uint16_t type_id) {
        if (type_to_id.contains(type)) {
          // Type already exists
          return false;
        }

        if (ValidTypeId(type_id)) {
            // Invalid
            return false;
        }
        type_to_id.try_emplace(type, type_id);
        id_to_type.emplace_back(type);
        starting_node_ids.emplace_back();
        ending_node_ids.emplace_back();
        properties.emplace_back();
        deleted_ids.emplace_back();
        return false;
    }

    uint16_t RelationshipTypes::getTypeId(const std::string &type) const {
        if (auto type_search = type_to_id.find(type); type_search != type_to_id.end()) {
          return type_search->second;
        }
        return 0;
    }

    uint16_t RelationshipTypes::insertOrGetTypeId(const std::string &type) {
        // Get
        if (auto type_search = type_to_id.find(type); type_search != type_to_id.end()) {
          return type_search->second;
        }
        // Insert
        uint16_t type_id = type_to_id.size();
        type_to_id.try_emplace(type, type_id);
        id_to_type.emplace_back(type);
        starting_node_ids.emplace_back();
        ending_node_ids.emplace_back();
        properties.emplace_back();
        deleted_ids.emplace_back();
        return type_id;
    }

    std::string RelationshipTypes::getType(const std::string &type) const {
        uint16_t type_id = getTypeId(type);
        return getType(type_id);
    }

    std::string RelationshipTypes::getType(uint16_t type_id) const {
        if (ValidTypeId(type_id)) {
            return id_to_type[type_id];
        }
        // If not found return empty
        return id_to_type[0];
    }

    uint64_t RelationshipTypes::getStartingNodeId(uint16_t type_id, uint64_t internal_id) const {
        if (ValidTypeId(type_id)) {
            return starting_node_ids[type_id][internal_id];
        }
        // Invalid Node
        return 0;
    }

    uint64_t RelationshipTypes::getEndingNodeId(uint16_t type_id, uint64_t internal_id) const {
        if (ValidTypeId(type_id)) {
            return ending_node_ids[type_id][internal_id];
        }
        // Invalid Node
        return 0;
    }

    bool RelationshipTypes::setStartingNodeId(uint16_t type_id, uint64_t internal_id, uint64_t external_id) {
        if (ValidTypeId(type_id)) {
            if (starting_node_ids[type_id].size() <= internal_id) {
                starting_node_ids[type_id].resize(internal_id + 1);
            }
            starting_node_ids[type_id][internal_id] = external_id;
            return true;
        }
        return false;
    }

    bool RelationshipTypes::setEndingNodeId(uint16_t type_id, uint64_t internal_id, uint64_t external_id) {
        if (ValidTypeId(type_id)) {
            if (ending_node_ids[type_id].size() <= internal_id) {
                ending_node_ids[type_id].resize(internal_id + 1);
            }
            ending_node_ids[type_id][internal_id] = external_id;
            return true;
        }
        return false;
    }

    bool RelationshipTypes::deleteTypeId(const std::string &type) {
        // TODO: Recycle type links
        if (uint16_t type_id = getTypeId(type); ValidTypeId(type_id) && getCount(type_id) == 0) {
            type_to_id[type] = 0;
            id_to_type[type_id].clear();
            starting_node_ids[type_id].clear();
            ending_node_ids[type_id].clear();
            properties[type_id].clear();
            deleted_ids[type_id].clear();
            return true;
        }
        return false;
    }

    bool RelationshipTypes::addId(uint16_t type_id, uint64_t internal_id) {
        if (ValidTypeId(type_id)) {
            deleted_ids[type_id].remove(internal_id);
            properties[type_id].deleteProperties(internal_id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool RelationshipTypes::removeId(uint16_t type_id, uint64_t internal_id) {
        if (ValidTypeId(type_id)) {
            deleted_ids[type_id].add(internal_id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool RelationshipTypes::containsId(uint16_t type_id, uint64_t internal_id) {
        if (ValidTypeId(type_id) && ValidRelationshipId(type_id, internal_id)) {
            return !deleted_ids[type_id].contains(internal_id);
        }
        // If not valid return false
        return false;
    }

    std::vector<uint64_t> RelationshipTypes::getIds(uint64_t skip, uint64_t limit) const {
        std::vector<uint64_t> allIds;
        int current = 1;
        // links are internal links, we need to switch to external links
        for (size_t type_id=1; type_id < id_to_type.size(); type_id++) {
            uint64_t max_id = starting_node_ids[type_id].size();
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

    std::vector<uint64_t>  RelationshipTypes::getIds(uint16_t type_id, uint64_t skip, uint64_t limit) const {
        std::vector<uint64_t>  allIds;
        int current = 1;
        if (ValidTypeId(type_id)) {
            uint64_t max_id = starting_node_ids[type_id].size();
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

    std::vector<Relationship> RelationshipTypes::getRelationships(uint64_t skip, uint64_t limit) const {
        std::vector<Relationship> allRelationships;
        int current = 1;
        // links are internal links, we need to switch to external links
        for (size_t type_id=1; type_id < id_to_type.size(); type_id++) {
            uint64_t max_id = starting_node_ids[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allRelationships;
                    }
                    if (current > skip) {
                        allRelationships.emplace_back(getRelationship(type_id, internal_id));
                    }
                    current++;
                }
            } else {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allRelationships;
                    }
                    if (current > skip) {
                        if (!deleted_ids[type_id].contains(internal_id)) {
                            allRelationships.emplace_back(getRelationship(type_id, internal_id));
                        }
                    }
                    current++;
                }
            }
        }
        return allRelationships;
    }

    std::vector<Relationship> RelationshipTypes::getRelationships(uint16_t type_id, uint64_t skip, uint64_t limit) const {
        std::vector<Relationship>  allRelationships;
        int current = 1;
        if (ValidTypeId(type_id)) {
            uint64_t max_id = starting_node_ids[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allRelationships;
                    }
                    if (current > skip) {
                        allRelationships.emplace_back(getRelationship(type_id, internal_id));
                    }
                    current++;
                }
            } else {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allRelationships;
                    }
                    if (current > skip) {
                        if (!deleted_ids[type_id].contains(internal_id)) {
                            allRelationships.emplace_back(getRelationship(type_id, internal_id));
                        }
                    }
                    current++;
                }
            }
        }
        return allRelationships;
    }

    std::vector<uint64_t>  RelationshipTypes::getDeletedIds() const {
        std::vector<uint64_t>  allIds;
        // links are internal links, we need to switch to external links
        for (size_t type_id=1; type_id < id_to_type.size(); type_id++) {
            for (auto iterator = deleted_ids[type_id].begin(); iterator != deleted_ids[type_id].end(); ++iterator) {
                allIds.emplace_back(Shard::internalToExternal(type_id, *iterator));
            }
        }

        return allIds;
    }

    bool  RelationshipTypes::hasDeleted(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return !deleted_ids[type_id].isEmpty();
        }
        return false;
    }

    uint64_t RelationshipTypes::getDeletedIdsMinimum(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return deleted_ids[type_id].minimum();
        }
        return 0;
    }

    bool RelationshipTypes::ValidTypeId(uint16_t type_id) const {
        // TypeId must be greater than zero
        return (type_id > 0 && type_id < id_to_type.size());
    }

    bool RelationshipTypes::ValidRelationshipId(uint16_t type_id, uint64_t internal_id) const {
        // If the type is valid, is the internal id within the vector size and is it not deleted?
        if (ValidTypeId(type_id)) {
            return starting_node_ids[type_id].size() > internal_id && !deleted_ids[type_id].contains(internal_id);
        }
        return false;
    }

    uint64_t RelationshipTypes::getCount(uint16_t type_id) const {
        if (ValidTypeId(type_id)) {
            return starting_node_ids[type_id].size() - deleted_ids[type_id].cardinality();
        }
        // If not valid return 0
        return 0;
    }

    uint64_t RelationshipTypes::getDeletedCount(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return deleted_ids[type_id].cardinality();
        }
        // If not valid return 0
        return 0;
    }

    roaring::Roaring64Map& RelationshipTypes::getDeletedMap(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return deleted_ids[type_id];
        }
        return deleted_ids[0];
    }

    uint16_t RelationshipTypes::getSize() const {
        return id_to_type.size();
    }

    std::set<std::string> RelationshipTypes::getTypes() {
        // Skip the empty type
        return {id_to_type.begin() + 1, id_to_type.end()};
    }

    std::set<uint16_t> RelationshipTypes::getTypeIds() const {
      std::set<uint16_t> type_ids;
      for (const auto& [key, value]: type_to_id) {
        type_ids.insert(value);
      }
      type_ids.erase(0);
      return type_ids;
    }

    bool RelationshipTypes::deleteTypeProperty(uint16_t type_id, const std::string &property) {
        if (ValidTypeId(type_id) ) {
            return properties[type_id].removePropertyType(property);
        }

        return false;
    }

    std::map<uint16_t, uint64_t> RelationshipTypes::getCounts() const {
        std::map<uint16_t,uint64_t> counts;
        for (size_t type_id=1; type_id < type_to_id.size(); type_id++) {
            counts.insert({type_id, starting_node_ids[type_id].size() - deleted_ids[type_id].cardinality()});
        }

        return counts;
    }

    std::map<std::string, property_type_t> RelationshipTypes::getRelationshipProperties(uint64_t external_id) const {
        return getRelationshipProperties(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id));
    }

    std::map<std::string, property_type_t> RelationshipTypes::getRelationshipProperties(uint16_t type_id, uint64_t internal_id) const {
        if(ValidTypeId(type_id)) {
            return properties.at(type_id).getProperties(internal_id);
        }
        return std::map<std::string, property_type_t>();
    }

    std::map<uint64_t, property_type_t> RelationshipTypes::getRelationshipsProperty(uint16_t type_id, const std::vector<uint64_t> &external_ids, const std::string& property) const {
        if(ValidTypeId(type_id)) {
            // Get internal ids
            std::vector<uint64_t> internal_ids;
            internal_ids.reserve(external_ids.size());
            for (const auto& id : external_ids) {
                internal_ids.emplace_back(Shard::externalToInternal(id));
            }
            return properties[type_id].getProperty(external_ids, internal_ids, property);
        }
        return std::map<uint64_t, property_type_t>();
    }

    std::map<uint64_t, std::map<std::string, property_type_t>> RelationshipTypes::getRelationshipsProperties(uint16_t type_id, const std::vector<uint64_t> &external_ids) const {
        if(ValidTypeId(type_id)) {
            // Get internal ids
            std::vector<uint64_t> internal_ids;
            internal_ids.reserve(external_ids.size());
            for (const auto& id : external_ids) {
                internal_ids.emplace_back(Shard::externalToInternal(id));
            }
            return properties[type_id].getProperties(external_ids, internal_ids);
        }
        return std::map<uint64_t, std::map<std::string, property_type_t>>();
    }

    Relationship RelationshipTypes::getRelationship(uint64_t external_id) const {
        return getRelationship(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), external_id);
    }

    Relationship RelationshipTypes::getRelationship(uint16_t type_id, uint64_t internal_id) const {
        return Relationship(Shard::internalToExternal(type_id, internal_id), getType(type_id), getStartingNodeId(type_id,internal_id), getEndingNodeId(type_id,internal_id), getRelationshipProperties(type_id, internal_id));
    }

    Relationship RelationshipTypes::getRelationship(uint16_t type_id, uint64_t internal_id, uint64_t external_id) const {
        return Relationship(external_id, getType(type_id), getStartingNodeId(type_id,internal_id), getEndingNodeId(type_id,internal_id), getRelationshipProperties(type_id, internal_id));
    }

    property_type_t RelationshipTypes::getRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property) {
        if(ValidTypeId(type_id) && ValidRelationshipId(type_id, internal_id)) {
            return properties[type_id].getProperty(property, internal_id);
        }
        return property_type_t();
    }

    property_type_t RelationshipTypes::getRelationshipProperty(uint64_t external_id, const std::string &property) {
        return getRelationshipProperty(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), property);
    }

    std::vector<uint64_t> &RelationshipTypes::getStartingNodeIds(uint16_t type_id) {
        return starting_node_ids[type_id];
    }

    std::vector<uint64_t> &RelationshipTypes::getEndingNodeIds(uint16_t type_id) {
        return ending_node_ids[type_id];
    }

    bool RelationshipTypes::setProperty(uint16_t data_type_id, simdjson::dom::element value, uint16_t type_id, const std::string &property, uint64_t internal_id) {
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

    bool RelationshipTypes::setRelationshipPropertyFromJson(uint16_t type_id, uint64_t internal_id, const std::string &property,
                                                            const std::string &json) {
        if (json.empty()) {
            return false;
        }

        // Get the properties
        simdjson::dom::element value;
        simdjson::error_code error = parser.parse(json).get(value);
        if (error == 0U) {
            uint16_t data_type_id = properties[type_id].getPropertyTypeId(property);
            return setProperty(data_type_id, value, type_id, property, internal_id);
        }

        return false;
    }

    bool RelationshipTypes::setRelationshipPropertyFromJson(uint64_t external_id, const std::string &property, const std::string &value) {
        return setRelationshipPropertyFromJson(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), property, value);
    }

    bool RelationshipTypes::deleteRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property) {
        return properties[type_id].deleteProperty(property, internal_id);
    }
    bool RelationshipTypes::deleteRelationshipProperty(uint64_t external_id, const std::string &property) {
        return deleteRelationshipProperty(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), property);
    }

    Properties &RelationshipTypes::getProperties(uint16_t type_id) {
        return properties[type_id];
    }

    bool RelationshipTypes::setRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property, const property_type_t& value) {
        // find out what data_type property is supposed to be, cast value to that.

        switch (properties[type_id].getPropertyTypeId(property)) {
        case Properties::boolean_type: return properties[type_id].setBooleanProperty(property, internal_id, get<bool>(value));
        case Properties::integer_type: return properties[type_id].setIntegerProperty(property, internal_id, get<int64_t>(value));
        case Properties::double_type: return properties[type_id].setDoubleProperty(property, internal_id, get<double>(value));
        case Properties::string_type: return properties[type_id].setStringProperty(property, internal_id, get<std::string>(value));
        case Properties::date_type: return properties[type_id].setDateProperty(property, internal_id, get<double>(value)); // Date are stored as doubles
        case Properties::boolean_list_type: return properties[type_id].setListOfBooleanProperty(property, internal_id, get<std::vector<bool>>(value));
        case Properties::integer_list_type: return properties[type_id].setListOfIntegerProperty(property, internal_id, get<std::vector<int64_t>>(value));
        case Properties::double_list_type: return properties[type_id].setListOfDoubleProperty(property, internal_id, get<std::vector<double>>(value));
        case Properties::string_list_type: return properties[type_id].setListOfStringProperty(property, internal_id, get<std::vector<std::string>>(value));
        case Properties::date_list_type: return properties[type_id].setListOfDateProperty(property, internal_id, get<std::vector<double>>(value)); // Lists of Dates are stored as Lists of Doubles
        default: return false;
        }
        return false;
    }

    bool RelationshipTypes::setRelationshipProperty(uint64_t external_id, const std::string &property, const property_type_t& value) {
        return setRelationshipProperty(Shard::externalToTypeId(external_id), Shard::externalToInternal(external_id), property, value);
    }

    bool RelationshipTypes::setPropertiesFromJSON(uint16_t type_id, uint64_t internal_id, const std::string& json) {
        if (json.empty()) {
            return false;
        }

        // Get the properties
        simdjson::dom::object object;
        simdjson::error_code error = parser.parse(json).get(object);

        if (!error) {
            // Add the relationship properties
            int valid = 0;
            for (const auto& [key, value] : object) {
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

        return false;
    }

    bool RelationshipTypes::deleteProperties(uint16_t type_id, uint64_t internal_id) {
        return properties[type_id].deleteProperties(internal_id);
    }
}
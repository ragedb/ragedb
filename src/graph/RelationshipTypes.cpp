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


namespace ragedb {

    RelationshipTypes::RelationshipTypes() : type_to_id(), id_to_type() {
        // start with empty blank type
        type_to_id.emplace("", 0);
        id_to_type.emplace_back("");
        relationships.emplace_back();
        relationship_properties.emplace_back();
        ids.emplace_back(Roaring64Map());
        deleted_ids.emplace_back(Roaring64Map());
    }

    void RelationshipTypes::Clear() {
        type_to_id.clear();
        id_to_type.clear();
        id_to_type.shrink_to_fit();
        relationships.clear();
        relationships.shrink_to_fit();
        relationship_properties.clear();
        relationship_properties.shrink_to_fit();
        ids.clear();
        deleted_ids.clear();

        // start with empty blank type
        type_to_id.emplace("", 0);
        id_to_type.emplace_back("");
        relationships.emplace_back();
        relationship_properties.emplace_back();
        ids.emplace_back(Roaring64Map());
        deleted_ids.emplace_back(Roaring64Map());
    }

    uint16_t RelationshipTypes::getTypeId(const std::string &type) {
        auto type_search = type_to_id.find(type);
        if (type_search != type_to_id.end()) {
            return type_search->second;
        }
        return 0;
    }

    uint16_t RelationshipTypes::insertOrGetTypeId(const std::string &type) {
        // Get
        auto type_search = type_to_id.find(type);
        if (type_search != type_to_id.end()) {
            return type_search->second;
        }
        // Insert
        uint16_t type_id = type_to_id.size();
        type_to_id.emplace(type, type_id);
        id_to_type.emplace_back(type);
        ids.emplace_back(Roaring64Map());
        deleted_ids.emplace_back(Roaring64Map());
        return type_id;
    }

    std::string RelationshipTypes::getType(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return id_to_type[type_id];
        }
        // If not found return empty
        return id_to_type[0];
    }

    bool RelationshipTypes::addId(uint16_t type_id, uint64_t id) {
        if (ValidTypeId(type_id)) {
            ids.at(type_id).add(id);
            deleted_ids.at(type_id).remove(id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool RelationshipTypes::removeId(uint16_t type_id, uint64_t id) {
        if (ValidTypeId(type_id)) {
            ids.at(type_id).remove(id);
            deleted_ids.at(type_id).add(id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool RelationshipTypes::containsId(uint16_t type_id, uint64_t id) {
        if (ValidTypeId(type_id)) {
            return ids.at(type_id).contains(id);
        }
        // If not valid return false
        return false;
    }

    Roaring64Map RelationshipTypes::getIds() const {
        Roaring64Map allIds;
        for (int i=1; i < type_to_id.size(); i++) {
            allIds.operator|=(ids[i]);
        }
        return allIds;
    }

    Roaring64Map RelationshipTypes::getIds(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return ids.at(type_id);
        }
        return ids.at(0);
    }

    Roaring64Map RelationshipTypes::getDeletedIds() const {
        Roaring64Map allIds;
        for (int i=1; i < type_to_id.size(); i++) {
            allIds.operator|=(deleted_ids[i]);
        }
        return allIds;
    }

    Roaring64Map RelationshipTypes::getDeletedIds(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return deleted_ids.at(type_id);
        }
        return deleted_ids.at(0);
    }

    bool RelationshipTypes::ValidTypeId(uint16_t type_id) const {
        // TypeId must be greater than zero
        return (type_id > 0 && type_id < id_to_type.size());
    }

    uint64_t RelationshipTypes::getCount(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return ids.at(type_id).cardinality();
        }
        // If not valid return 0
        return 0;
    }

    uint16_t RelationshipTypes::getSize() const {
        return id_to_type.size() - 1;
    }

    std::set<std::string> RelationshipTypes::getTypes() {
        // Skip the empty type
        return {id_to_type.begin() + 1, id_to_type.end()};
    }

    std::set<uint16_t> RelationshipTypes::getTypeIds() {
        std::set<uint16_t> type_ids;
        for (int i=1; i < type_to_id.size(); i++) {
            type_ids.insert(i);
        }

        return type_ids;
    }

    std::map<uint16_t, uint64_t> RelationshipTypes::getCounts() {
        std::map<uint16_t,uint64_t> counts;
        for (int i=1; i < type_to_id.size(); i++) {
            counts.insert({i, ids[i].cardinality()});
        }

        return counts;
    }

    bool RelationshipTypes::addTypeId(const std::string &type, uint16_t type_id) {
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
            ids.emplace_back(Roaring64Map());
            deleted_ids.emplace_back(Roaring64Map());
            return false;
        }
    }

    Properties &RelationshipTypes::getRelationshipTypeProperties(uint16_t type_id) {
        return relationship_properties[type_id];
    }

    bool RelationshipTypes::setProperties(uint16_t type_id, uint64_t internal_id, const std::string& properties) {
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
                            relationship_properties[type_id].setIntegerProperty(property, internal_id, int64_t(value));
                            break;
                        case simdjson::dom::element_type::UINT64:
                            // Unsigned Integer Values are not allowed, convert to signed
                            relationship_properties[type_id].setIntegerProperty(property, internal_id, static_cast<std::make_signed_t<uint64_t>>(value));
                            break;
                        case simdjson::dom::element_type::DOUBLE:
                            relationship_properties[type_id].setDoubleProperty(property, internal_id, double(value));
                            break;
                        case simdjson::dom::element_type::STRING:
                            relationship_properties[type_id].setStringProperty(property, internal_id, std::string(value));
                            break;
                        case simdjson::dom::element_type::BOOL:
                            relationship_properties[type_id].setBooleanProperty(property, internal_id, bool(value));
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
                                        relationship_properties[type_id].setListOfIntegerProperty(property, internal_id, int_vector);
                                        break;
                                    case simdjson::dom::element_type::UINT64:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            int_vector.emplace_back(static_cast<std::make_signed_t<uint64_t>>(child));
                                        }
                                        relationship_properties[type_id].setListOfIntegerProperty(property, internal_id, int_vector);
                                        break;
                                    case simdjson::dom::element_type::DOUBLE:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            double_vector.emplace_back(double(child));
                                        }
                                        relationship_properties[type_id].setListOfDoubleProperty(property, internal_id, double_vector);
                                        break;
                                    case simdjson::dom::element_type::STRING:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            string_vector.emplace_back(child);
                                        }
                                        relationship_properties[type_id].setListOfStringProperty(property, internal_id, string_vector);
                                        break;
                                    case simdjson::dom::element_type::BOOL:
                                        for (simdjson::dom::element child : simdjson::dom::array(value)) {
                                            bool_vector.emplace_back(bool(child));
                                        }
                                        relationship_properties[type_id].setListOfBooleanProperty(property, internal_id, bool_vector);
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
}
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

namespace ragedb {

    static const unsigned int SHARD_BITS = 10U;
    static const unsigned int SHARD_MASK = 0x00000000000003FFU;
    static const unsigned int TYPE_BITS = 16U;
    static const unsigned int TYPE_MASK = 0x0000000003FFFFFFU;

    static const std::any tombstone_any = std::any();

    RelationshipTypes::RelationshipTypes() : type_to_id(), id_to_type(), shard_id(seastar::this_shard_id()) {
        // start with empty blank type
        type_to_id.emplace("", 0);
        id_to_type.emplace_back("");
        relationships.emplace_back();
        relationship_properties.emplace_back();
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
        deleted_ids.clear();

        // start with empty blank type
        type_to_id.emplace("", 0);
        id_to_type.emplace_back("");
        relationships.emplace_back();
        relationship_properties.emplace_back();
        deleted_ids.emplace_back(Roaring64Map());
    }

    uint64_t RelationshipTypes::internalToExternal(uint16_t type_id, uint64_t internal_id) const {
        return (((internal_id << TYPE_BITS) + type_id) << SHARD_BITS) + shard_id;
    }

    uint64_t RelationshipTypes::externalToInternal(uint64_t id) {
        return (id >> (TYPE_BITS + SHARD_BITS));
    }

    uint16_t RelationshipTypes::externalToTypeId(uint64_t id) {
        return (id & TYPE_MASK ) >> SHARD_BITS;
    }

    bool RelationshipTypes::addTypeId(const std::string &type, uint16_t type_id) {
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
        deleted_ids.emplace_back(Roaring64Map());
        return false;
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
        deleted_ids.emplace_back(Roaring64Map());
        return type_id;
    }

    std::string RelationshipTypes::getType(const std::string &type) {
        uint16_t type_id = getTypeId(type);
        return getType(type_id);
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
            deleted_ids[type_id].remove(id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool RelationshipTypes::removeId(uint16_t type_id, uint64_t id) {
        if (ValidTypeId(type_id)) {
            deleted_ids[type_id].add(id);
            return true;
        }
        // If not valid return false
        return false;
    }

    bool RelationshipTypes::containsId(uint16_t type_id, uint64_t id) {
        if (ValidTypeId(type_id)) {
            if (ValidRelationshipId(type_id, id)) {
                return !deleted_ids[type_id].contains(id);
            }
        }
        // If not valid return false
        return false;
    }

    std::vector<uint64_t> RelationshipTypes::getIds(uint64_t skip, uint64_t limit) const {
        std::vector<uint64_t> allIds;
        int current = 1;
        // ids are internal ids, we need to switch to external ids
        for (int type_id=1; type_id < id_to_type.size(); type_id++) {
            uint64_t max_id = from[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip && current <= (skip + limit)) {
                        internalToExternal(type_id, internal_id);
                    }
                    current++;
                }
            } else {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip && current <= (skip + limit)) {
                        if (!deleted_ids[type_id].contains(internal_id)) {
                            internalToExternal(type_id, internal_id);
                        }
                    }
                    current++;
                }
            }
        }
        return allIds;
    }

    std::vector<uint64_t>  RelationshipTypes::getIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
        std::vector<uint64_t>  allIds;
        int current = 1;
        if (ValidTypeId(type_id)) {
            uint64_t max_id = from[type_id].size();
            if (deleted_ids[type_id].isEmpty()) {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip && current <= (skip + limit)) {
                        internalToExternal(type_id, internal_id);
                    }
                    current++;
                }
            } else {
                for (uint64_t internal_id=0; internal_id < max_id; ++internal_id) {
                    if (current > (skip + limit)) {
                        return allIds;
                    }
                    if (current > skip && current <= (skip + limit)) {
                        if (!deleted_ids[type_id].contains(internal_id)) {
                            internalToExternal(type_id, internal_id);
                        }
                    }
                    current++;
                }
            }
        }
        return allIds;
    }

    std::vector<uint64_t>  RelationshipTypes::getDeletedIds() const {
        std::vector<uint64_t>  allIds;
        // ids are internal ids, we need to switch to external ids
        for (int type_id=1; type_id < id_to_type.size(); type_id++) {
            for (Roaring64MapSetBitForwardIterator iterator = deleted_ids[type_id].begin(); iterator != deleted_ids[type_id].end(); iterator++) {
                allIds.emplace_back(internalToExternal(type_id, iterator.operator*()));
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
            for (Roaring64MapSetBitForwardIterator iterator = deleted_ids[type_id].begin(); iterator != deleted_ids[type_id].end(); iterator++) {
                return iterator.operator*();
            }
        }
        return 0;
    }

    bool RelationshipTypes::ValidTypeId(uint16_t type_id) const {
        // TypeId must be greater than zero
        return (type_id > 0 && type_id < id_to_type.size());
    }

    bool RelationshipTypes::ValidRelationshipId(uint16_t type_id, uint64_t internal_id) {
        // If the type is valid, is the internal id within the vector size and is it not deleted?
        if (ValidTypeId(type_id)) {
            return from[type_id].size() > internal_id && !deleted_ids[type_id].contains(internal_id);
        }
        return false;
    }

    uint64_t RelationshipTypes::getCount(uint16_t type_id) {
        if (ValidTypeId(type_id)) {
            return from[type_id].size() - deleted_ids[type_id].cardinality();
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
        for (int type_id=1; type_id < type_to_id.size(); type_id++) {
            counts.insert({type_id, from[type_id].size() - deleted_ids[type_id].cardinality()});
        }

        return counts;
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
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
        properties.emplace_back();
        starting_node_ids.emplace_back();
        ending_node_ids.emplace_back();
        deleted_ids.emplace_back(roaring::Roaring64Map());
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
        type_to_id.emplace("", 0);
        id_to_type.emplace_back("");
        properties.emplace_back();
        starting_node_ids.emplace_back();
        ending_node_ids.emplace_back();
        deleted_ids.emplace_back(roaring::Roaring64Map());
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
            // Invalid
            return false;
        }
        type_to_id.emplace(type, type_id);
        id_to_type.emplace_back(type);
        starting_node_ids.emplace_back();
        ending_node_ids.emplace_back();
        properties.emplace_back(Properties());
        deleted_ids.emplace_back(roaring::Roaring64Map());
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
        auto type_id = type_to_id.size();
        type_to_id.emplace(type, type_id);
        id_to_type.emplace_back(type);
        starting_node_ids.emplace_back();
        ending_node_ids.emplace_back();
        properties.emplace_back(Properties());
        deleted_ids.emplace_back(roaring::Roaring64Map());
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

    uint64_t RelationshipTypes::getStartingNodeId(uint16_t type_id, uint64_t internal_id) {
        if (ValidTypeId(type_id)) {
            return starting_node_ids[type_id][internal_id];
        }
        // Invalid Node
        return 0;
    }

    uint64_t RelationshipTypes::getEndingNodeId(uint16_t type_id, uint64_t internal_id) {
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
        uint16_t type_id = getTypeId(type);
        if (ValidTypeId(type_id)) {
            if (getCount(type_id) == 0) {
                type_to_id[type] = 0;
                id_to_type[type_id].clear();
                starting_node_ids[type_id].clear();
                ending_node_ids[type_id].clear();
                properties[type_id].clear();
                deleted_ids[type_id].clear();
                return true;
            }
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
        if (ValidTypeId(type_id)) {
            if (ValidRelationshipId(type_id, internal_id)) {
                return !deleted_ids[type_id].contains(internal_id);
            }
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

    std::vector<uint64_t>  RelationshipTypes::getIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
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

    std::vector<Relationship> RelationshipTypes::getRelationships(uint64_t skip, uint64_t limit) {
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

    std::vector<Relationship> RelationshipTypes::getRelationships(uint16_t type_id, uint64_t skip, uint64_t limit) {
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

    uint64_t RelationshipTypes::findCount(uint16_t type_id, const std::string &property, Operation operation, std::any value) {
        uint64_t count = 0;
        if (ValidTypeId(type_id)) {
            const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
            const uint64_t max_id = starting_node_ids[type_id].size();

            if(operation == Operation::IS_NULL) {
                uint64_t deleted_rels = getDeletedCount(type_id);
                uint64_t deleted_properties = properties[type_id].getDeletedCount(property);
                return (deleted_properties - deleted_rels);
            }

            if(operation == Operation::NOT_IS_NULL) {
                uint64_t deleted_rels = getDeletedCount(type_id);
                uint64_t deleted_properties = properties[type_id].getDeletedCount(property);
                return max_id - (deleted_properties - deleted_rels);
            }

            roaring::Roaring64Map blank;
            blank |= properties[type_id].getDeletedMap(property);
            blank |= getDeletedMap(type_id);

            switch (property_type_id) {
                case Properties::getBooleanPropertyType(): {
                    if (Properties::isBooleanProperty(value)) {
                        const bool typedValue = std::any_cast<bool>(value);
                        const std::vector<bool> &vec = properties[type_id].getBooleans(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<bool>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            count++;
                        }
                        return count;
                    }
                }
                case Properties::getIntegerPropertyType(): {
                    if (Properties::isIntegerProperty(value)) {
                        const int64_t typedValue = std::any_cast<int64_t>(value);
                        const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<int64_t>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }

                            count++;
                        }
                        return count;
                    }
                }
                case Properties::getDoublePropertyType(): {
                    if (Properties::isDoubleProperty(value)) {
                        const double typedValue = std::any_cast<double>(value);
                        const std::vector<double> &vec = properties[type_id].getDoubles(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<double>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }

                            count++;
                        }
                        return count;
                    }
                }
                case Properties::getStringPropertyType(): {
                    if (Properties::isStringProperty(value)) {
                        const std::string typedValue = std::any_cast<std::string>(value);
                        const std::vector<std::string> &vec = properties[type_id].getStrings(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateString(operation, vec[internal_id], typedValue)) {
                                continue;
                            }

                            count++;
                        }
                        return count;
                    }
                }
                case Properties::getBooleanListPropertyType(): {
                    if (Properties::isBooleanListProperty(value)) {
                        const std::vector<bool> typedValue = std::any_cast<std::vector<bool>>(value);
                        const std::vector<std::vector<bool>> &vec = properties[type_id].getBooleansList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<bool>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }

                            count++;
                        }
                        return count;
                    }
                }
                case Properties::getIntegerListPropertyType(): {
                    if (Properties::isIntegerListProperty(value)) {
                        const std::vector<int64_t> typedValue = std::any_cast<std::vector<int64_t>>(value);
                        const std::vector<std::vector<int64_t>> &vec = properties[type_id].getIntegersList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<int64_t>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }

                            count++;
                        }
                        return count;
                    }
                }
                case Properties::getDoubleListPropertyType(): {
                    if (Properties::isDoubleListProperty(value)) {
                        const std::vector<double> typedValue = std::any_cast<std::vector<double>>(value);
                        const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }

                            count++;
                        }
                        return count;
                    }
                }
                case Properties::getStringListPropertyType(): {
                    if (Properties::isStringListProperty(value)) {
                        const std::vector<std::string> typedValue = std::any_cast<std::vector<std::string>>(value);
                        const std::vector<std::vector<std::string>> &vec = properties[type_id].getStringsList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<std::string>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }

                            count++;
                        }
                        return count;
                    }
                }
                default: {
                    return count;
                }
            }
        }

        return count;
    }

    std::vector<uint64_t> RelationshipTypes::findIds(uint16_t type_id, const std::string &property, Operation operation, std::any value, uint64_t skip, uint64_t limit) {
        std::vector<uint64_t> ids;
        if (ValidTypeId(type_id)) {
            const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
            int current = 1;
            const uint64_t max_id = starting_node_ids[type_id].size();

            // Start blank, union with deleted properties, remove deleted nodes
            if(operation == Operation::IS_NULL) {
                roaring::Roaring64Map blank;
                blank |= properties[type_id].getDeletedMap(property);
                blank -= getDeletedMap(type_id);
                for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
                    if (current > (skip + limit)) {
                        break;
                    }
                    if (current++ > skip ) {
                        ids.emplace_back(internalToExternal(type_id, *iterator));
                    }
                }
                return ids;
            }

            // Start blank, fill from 0 to max_id, remove deleted nodes, remove deleted properties
            if(operation == Operation::NOT_IS_NULL) {
                roaring::Roaring64Map blank;
                blank.flip(0, max_id);
                blank -= getDeletedMap(type_id);
                blank -= properties[type_id].getDeletedMap(property);

                for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
                    if (current > (skip + limit)) {
                        break;
                    }
                    if (current++ > skip ) {
                        ids.emplace_back(internalToExternal(type_id, *iterator));
                    }
                }
                return ids;
            }

            roaring::Roaring64Map blank;
            blank |= properties[type_id].getDeletedMap(property);
            blank |= getDeletedMap(type_id);

            switch (property_type_id) {
                case Properties::getBooleanPropertyType(): {
                    if (Properties::isBooleanProperty(value)) {
                        const bool typedValue = std::any_cast<bool>(value);
                        const std::vector<bool> &vec = properties[type_id].getBooleans(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return ids;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<bool>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                ids.emplace_back(internalToExternal(type_id, internal_id));
                            }

                            current++;
                        }
                        return ids;
                    }
                }
                case Properties::getIntegerPropertyType(): {
                    if (Properties::isIntegerProperty(value)) {
                        const int64_t typedValue = std::any_cast<int64_t>(value);
                        const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return ids;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<int64_t>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                ids.emplace_back(internalToExternal(type_id, internal_id));
                            }

                            current++;
                        }
                        return ids;
                    }
                }
                case Properties::getDoublePropertyType(): {
                    if (Properties::isDoubleProperty(value)) {
                        const double typedValue = std::any_cast<double>(value);
                        const std::vector<double> &vec = properties[type_id].getDoubles(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return ids;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<double>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                ids.emplace_back(internalToExternal(type_id, internal_id));
                            }

                            current++;
                        }
                        return ids;
                    }
                }
                case Properties::getStringPropertyType(): {
                    if (Properties::isStringProperty(value)) {
                        const std::string typedValue = std::any_cast<std::string>(value);
                        const std::vector<std::string> &vec = properties[type_id].getStrings(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return ids;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateString(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                ids.emplace_back(internalToExternal(type_id, internal_id));
                            }

                            current++;
                        }
                        return ids;
                    }
                }
                case Properties::getBooleanListPropertyType(): {
                    if (Properties::isBooleanListProperty(value)) {
                        const std::vector<bool> typedValue = std::any_cast<std::vector<bool>>(value);
                        const std::vector<std::vector<bool>> &vec = properties[type_id].getBooleansList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return ids;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<bool>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                ids.emplace_back(internalToExternal(type_id, internal_id));
                            }

                            current++;
                        }
                        return ids;
                    }
                }
                case Properties::getIntegerListPropertyType(): {
                    if (Properties::isIntegerListProperty(value)) {
                        const std::vector<int64_t> typedValue = std::any_cast<std::vector<int64_t>>(value);
                        const std::vector<std::vector<int64_t>> &vec = properties[type_id].getIntegersList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return ids;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<int64_t>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                ids.emplace_back(internalToExternal(type_id, internal_id));
                            }

                            current++;
                        }
                        return ids;
                    }
                }
                case Properties::getDoubleListPropertyType(): {
                    if (Properties::isDoubleListProperty(value)) {
                        const std::vector<double> typedValue = std::any_cast<std::vector<double>>(value);
                        const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return ids;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                ids.emplace_back(internalToExternal(type_id, internal_id));
                            }

                            current++;
                        }
                        return ids;
                    }
                }
                case Properties::getStringListPropertyType(): {
                    if (Properties::isStringListProperty(value)) {
                        const std::vector<std::string> typedValue = std::any_cast<std::vector<std::string>>(value);
                        const std::vector<std::vector<std::string>> &vec = properties[type_id].getStringsList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return ids;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<std::string>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                ids.emplace_back(internalToExternal(type_id, internal_id));
                            }

                            current++;
                        }
                        return ids;
                    }
                }
                default: {
                    return ids;
                }
            }
        }
        return ids;
    }

    std::vector<Relationship> RelationshipTypes::findRelationships(uint16_t type_id, const std::string &property, Operation operation, std::any value, uint64_t skip, uint64_t limit) {
        std::vector<Relationship> relationships;
        if (ValidTypeId(type_id)) {
            const uint16_t property_type_id = properties[type_id].getPropertyTypeId(property);
            int current = 1;
            const uint64_t max_id = starting_node_ids[type_id].size();

            // Start blank, union with deleted properties, remove deleted nodes
            if(operation == Operation::IS_NULL) {
                roaring::Roaring64Map blank;
                blank |= properties[type_id].getDeletedMap(property);
                blank -= getDeletedMap(type_id);
                for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
                    if (current > (skip + limit)) {
                        break;
                    }
                    if (current++ > skip ) {
                        relationships.emplace_back(getRelationship(type_id, *iterator));
                    }
                }
                return relationships;
            }

            // Start blank, fill from 0 to max_id, remove deleted nodes, remove deleted properties
            if(operation == Operation::NOT_IS_NULL) {
                roaring::Roaring64Map blank;
                blank.flip(0, max_id);
                blank -= getDeletedMap(type_id);
                blank -= properties[type_id].getDeletedMap(property);

                for (roaring::Roaring64MapSetBitForwardIterator iterator = blank.begin(); iterator != blank.end(); ++iterator) {
                    if (current > (skip + limit)) {
                        break;
                    }
                    if (current++ > skip ) {
                        relationships.emplace_back(getRelationship(type_id, *iterator));
                    }
                }
                return relationships;
            }

            roaring::Roaring64Map blank;
            blank |= properties[type_id].getDeletedMap(property);
            blank |= getDeletedMap(type_id);

            switch (property_type_id) {
                case Properties::getBooleanPropertyType(): {
                    if (Properties::isBooleanProperty(value)) {
                        const bool typedValue = std::any_cast<bool>(value);
                        const std::vector<bool> &vec = properties[type_id].getBooleans(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return relationships;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<bool>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                relationships.emplace_back(getRelationship(type_id, internal_id));
                            }

                            current++;
                        }
                        return relationships;
                    }
                }
                case Properties::getIntegerPropertyType(): {
                    if (Properties::isIntegerProperty(value)) {
                        const int64_t typedValue = std::any_cast<int64_t>(value);
                        const std::vector<int64_t> &vec = properties[type_id].getIntegers(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return relationships;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<int64_t>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                relationships.emplace_back(getRelationship(type_id, internal_id));
                            }

                            current++;
                        }
                        return relationships;
                    }
                }
                case Properties::getDoublePropertyType(): {
                    if (Properties::isDoubleProperty(value)) {
                        const double typedValue = std::any_cast<double>(value);
                        const std::vector<double> &vec = properties[type_id].getDoubles(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return relationships;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::Evaluate<double>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                relationships.emplace_back(getRelationship(type_id, internal_id));
                            }

                            current++;
                        }
                        return relationships;
                    }
                }
                case Properties::getStringPropertyType(): {
                    if (Properties::isStringProperty(value)) {
                        const std::string typedValue = std::any_cast<std::string>(value);
                        const std::vector<std::string> &vec = properties[type_id].getStrings(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return relationships;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateString(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                relationships.emplace_back(getRelationship(type_id, internal_id));
                            }

                            current++;
                        }
                        return relationships;
                    }
                }
                case Properties::getBooleanListPropertyType(): {
                    if (Properties::isBooleanListProperty(value)) {
                        const std::vector<bool> typedValue = std::any_cast<std::vector<bool>>(value);
                        const std::vector<std::vector<bool>> &vec = properties[type_id].getBooleansList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return relationships;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<bool>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                relationships.emplace_back(getRelationship(type_id, internal_id));
                            }

                            current++;
                        }
                        return relationships;
                    }
                }
                case Properties::getIntegerListPropertyType(): {
                    if (Properties::isIntegerListProperty(value)) {
                        const std::vector<int64_t> typedValue = std::any_cast<std::vector<int64_t>>(value);
                        const std::vector<std::vector<int64_t>> &vec = properties[type_id].getIntegersList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return relationships;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<int64_t>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                relationships.emplace_back(getRelationship(type_id, internal_id));
                            }

                            current++;
                        }
                        return relationships;
                    }
                }
                case Properties::getDoubleListPropertyType(): {
                    if (Properties::isDoubleListProperty(value)) {
                        const std::vector<double> typedValue = std::any_cast<std::vector<double>>(value);
                        const std::vector<std::vector<double>> &vec = properties[type_id].getDoublesList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return relationships;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<double>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                relationships.emplace_back(getRelationship(type_id, internal_id));
                            }

                            current++;
                        }
                        return relationships;
                    }
                }
                case Properties::getStringListPropertyType(): {
                    if (Properties::isStringListProperty(value)) {
                        const std::vector<std::string> typedValue = std::any_cast<std::vector<std::string>>(value);
                        const std::vector<std::vector<std::string>> &vec = properties[type_id].getStringsList(property);
                        for(unsigned internal_id = 0; internal_id < vec.size(); ++internal_id) {
                            // If we reached out limit, return
                            if (current > (skip + limit)) {
                                return relationships;
                            }
                            // If the node or property has been deleted, ignore it
                            if (blank.contains(internal_id)) {
                                continue;
                            }
                            if (!Expression::EvaluateVector<std::string>(operation, vec[internal_id], typedValue)) {
                                continue;
                            }
                            // If it is true add it if we are over the skip, otherwise ignore it
                            if (current > skip) {
                                relationships.emplace_back(getRelationship(type_id, internal_id));
                            }

                            current++;
                        }
                        return relationships;
                    }
                }
                default: {
                    return relationships;
                }
            }
        }
        return relationships;
    }

    std::vector<uint64_t>  RelationshipTypes::getDeletedIds() const {
        std::vector<uint64_t>  allIds;
        // links are internal links, we need to switch to external links
        for (size_t type_id=1; type_id < id_to_type.size(); type_id++) {
            for (roaring::Roaring64MapSetBitForwardIterator iterator = deleted_ids[type_id].begin(); iterator != deleted_ids[type_id].end(); ++iterator) {
                allIds.emplace_back(internalToExternal(type_id, *iterator));
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

    bool RelationshipTypes::ValidRelationshipId(uint16_t type_id, uint64_t internal_id) {
        // If the type is valid, is the internal id within the vector size and is it not deleted?
        if (ValidTypeId(type_id)) {
            return starting_node_ids[type_id].size() > internal_id && !deleted_ids[type_id].contains(internal_id);
        }
        return false;
    }

    uint64_t RelationshipTypes::getCount(uint16_t type_id) {
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
        return id_to_type.size() - 1;
    }

    std::set<std::string> RelationshipTypes::getTypes() {
        // Skip the empty type
        return {id_to_type.begin() + 1, id_to_type.end()};
    }

    std::set<uint16_t> RelationshipTypes::getTypeIds() {
        std::set<uint16_t> type_ids;
        for (size_t i=1; i < type_to_id.size(); i++) {
            type_ids.insert(i);
        }

        return type_ids;
    }

    bool RelationshipTypes::deleteTypeProperty(uint16_t type_id, const std::string &property) {
        if (ValidTypeId(type_id) ) {
            return properties[type_id].removePropertyType(property);
        }

        return false;
    }

    std::map<uint16_t, uint64_t> RelationshipTypes::getCounts() {
        std::map<uint16_t,uint64_t> counts;
        for (size_t type_id=1; type_id < type_to_id.size(); type_id++) {
            counts.insert({type_id, starting_node_ids[type_id].size() - deleted_ids[type_id].cardinality()});
        }

        return counts;
    }

    std::map<std::string, std::any> RelationshipTypes::getRelationshipProperties(uint16_t type_id, uint64_t internal_id) {
        if(ValidTypeId(type_id)) {
            return properties[type_id].getProperties(internal_id);
        }
        return std::map<std::string, std::any>();
    }

    Relationship RelationshipTypes::getRelationship(uint64_t external_id) {
        return getRelationship(externalToTypeId(external_id), externalToInternal(external_id), external_id);
    }

    Relationship RelationshipTypes::getRelationship(uint16_t type_id, uint64_t internal_id) {
        return Relationship(internalToExternal(type_id, internal_id), getType(type_id), getStartingNodeId(type_id,internal_id), getEndingNodeId(type_id,internal_id), getRelationshipProperties(type_id, internal_id));
    }

    Relationship RelationshipTypes::getRelationship(uint16_t type_id, uint64_t internal_id, uint64_t external_id) {
        return Relationship(external_id, getType(type_id), getStartingNodeId(type_id,internal_id), getEndingNodeId(type_id,internal_id), getRelationshipProperties(type_id, internal_id));
    }

    std::any RelationshipTypes::getRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property) {
        if(ValidTypeId(type_id)) {
            if (ValidRelationshipId(type_id, internal_id)) {
                return properties[type_id].getProperty(property, internal_id);
            }
        }
        return tombstone_any;
    }

    std::any RelationshipTypes::getRelationshipProperty(uint64_t external_id, const std::string &property) {
        return getRelationshipProperty(externalToTypeId(external_id), externalToInternal(external_id), property);
    }

    std::vector<uint64_t> &RelationshipTypes::getStartingNodeIds(uint16_t type_id) {
        return starting_node_ids[type_id];
    }

    std::vector<uint64_t> &RelationshipTypes::getEndingNodeIds(uint16_t type_id) {
        return ending_node_ids[type_id];
    }

    bool RelationshipTypes::setRelationshipPropertyFromJson(uint16_t type_id, uint64_t internal_id, const std::string &property,
                                                            const std::string &json) {
        if (!json.empty()) {
            // Get the properties
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

    bool RelationshipTypes::setRelationshipPropertyFromJson(uint64_t external_id, const std::string &property, const std::string &value) {
        return setRelationshipPropertyFromJson(externalToTypeId(external_id), externalToInternal(external_id), property, value);
    }

    bool RelationshipTypes::deleteRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property) {
        return properties[type_id].deleteProperty(property, internal_id);
    }
    bool RelationshipTypes::deleteRelationshipProperty(uint64_t external_id, const std::string &property) {
        return deleteRelationshipProperty(externalToTypeId(external_id), externalToInternal(external_id), property);
    }

    Properties &RelationshipTypes::getProperties(uint16_t type_id) {
        return properties[type_id];
    }

    bool RelationshipTypes::setRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property, const std::any& value) {
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

    bool RelationshipTypes::setRelationshipProperty(uint64_t external_id, const std::string &property, const std::any& value) {
        return setRelationshipProperty(externalToTypeId(external_id), externalToInternal(external_id), property, value);
    }

    bool RelationshipTypes::setPropertiesFromJSON(uint16_t type_id, uint64_t internal_id, const std::string& json) {
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

    bool RelationshipTypes::deleteProperties(uint16_t type_id, uint64_t internal_id) {
        return properties[type_id].deleteProperties(internal_id);
    }
}
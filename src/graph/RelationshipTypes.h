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

#ifndef RAGEDB_RELATIONSHIPTYPES_H
#define RAGEDB_RELATIONSHIPTYPES_H

#include <cstdint>
#include <roaring/roaring64map.hh>
#include <set>
#include "Relationship.h"
#include "Properties.h"
#include "Operation.h"
#include <simdjson.h>

namespace ragedb {

    class RelationshipTypes {
    private:
        std::unordered_map<std::string, uint16_t> type_to_id;
        std::vector<std::string> id_to_type;
        std::vector<std::vector<uint64_t>> starting_node_ids;
        std::vector<std::vector<uint64_t>> ending_node_ids;
        std::vector<Properties> properties;                             // Store of the properties of Relationships
        std::vector<roaring::Roaring64Map> deleted_ids;

        simdjson::dom::parser parser;
        uint shard_id;

    public:
        RelationshipTypes();
        void Clear();

        bool addTypeId(const std::string &type, uint16_t relationship_type_id);
        uint16_t getTypeId(const std::string &type);
        uint16_t insertOrGetTypeId(const std::string &type);
        std::string getType(const std::string &type);
        std::string getType(uint16_t relationship_type_id);
        bool deleteTypeId(const std::string &type);

        bool addId(uint16_t, uint64_t);
        bool removeId(uint16_t type_id, uint64_t internal_id);
        bool containsId(uint16_t, uint64_t);

        std::vector<uint64_t> getIds(uint64_t skip, uint64_t limit) const;
        std::vector<uint64_t> getIds(uint16_t type_id, uint64_t skip, uint64_t limit);
        std::vector<Relationship> getRelationships(uint64_t skip, uint64_t limit);
        std::vector<Relationship> getRelationships(uint16_t type_id, uint64_t skip, uint64_t limit);

        uint64_t findCount(uint16_t type_id, const std::string &property, Operation operation, property_type_t value);
        std::vector<uint64_t> findIds(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit);
        std::vector<Relationship> findRelationships(uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit);

        uint64_t filterCount(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, property_type_t value);
        std::vector<uint64_t> filterIds(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit);
        std::vector<Relationship> filterRelationships(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, property_type_t value, uint64_t skip, uint64_t limit);

        std::vector<uint64_t> getDeletedIds() const;
        bool hasDeleted(uint16_t type_id);
        uint64_t getDeletedIdsMinimum(uint16_t type_id);

        bool ValidTypeId(uint16_t type_id) const;
        bool ValidRelationshipId(uint16_t type_id, uint64_t internal_id);

        std::map<uint16_t, uint64_t> getCounts();
        uint64_t getCount(uint16_t type_id);
        uint64_t getDeletedCount(uint16_t node_type_id);
        roaring::Roaring64Map& getDeletedMap(uint16_t type_id);
        uint16_t getSize() const;

        std::set<std::string> getTypes();
        std::set<uint16_t> getTypeIds();
        bool deleteTypeProperty(uint16_t type_id, const std::string &property);
        uint64_t getStartingNodeId(uint16_t type_id, uint64_t internal_id);
        uint64_t getEndingNodeId(uint16_t type_id, uint64_t internal_id);
        bool setStartingNodeId(uint16_t type_id, uint64_t internal_id, uint64_t external_id);
        bool setEndingNodeId(uint16_t type_id, uint64_t internal_id, uint64_t external_id);

        std::map<std::string, property_type_t> getRelationshipProperties(uint16_t type_id, uint64_t internal_id);
        Relationship getRelationship(uint64_t external_id);
        Relationship getRelationship(uint16_t type_id, uint64_t internal_id);
        Relationship getRelationship(uint16_t type_id, uint64_t internal_id, uint64_t external_id);
        property_type_t getRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property);
        property_type_t getRelationshipProperty(uint64_t external_id, const std::string &property);
        std::vector<uint64_t> &getStartingNodeIds(uint16_t type_id);
        std::vector<uint64_t> &getEndingNodeIds(uint16_t type_id);
        bool setRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property, const property_type_t& value);
        bool setRelationshipProperty(uint64_t external_id, const std::string &property, const property_type_t& value);
        bool setRelationshipPropertyFromJson(uint16_t type_id, uint64_t internal_id, const std::string &property, const std::string& json);
        bool setRelationshipPropertyFromJson(uint64_t external_id, const std::string &property, const std::string& value);
        bool deleteRelationshipProperty(uint16_t type_id, uint64_t internal_id, const std::string &property);
        bool deleteRelationshipProperty(uint64_t external_id, const std::string &property);

        Properties &getProperties(uint16_t type_id);
        bool setPropertiesFromJSON(uint16_t type_id, uint64_t internal_id, const std::string &json);
        bool deleteProperties(uint16_t type_id, uint64_t internal_id);

    };
}


#endif //RAGEDB_RELATIONSHIPTYPES_H

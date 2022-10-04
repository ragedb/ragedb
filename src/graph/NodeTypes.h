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

#ifndef RAGEDB_NODETYPES_H
#define RAGEDB_NODETYPES_H

#include <cstdint>
#include <set>
#include "Expression.h"
#include "Group.h"
#include "Node.h"
#include "Properties.h"
#include "Operation.h"
#include <simdjson.h>

namespace ragedb {

    class NodeTypes {
    private:
        std::unordered_map<std::string, uint16_t> type_to_id;
        std::vector<std::string> id_to_type;
        std::vector<tsl::sparse_map<std::string, uint64_t>> key_to_node_id;  // "Index" to get node id by type:key
        std::vector<std::vector<std::string>> keys;                          // Store of the keys of Nodes
        std::vector<Properties> properties;                             // Store of the properties of Nodes
        std::vector<std::vector<std::vector<Group>>> outgoing_relationships; // Outgoing relationships of each node
        std::vector<std::vector<std::vector<Group>>> incoming_relationships; // Incoming relationships of each node
        std::vector<roaring::Roaring64Map> deleted_ids; // all links are internal links

        simdjson::dom::parser parser;
        bool setProperty(uint16_t data_type_id, simdjson::dom::element value, uint16_t type_id, const std::string &property, uint64_t internal_id);

    public:
        NodeTypes();
        void Clear();

        bool addTypeId(const std::string &type, uint16_t type_id);
        uint16_t getTypeId(const std::string &type) const;
        uint16_t insertOrGetTypeId(const std::string &type);
        std::string getType(const std::string &type) const;
        std::string getType(uint16_t type_id) const;
        bool deleteTypeId(const std::string &type);

        bool addId(uint16_t type_id, uint64_t internal_id);
        bool removeId(uint16_t type_id, uint64_t internal_id);
        bool containsId(uint16_t type_id, uint64_t internal_id);

        std::vector<uint64_t> getIds(uint64_t skip, uint64_t limit) const;
        std::vector<uint64_t> getIds(uint16_t type_id, uint64_t skip, uint64_t limit) const;
        std::vector<Node> getNodes(uint64_t skip, uint64_t limit);
        std::vector<Node> getNodes(uint16_t type_id, uint64_t skip, uint64_t limit);


        // Find Helpers
        roaring::Roaring64Map getBlanks(uint16_t type_id, const std::string &property);
        uint64_t countBooleans(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t countIntegers(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t countDoubles(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t countStrings(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t countBooleanLists(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t countIntegerLists(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t countDoubleLists(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t countStringLists(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);

        std::vector<uint64_t> findNullIds(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findNotNullIds(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findBooleanIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findIntegerIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findDoubleIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findStringIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findBooleanListIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findIntegerListIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findDoubleListIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> findStringListIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);

        std::vector<Node> findNullNodes(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit);
        std::vector<Node> findNotNullNodes(uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit);
        std::vector<Node> findBooleanNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> findIntegerNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> findDoubleNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> findStringNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> findBooleanListNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> findIntegerListNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> findDoubleListNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> findStringListNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);

        uint64_t findCount(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        std::vector<uint64_t> findIds(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> findNodes(uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);

        // Filter Helpers
        void removeDeletedIds(uint16_t type_id, std::vector<uint64_t> &list);
        void removeDeletedProperties(uint16_t type_id, std::vector<uint64_t> &list, const std::string &property);
        uint64_t filterCountBooleans(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t filterCountIntegers(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t filterCountDoubles(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t filterCountStrings(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t filterCountBooleanLists(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t filterCountIntegerLists(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t filterCountDoubleLists(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        uint64_t filterCountStringLists(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);

        std::vector<uint64_t> filterNullIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterNotNullIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterBooleanIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterIntegerIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterDoubleIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterStringIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterBooleanListIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterIntegerListIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterDoubleListIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> filterStringListIds(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);

        std::vector<Node> filterNullNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit);
        std::vector<Node> filterNotNullNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, uint64_t skip, uint64_t limit);
        std::vector<Node> filterBooleanNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> filterIntegerNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> filterDoubleNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> filterStringNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> filterBooleanListNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> filterIntegerListNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> filterDoubleListNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> filterStringListNodes(const std::vector<uint64_t>& list, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);

        uint64_t filterCount(const std::vector<uint64_t>& unfiltered, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value);
        std::vector<uint64_t> filterIds(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);
        std::vector<Node> filterNodes(std::vector<uint64_t> unfiltered, uint16_t type_id, const std::string &property, Operation operation, const property_type_t& value, uint64_t skip, uint64_t limit);

        std::vector<uint64_t> getDeletedIds() const;
        bool hasDeleted(uint16_t type_id);
        uint64_t getDeletedIdsMinimum(uint16_t type_id);

        bool ValidTypeId(uint16_t type_id) const;
        bool InvalidTypeId(uint16_t type_id) const;
        bool ValidNodeId(uint16_t type_id, uint64_t internal_id) const;
        bool InvalidNodeId(uint16_t type_id, uint64_t internal_id) const;

        std::map<uint16_t, uint64_t> getCounts();
        uint64_t getCount(uint16_t type_id) const;
        uint64_t getDeletedCount(uint16_t type_id);
        roaring::Roaring64Map& getDeletedMap(uint16_t type_id);
        uint16_t getSize() const;

        std::set<std::string> getTypes();
        std::set<uint16_t> getTypeIds() const;
        bool deleteTypeProperty(uint16_t type_id, const std::string &property);

        uint64_t getNodeId(uint16_t type_id, const std::string &key);
        uint64_t getNodeId(const std::string &type, const std::string &key);
        std::string getNodeKey(uint16_t type_id, uint64_t internal_id) const;
        std::map<std::string, property_type_t> getNodeProperties(uint16_t type_id, uint64_t internal_id) const;
        std::map<uint64_t, std::map<std::string, property_type_t>> getNodesProperties(uint16_t type_id, const std::vector<uint64_t> &external_ids) const;
        std::map<uint64_t, property_type_t> getNodesProperty(uint16_t type_id, const std::vector<uint64_t> &external_ids, const std::string& property) const;
        Node getNode(uint64_t external_id);
        Node getNode(uint16_t type_id, uint64_t internal_id);
        Node getNode(uint16_t type_id, uint64_t internal_id, uint64_t external_id);
        property_type_t getNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property);
        property_type_t getNodeProperty(uint64_t external_id, const std::string &property);
        bool setNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property, const property_type_t& value);
        bool setNodeProperty(uint64_t external_id, const std::string &property, const property_type_t& value);
        bool setNodePropertyFromJson(uint16_t type_id, uint64_t internal_id, const std::string &property, const std::string& value);
        bool setNodePropertyFromJson(uint64_t external_id, const std::string &property, const std::string& value);
        bool deleteNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property);
        bool deleteNodeProperty(uint64_t external_id, const std::string &property);

        Properties &getProperties(uint16_t type_id);
        bool setPropertiesFromJSON(uint16_t type_id, uint64_t internal_id, const std::string &json);
        bool deleteProperties(uint16_t type_id, uint64_t internal_id);

        tsl::sparse_map<std::string, uint64_t> &getKeysToNodeId(uint16_t type_id);
        std::vector<std::string> &getKeys(uint16_t type_id);
        std::vector<std::vector<Group>> &getOutgoingRelationships(uint16_t type_id);
        std::vector<std::vector<Group>> &getIncomingRelationships(uint16_t type_id);

    };
}


#endif //RAGEDB_NODETYPES_H

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
#include <roaring/roaring64map.hh>
#include <set>
#include "Group.h"
#include "Node.h"
#include "Properties.h"

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
        std::vector<Roaring64Map> deleted_ids; // all ids are internal ids

        simdjson::dom::parser parser;
        uint shard_id;

        uint64_t internalToExternal(uint16_t type_id, uint64_t internal_id) const;
        static uint64_t externalToInternal(uint64_t id);
        static uint16_t externalToTypeId(uint64_t id);

    public:
        NodeTypes();
        void Clear();

        bool addTypeId(const std::string &type, uint16_t type_id);
        uint16_t getTypeId(const std::string &type);
        uint16_t insertOrGetTypeId(const std::string &type);
        std::string getType(const std::string &type);
        std::string getType(uint16_t type_id);
        bool deleteTypeId(const std::string &type);

        bool addId(uint16_t type_id, uint64_t internal_id);
        bool removeId(uint16_t type_id, uint64_t internal_id);
        bool containsId(uint16_t type_id, uint64_t internal_id);

        std::vector<uint64_t> getIds(uint64_t skip, uint64_t limit) const;
        std::vector<uint64_t> getIds(uint16_t type_id, uint64_t skip, uint64_t limit);
        std::vector<uint64_t> getDeletedIds() const;
        bool hasDeleted(uint16_t type_id);
        uint64_t getDeletedIdsMinimum(uint16_t type_id);

        bool ValidTypeId(uint16_t type_id) const;
        bool ValidNodeId(uint16_t type_id, uint64_t internal_id);

        std::map<uint16_t, uint64_t> getCounts();
        uint64_t getCount(uint16_t type_id);
        uint64_t getDeletedCount(uint16_t type_id);
        uint16_t getSize() const;

        std::set<std::string> getTypes();
        std::set<uint16_t> getTypeIds();
        bool deleteTypeProperty(uint16_t type_id, const std::string &property);

        uint64_t getNodeId(uint16_t type_id, const std::string &key);
        uint64_t getNodeId(const std::string &type, const std::string &key);
        std::string getNodeKey(uint16_t type_id, uint64_t internal_id);
        std::map<std::string, std::any> getNodeProperties(uint16_t type_id, uint64_t internal_id);
        Node getNode(uint64_t external_id);
        Node getNode(uint16_t type_id, uint64_t internal_id, uint64_t external_id);
        std::any getNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property);
        std::any getNodeProperty(uint64_t external_id, const std::string &property);
        bool setNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property, const std::string &value);
        bool setNodeProperty(uint64_t external_id, const std::string &property, const std::string &value);
        bool deleteNodeProperty(uint16_t type_id, uint64_t internal_id, const std::string &property);
        bool deleteNodeProperty(uint64_t external_id, const std::string &property);


        Properties &getNodeTypeProperties(uint16_t type_id);
        bool setNodePropertiesFromJSON(uint16_t type_id, uint64_t internal_id, const std::string &json);
        bool deleteNodeProperties(uint16_t type_id, uint64_t internal_id);

        tsl::sparse_map<std::string, uint64_t> &getKeysToNodeId(uint16_t type_id);
        std::vector<std::string> &getKeys(uint16_t type_id);
        std::vector<std::vector<Group>> &getOutgoingRelationships(uint16_t type_id);
        std::vector<std::vector<Group>> &getIncomingRelationships(uint16_t type_id);

    };
}


#endif //RAGEDB_NODETYPES_H

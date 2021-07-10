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

#ifndef RAGEDB_SHARD_H
#define RAGEDB_SHARD_H

#include <seastar/core/sharded.hh>
#include <seastar/core/rwlock.hh>
#include <tsl/sparse_map.h>
#include "Node.h"
#include "Relationship.h"
#include "NodeTypes.h"
#include "RelationshipTypes.h"

namespace ragedb {

    class Shard : public seastar::peering_sharded_service<Shard> {

    private:
        uint cpus;
        uint shard_id;

        seastar::rwlock rel_type_lock;
        seastar::rwlock node_type_lock;

        std::map<std::string, tsl::sparse_map<std::string, uint64_t>> node_keys;    // "Index" to get node id by type:key
        NodeTypes node_types;                                                       // Store string and id of node types
        RelationshipTypes relationship_types;                                       // Store string and id of relationship types

    public:
        explicit Shard(uint _cpus) : cpus(_cpus), shard_id(seastar::this_shard_id()) {
            std::stringstream ss;
            ss << "Starting Shard " << shard_id << '\n';
            std::cout << ss.str();
        }

        static seastar::future<> stop();
        void Clear();

        // Ids
        uint64_t internalToExternal(uint16_t type_id, uint64_t internal_id) const;
        static uint64_t externalToInternal(uint64_t id);
        static uint16_t externalToTypeId(uint64_t id);
        static uint16_t CalculateShardId(uint64_t id);
        uint16_t CalculateShardId(const std::string &type, const std::string &key) const;

        // *****************************************************************************************************************************
        //                                               Single Shard
        // *****************************************************************************************************************************

        static seastar::future<std::string> HealthCheck();

        // Node Type
        std::string NodeTypeGetType(uint16_t type_id);
        uint16_t NodeTypeGetTypeId(const std::string& type);
        bool NodeTypeInsert(const std::string& type, uint16_t type_id);

        // Nodes
        uint64_t NodeAddEmpty(const std::string& type, uint16_t type_id, const std::string& key);

        // *****************************************************************************************************************************
        //                                               Peered
        // *****************************************************************************************************************************

        seastar::future<std::vector<std::string>> HealthCheckPeered();

        // Node Type
        std::string NodeTypeGetTypePeered(uint16_t type_id);
        uint16_t NodeTypeGetTypeIdPeered(const std::string& type);
        seastar::future<uint16_t> NodeTypeInsertPeered(const std::string& type);

        // Nodes
        seastar::future<uint64_t> NodeAddEmptyPeered(const std::string& type, const std::string& key);
    };
}


#endif //RAGEDB_SHARD_H

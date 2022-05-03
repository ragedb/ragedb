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

#define SOL_ALL_SAFETIES_ON 1

#include <coroutine>
#include <iostream>
#include <fstream>
#include <seastar/core/sharded.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_intent.hh>
#include <simdjson.h>
#include <sol/sol.hpp>
#include <tsl/sparse_map.h>
#include "Direction.h"
#include "Link.h"
#include "Node.h"
#include "Operation.h"
#include "Relationship.h"
#include "NodeTypes.h"
#include "RelationshipTypes.h"
#include "eve/CollectIndexes.h"
#include <cppcodec/base64_default_url_unpadded.hpp>
#include <cpr/cpr.h>

extern unsigned int SHARD_BITS;
extern unsigned int SHARD_MASK;

namespace ragedb {

  class Shard : public seastar::peering_sharded_service<Shard> {

    private:
        uint cpus;
        uint shard_id;

        seastar::rwlock rel_type_lock;                  // Global lock to keep Relationship Type ids in sync
        seastar::rwlock node_type_lock;                 // Global lock to keep Node Type ids in sync
        seastar::rwlock lua_lock;                       // Per Shard lock to run a single Lua script each

        sol::state lua;                                 // Lua State
        sol::environment env;                           // Lua Sandboxed Environment
        sol::environment read_write_env;                // Lua Read Write Sandboxed Environment
        sol::environment read_only_env;                 // Lua Read Only Sandboxed Environment

        NodeTypes node_types;                           // Store string and id of node types
        RelationshipTypes relationship_types;           // Store string and id of relationship types


        inline static const std::string EXCEPTION = "An exception has occurred: ";

    public:
        explicit Shard(uint _cpus);

        inline static const uint64_t SKIP = 0;
        inline static const uint64_t LIMIT = 100;
        inline static const std::vector<std::string> READ_PATHS = {};   // No read path is valid for now
        inline static const std::vector<std::string> WRITE_PATHS = {};  // No write path is valid for now

        static seastar::future<> stop();
        void Clear();

        seastar::future<std::string> RunLua(const std::string &script, sol::environment& environment);
        seastar::future<std::string> RunAdminLua(const std::string &script);
        seastar::future<std::string> RunRWLua(const std::string &script);
        seastar::future<std::string> RunROLua(const std::string &script);

        // Sandboxed
        std::tuple<sol::object, sol::object> loadstring(const std::string &str, const sol::optional<std::string> &chunkname = sol::detail::default_chunk_name());
        std::tuple<sol::object, sol::object> loadfile(const std::string &path);
        sol::object dofile(const std::string &path);
        bool checkPath(const std::string &filepath, bool write);

          // Ids
        static uint64_t internalToExternal(uint16_t type_id, uint64_t internal_id);
        static uint64_t externalToInternal(uint64_t id);
        static uint16_t externalToTypeId(uint64_t id);
        static uint16_t CalculateShardId(uint64_t id);
        uint16_t CalculateShardId(const std::string &type, const std::string &key) const;
        uint16_t CalculateShardId(const std::string &type, const std::string &key, const property_type_t &value) const;
        bool ValidNodeId(uint64_t id);
        bool ValidNodeId(uint64_t id, uint16_t type_id, uint64_t internal_id);
        bool ValidRelationshipId(uint64_t id);
        bool ValidRelationshipId(uint64_t id, uint16_t type_id, uint64_t internal_id);

        // *****************************************************************************************************************************
        //                                               Single Shard
        // *****************************************************************************************************************************

        static seastar::future<std::string> HealthCheck();

        // Node Types
        uint16_t NodeTypesGetCount();
        uint64_t NodeTypesGetCount(uint16_t type_id);
        uint64_t NodeTypesGetCount(const std::string& type);
        std::set<std::string> NodeTypesGet();
        std::map<std::string, std::string, std::less<>> NodeTypeGet(const std::string& type);

        // Relationship Types
        uint16_t RelationshipTypesGetCount();
        uint64_t RelationshipTypesGetCount(uint16_t type_id);
        uint64_t RelationshipTypesGetCount(const std::string& type);
        std::set<std::string> RelationshipTypesGet();
        std::map<std::string, std::string, std::less<>> RelationshipTypeGet(const std::string& type);

        // Node Type
        std::string NodeTypeGetType(uint16_t type_id);
        uint16_t NodeTypeGetTypeId(const std::string& type);
        bool NodeTypeInsert(const std::string& type, uint16_t type_id);
        bool DeleteNodeType(const std::string& type);

        // Relationship Type
        std::string RelationshipTypeGetType(uint16_t type_id);
        uint16_t RelationshipTypeGetTypeId(const std::string& type);
        bool RelationshipTypeInsert(const std::string& type, uint16_t type_id);
        bool DeleteRelationshipType(const std::string& type);

        // Property Types
        uint8_t NodePropertyTypeAdd(uint16_t type_id, const std::string& key, uint8_t property_type_id);
        uint8_t RelationshipPropertyTypeAdd(uint16_t type_id, const std::string& key, uint8_t property_type_id);
        std::string NodePropertyTypeGet(const std::string& type, const std::string& key);
        std::string RelationshipPropertyTypeGet(const std::string& type,  const std::string& key);
        bool NodePropertyTypeDelete(uint16_t type_id, const std::string& key);
        bool RelationshipPropertyTypeDelete(uint16_t type_id, const std::string& key);

        // Helpers
        std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> NodeRemoveGetIncoming(uint64_t external_id);
        bool NodeRemoveDeleteIncoming(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>>&grouped_relationships);
        std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> NodeRemoveGetOutgoing(uint64_t external_id);
        bool NodeRemoveDeleteOutgoing(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>>&grouped_relationships);
        std::pair <uint16_t ,uint64_t> RelationshipRemoveGetIncoming(uint64_t internal_id);
        bool RelationshipRemoveIncoming(uint16_t rel_type_id, uint64_t external_id, uint64_t node_id);

        // Node
        uint64_t NodeAddEmpty(uint16_t type_id, const std::string& key);
        uint64_t NodeAdd(uint16_t type_id, const std::string& key, const std::string& properties);
        std::vector<uint64_t> NodeAddMany(uint16_t type_id, const std::vector<std::tuple<std::string, std::string>> nodes);
        uint64_t NodeGetID(const std::string& type, const std::string& key);
        Node NodeGet(uint64_t id);
        Node NodeGet(const std::string& type, const std::string& key);
        static uint16_t NodeGetTypeId(uint64_t id);
        std::string NodeGetType(uint64_t id);
        std::string NodeGetKey(uint64_t id);
        bool NodeRemove(uint64_t id);
        bool NodeRemove(const std::string& type, const std::string& key);

        // Nodes
        std::vector<Node> NodesGet(const std::vector<uint64_t>&);
        std::vector<Node> NodesGet(const std::vector<Link>& links);
        std::map<uint64_t, std::string> NodesGetKey(const std::vector<uint64_t>&);
        std::map<Link, std::string> NodesGetKey(const std::vector<Link>& links);
        std::map<uint64_t, std::string> NodesGetType(const std::vector<uint64_t>&);
        std::map<Link, std::string> NodesGetType(const std::vector<Link>& links);
        std::map<uint64_t, uint16_t> NodesGetTypeId(const std::vector<uint64_t>&);
        std::map<Link, uint16_t> NodesGetTypeId(const std::vector<Link>& links);

        std::map<uint64_t, property_type_t> NodesGetProperty(const std::vector<uint64_t> &ids, const std::string& property);
        std::map<Link, property_type_t> NodesGetProperty(const std::vector<Link>& links, const std::string& property);
        std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>> NodesGetProperties(const std::vector<uint64_t>&);
        std::map<Link, std::map<std::string, property_type_t, std::less<>>> NodesGetProperties(const std::vector<Link>& links);

        // Node Property
        property_type_t NodeGetProperty(uint64_t id, const std::string& property);
        bool NodeSetProperty(uint64_t id, const std::string& property, property_type_t value);
        bool NodeSetPropertyFromJson(uint64_t id, const std::string& property, const std::string& value);
        property_type_t NodeGetProperty(const std::string& type, const std::string& key, const std::string& property);
        bool NodeGetProperty(const std::string& type, const std::string& key, const std::string& property, property_type_t value);
        bool NodeSetPropertyFromJson(const std::string& type, const std::string& key, const std::string& property,const std::string& value);
        bool NodeDeleteProperty(const std::string& type, const std::string& key, const std::string& property);
        bool NodeDeleteProperty(uint64_t id, const std::string& property);

        // Node Properties
        std::map<std::string, property_type_t, std::less<>> NodeGetProperties(const std::string& type, const std::string& key);
        std::map<std::string, property_type_t, std::less<>> NodeGetProperties(uint64_t id);
        bool NodeSetPropertiesFromJson(const std::string& type, const std::string& key, const std::string& value);
        bool NodeSetPropertiesFromJson(uint64_t id, const std::string& value);
        bool NodeResetPropertiesFromJson(const std::string& type, const std::string& key, const std::string& value);
        bool NodeResetPropertiesFromJson(uint64_t id, const std::string& value);
        bool NodeDeleteProperties(const std::string& type, const std::string& key);
        bool NodeDeleteProperties(uint64_t id);

        // Relationship
        uint64_t RelationshipAddEmptySameShard(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
        uint64_t RelationshipAddEmptySameShard(uint16_t rel_type, const std::string& type1, const std::string& key1,
                                               const std::string& type2, const std::string& key2);
        uint64_t RelationshipAddEmptyToOutgoing(uint16_t rel_type, uint64_t id1, uint64_t id2);
        uint64_t RelationshipAddToIncoming(uint16_t rel_type, uint64_t rel_id, uint64_t id1, uint64_t id2);

        uint64_t RelationshipAddSameShard(uint16_t rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        uint64_t RelationshipAddSameShard(uint16_t rel_type, const std::string& type1, const std::string& key1,
                                          const std::string& type2, const std::string& key2, const std::string& properties);
        uint64_t RelationshipAddToOutgoing(uint16_t rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        Relationship RelationshipGet(uint64_t rel_id);
        std::string RelationshipGetType(uint64_t id);
        uint16_t RelationshipGetTypeId(uint64_t id);
        uint64_t RelationshipGetStartingNodeId(uint64_t id);
        uint64_t RelationshipGetEndingNodeId(uint64_t id);

        // Relationships
        std::vector<Relationship> RelationshipsGet(const std::vector<uint64_t>& ids);
        std::vector<Relationship> RelationshipsGet(const std::vector<Link>& links);
        std::map<uint64_t, std::string> RelationshipsGetType(const std::vector<uint64_t>& ids);
        std::map<Link, std::string> RelationshipsGetType(const std::vector<Link>& links);
        std::map<uint64_t, uint16_t> RelationshipsGetTypeId(const std::vector<uint64_t>& ids);
        std::map<Link, uint16_t> RelationshipsGetTypeId(const std::vector<Link>& links);

        std::map<uint64_t, property_type_t> RelationshipsGetProperty(const std::vector<uint64_t> &ids, const std::string& property);
        std::map<Link, property_type_t> RelationshipsGetProperty(const std::vector<Link>& links, const std::string& property);
        std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>> RelationshipsGetProperties(const std::vector<uint64_t>& ids);
        std::map<Link, std::map<std::string, property_type_t, std::less<>>> RelationshipsGetProperties(const std::vector<Link>& links);

        // Relationship Property
        property_type_t RelationshipGetProperty(uint64_t id, const std::string& property);
        bool RelationshipSetProperty(uint64_t id, const std::string& property, const property_type_t& value);
        bool RelationshipSetPropertyFromJson(uint64_t id, const std::string& property, const std::string& value);
        bool RelationshipDeleteProperty(uint64_t id, const std::string& property);

        // Relationship Properties
        std::map<std::string, property_type_t, std::less<>> RelationshipGetProperties(uint64_t id);
        bool RelationshipSetPropertiesFromJson(uint64_t id, const std::string& value);
        bool RelationshipResetPropertiesFromJson(uint64_t id, const std::string& value);
        bool RelationshipDeleteProperties(uint64_t id);

        // Node Degree
        uint64_t NodeGetDegree(const std::string& type, const std::string& key);
        uint64_t NodeGetDegree(const std::string& type, const std::string& key, Direction direction);
        uint64_t NodeGetDegree(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        uint64_t NodeGetDegree(const std::string& type, const std::string& key, Direction direction,
                               const std::vector<std::string>& rel_types);
        uint64_t NodeGetDegree(uint64_t id);
        uint64_t NodeGetDegree(uint64_t id, Direction direction);
        uint64_t NodeGetDegree(uint64_t id, Direction direction, const std::string& rel_type);
        uint64_t NodeGetDegree(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        // Traversing
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key);
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, Direction direction);
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);
        std::vector<Link> NodeGetLinks(uint64_t id);
        std::vector<Link> NodeGetLinks(uint64_t id, Direction direction);
        std::vector<Link> NodeGetLinks(uint64_t id, Direction direction, const std::string& rel_type);
        std::vector<Link> NodeGetLinks(uint64_t id, Direction direction, uint16_t type_id);
        std::vector<Link> NodeGetLinks(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        // Relationships Between Two Given Nodes
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, uint64_t id2);
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, uint64_t id2, Direction direction);
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, uint64_t id2, Direction direction, const std::string& rel_type);
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, uint64_t id2, Direction direction, uint16_t type_id);
        std::vector<Link> NodeGetLinks(const std::string& type, const std::string& key, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types);
        std::vector<Link> NodeGetLinks(uint64_t id, uint64_t id2);
        std::vector<Link> NodeGetLinks(uint64_t id, uint64_t id2, Direction direction);
        std::vector<Link> NodeGetLinks(uint64_t id, uint64_t id2, Direction direction, const std::string& rel_type);
        std::vector<Link> NodeGetLinks(uint64_t id, uint64_t id2, Direction direction, uint16_t type_id);
        std::vector<Link> NodeGetLinks(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(uint64_t id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(uint64_t id, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(uint64_t id, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(uint64_t id, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(const std::string& type, const std::string& key);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(const std::string& type, const std::string& key, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(uint64_t id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(uint64_t id, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(uint64_t id, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(uint64_t id, const std::vector<std::string> &rel_types);

        std::vector<Relationship> NodeGetOutgoingRelationships(const std::string& type, const std::string& key);
        std::vector<Relationship> NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::string& rel_type);
        std::vector<Relationship> NodeGetOutgoingRelationships(const std::string& type, const std::string& key, uint16_t type_id);
        std::vector<Relationship> NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        std::vector<Relationship> NodeGetOutgoingRelationships(uint64_t id);
        std::vector<Relationship> NodeGetOutgoingRelationships(uint64_t id, const std::string& rel_type);
        std::vector<Relationship> NodeGetOutgoingRelationships(uint64_t id, uint16_t type_id);
        std::vector<Relationship> NodeGetOutgoingRelationships(uint64_t id, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(uint64_t id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(uint64_t id, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(uint64_t id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(uint64_t id, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(uint64_t id, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(uint64_t id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::string& rel_type);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(uint64_t id, uint16_t type_id);
        std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types);

        // Bulk
        std::map<Link, std::vector<Link>> LinksGetLinks(std::vector<Link> links);
        std::map<Link, std::vector<Link>> LinksGetLinks(std::vector<Link> links, Direction direction);
        std::map<Link, std::vector<Link>> LinksGetLinks(std::vector<Link> links, Direction direction, const std::string& rel_type);
        std::map<Link, std::vector<Link>> LinksGetLinks(std::vector<Link> links, Direction direction, uint16_t type_id);
        std::map<Link, std::vector<Link>> LinksGetLinks(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types);

        std::map<uint16_t, std::map<Link, std::vector<Link>>> LinksGetShardedIncomingLinks(std::vector<Link> links);
        std::map<uint16_t, std::map<Link, std::vector<Link>>> LinksGetShardedIncomingLinks(std::vector<Link> links, const std::string& rel_type);
        std::map<uint16_t, std::map<Link, std::vector<Link>>> LinksGetShardedIncomingLinks(std::vector<Link> links, uint16_t type_id);
        std::map<uint16_t, std::map<Link, std::vector<Link>>> LinksGetShardedIncomingLinks(std::vector<Link> links, const std::vector<std::string> &rel_types);

        std::map<Link, std::vector<Relationship>> LinksGetRelationships(std::map<Link, std::vector<Link>> links);


        std::map<Link, std::vector<Node>> LinksGetNeighbors(std::map<Link, std::vector<Link>> links);

        // All
        std::map<uint16_t, uint64_t> NodeCounts();
        uint64_t NodeCount(const std::string& type);
        uint64_t NodeCount(uint16_t type_id);

        std::vector<uint64_t> AllNodeIds(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<uint64_t> AllNodeIds(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<uint64_t> AllNodeIds(uint16_t type_id, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        std::vector<Node> AllNodes(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<Node> AllNodes(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<Node> AllNodes(uint16_t type_id, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        std::vector<uint64_t> AllRelationshipIds(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<uint64_t> AllRelationshipIds(const std::string& rel_type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<uint64_t> AllRelationshipIds(uint16_t type_id, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        std::vector<Relationship> AllRelationships(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<Relationship> AllRelationships(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<Relationship> AllRelationships(uint16_t type_id, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        std::map<uint16_t, uint64_t> AllRelationshipIdCounts();
        uint64_t AllRelationshipIdCounts(const std::string& type);
        uint64_t AllRelationshipIdCounts(uint16_t type_id);

        // Find
        uint64_t FindNodeCount(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value);
        uint64_t FindNodeCount(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value);

        std::vector<uint64_t> FindNodeIds(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<uint64_t> FindNodeIds(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        std::vector<Node> FindNodes(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<Node> FindNodes(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        uint64_t FindRelationshipCount(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value);
        uint64_t FindRelationshipCount(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value);

        std::vector<uint64_t> FindRelationshipIds(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<uint64_t> FindRelationshipIds(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        std::vector<Relationship> FindRelationships(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<Relationship> FindRelationships(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        // Filter
        uint64_t FilterNodeCount(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value);
        uint64_t FilterNodeCount(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value);

        std::vector<uint64_t> FilterNodeIds(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<uint64_t> FilterNodeIds(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        std::vector<Node> FilterNodes(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<Node> FilterNodes(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        uint64_t FilterRelationshipCount(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value);
        uint64_t FilterRelationshipCount(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value);

        std::vector<uint64_t> FilterRelationshipIds(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<uint64_t> FilterRelationshipIds(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        std::vector<Relationship> FilterRelationships(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        std::vector<Relationship> FilterRelationships(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);



        // *****************************************************************************************************************************
        //                                               Peered
        // *****************************************************************************************************************************

        seastar::future<std::vector<std::string>> HealthCheckPeered();
        seastar::future<std::string> RestorePeered(const std::string& name);

        // Node Types
        uint16_t NodeTypesGetCountPeered();
        seastar::future<uint64_t> NodeTypesGetCountPeered(uint16_t type_id);
        seastar::future<uint64_t> NodeTypesGetCountPeered(const std::string& type);
        std::set<std::string> NodeTypesGetPeered();
        std::map<std::string, std::string, std::less<>> NodeTypeGetPeered(const std::string& type);

        // Relationship Types
        uint16_t RelationshipTypesGetCountPeered();
        seastar::future<uint64_t> RelationshipTypesGetCountPeered(uint16_t type_id);
        seastar::future<uint64_t> RelationshipTypesGetCountPeered(const std::string& type);
        std::set<std::string> RelationshipTypesGetPeered();
        std::map<std::string, std::string, std::less<>> RelationshipTypeGetPeered(const std::string& type);

        // Node Type
        std::string NodeTypeGetTypePeered(uint16_t type_id);
        uint16_t NodeTypeGetTypeIdPeered(const std::string& type);
        seastar::future<uint16_t> NodeTypeInsertPeered(const std::string& type);
        seastar::future<bool> DeleteNodeTypePeered(const std::string& type);

        // Relationship Type
        std::string RelationshipTypeGetTypePeered(uint16_t type_id);
        uint16_t RelationshipTypeGetTypeIdPeered(const std::string& type);
        seastar::future<uint16_t> RelationshipTypeInsertPeered(const std::string& type);
        seastar::future<bool> DeleteRelationshipTypePeered(const std::string& type);

        // Node
        seastar::future<uint64_t> NodeAddEmptyPeered(const std::string& type, const std::string& key);
        seastar::future<uint64_t> NodeAddPeered(const std::string& type, const std::string& key, const std::string& properties);
        seastar::future<std::vector<uint64_t>> NodeAddManyPeered(const std::string &type, const std::vector<std::string> keys, const std::vector<std::string> properties);
        seastar::future<uint64_t> NodeGetIDPeered(const std::string& type, const std::string& key);

        seastar::future<Node> NodeGetPeered(const std::string& type, const std::string& key);
        seastar::future<Node> NodeGetPeered(uint64_t id);
        seastar::future<bool> NodeRemovePeered(const std::string& type, const std::string& key);
        seastar::future<bool> NodeRemovePeered(uint64_t id);
        seastar::future<bool> NodeRemovePeeredIncoming(uint16_t node_shard_id, uint64_t external_id);
        seastar::future<bool> NodeRemovePeeredOutgoing(uint16_t node_shard_id, uint64_t external_id);
        seastar::future<uint16_t> NodeGetTypeIdPeered(uint64_t id);
        seastar::future<std::string> NodeGetTypePeered(uint64_t id);
        seastar::future<std::string> NodeGetKeyPeered(uint64_t id);

        // Nodes
        seastar::future<std::vector<Node>> NodesGetPeered(const std::vector<uint64_t> &ids);
        seastar::future<std::vector<Node>> NodesGetPeered(const std::vector<Link>& links);
        seastar::future<std::map<uint64_t, std::string>> NodesGetKeyPeered(const std::vector<uint64_t>&);
        seastar::future<std::map<Link, std::string>> NodesGetKeyPeered(const std::vector<Link>& links);
        seastar::future<std::map<uint64_t, std::string>> NodesGetTypePeered(const std::vector<uint64_t>&);
        seastar::future<std::map<Link, std::string>> NodesGetTypePeered(const std::vector<Link>& links);
        seastar::future<std::map<uint64_t, uint16_t>> NodesGetTypeIdPeered(const std::vector<uint64_t>&);
        seastar::future<std::map<Link, uint16_t>> NodesGetTypeIdPeered(const std::vector<Link>& links);
        seastar::future<std::map<uint64_t, property_type_t>> NodesGetPropertyPeered(const std::vector<uint64_t> &ids, const std::string& property);
        seastar::future<std::map<Link, property_type_t>> NodesGetPropertyPeered(const std::vector<Link>& links, const std::string& property);
        seastar::future<std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>>> NodesGetPropertiesPeered(const std::vector<uint64_t>&);
        seastar::future<std::map<Link, std::map<std::string, property_type_t, std::less<>>>> NodesGetPropertiesPeered(const std::vector<Link>& links);

        // Property Types
        seastar::future<uint8_t> NodePropertyTypeInsertPeered(uint16_t type_id, const std::string& key, const std::string& type);
        seastar::future<uint8_t> RelationshipPropertyTypeInsertPeered(uint16_t type_id, const std::string& key, const std::string& type);
        seastar::future<uint8_t> NodePropertyTypeAddPeered(const std::string& node_type, const std::string& key, const std::string& type);
        seastar::future<uint8_t> RelationshipPropertyTypeAddPeered(const std::string& relationship_type, const std::string& key, const std::string& type);
        seastar::future<bool> NodePropertyTypeDeletePeered(const std::string& type, const std::string& key);
        seastar::future<bool> RelationshipPropertyTypeDeletePeered(const std::string& type, const std::string& key);

        // Node Properties
        seastar::future<property_type_t> NodeGetPropertyPeered(const std::string& type, const std::string& key, const std::string& property);
        seastar::future<property_type_t> NodeGetPropertyPeered(uint64_t id, const std::string& property);
        seastar::future<bool> NodeSetPropertyPeered(const std::string& type, const std::string& key, const std::string& property, const property_type_t& value);
        seastar::future<bool> NodeSetPropertyPeered(uint64_t id, const std::string& property, const property_type_t& value);
        seastar::future<bool> NodeSetPropertyFromJsonPeered(const std::string& type, const std::string& key, const std::string& property, const std::string& value);
        seastar::future<bool> NodeSetPropertyFromJsonPeered(uint64_t id, const std::string& property, const std::string& value);
        seastar::future<bool> NodeDeletePropertyPeered(const std::string& type, const std::string& key, const std::string& property);
        seastar::future<bool> NodeDeletePropertyPeered(uint64_t id, const std::string& property);

        seastar::future<std::map<std::string, property_type_t, std::less<>>> NodeGetPropertiesPeered(const std::string& type, const std::string& key);
        seastar::future<std::map<std::string, property_type_t, std::less<>>> NodeGetPropertiesPeered(uint64_t id);
        seastar::future<bool> NodeSetPropertiesFromJsonPeered(const std::string& type, const std::string& key, const std::string& value);
        seastar::future<bool> NodeSetPropertiesFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> NodeResetPropertiesFromJsonPeered(const std::string& type, const std::string& key, const std::string& value);
        seastar::future<bool> NodeResetPropertiesFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> NodeDeletePropertiesPeered(const std::string& type, const std::string& key);
        seastar::future<bool> NodeDeletePropertiesPeered(uint64_t id);

        // Relationship
        seastar::future<uint64_t> RelationshipAddEmptyPeered(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                                             const std::string& type2, const std::string& key2);
        seastar::future<uint64_t> RelationshipAddEmptyPeered(const std::string& rel_type, uint64_t id1, uint64_t id2);
        seastar::future<uint64_t> RelationshipAddEmptyPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
        seastar::future<uint64_t> RelationshipAddPeered(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                                        const std::string& type2, const std::string& key2, const std::string& properties);
        seastar::future<uint64_t> RelationshipAddPeered(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        seastar::future<uint64_t> RelationshipAddPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties);
        seastar::future<Relationship> RelationshipGetPeered(uint64_t id);

        seastar::future<bool> RelationshipRemovePeered(uint64_t id);
        seastar::future<std::string> RelationshipGetTypePeered(uint64_t id);
        seastar::future<uint16_t> RelationshipGetTypeIdPeered(uint64_t id);
        seastar::future<uint64_t> RelationshipGetStartingNodeIdPeered(uint64_t id);
        seastar::future<uint64_t> RelationshipGetEndingNodeIdPeered(uint64_t id);

        // Relationships
        seastar::future<std::vector<Relationship>> RelationshipsGetPeered(const std::vector<uint64_t> &ids);
        seastar::future<std::vector<Relationship>> RelationshipsGetPeered(const std::vector<Link> &links);
        seastar::future<std::map<uint64_t, std::string>> RelationshipsGetTypePeered(const std::vector<uint64_t>& ids);
        seastar::future<std::map<Link, std::string>> RelationshipsGetTypePeered(const std::vector<Link>& links);
        seastar::future<std::map<uint64_t, uint16_t>> RelationshipsGetTypeIdPeered(const std::vector<uint64_t>& ids);
        seastar::future<std::map<Link, uint16_t>> RelationshipsGetTypeIdPeered(const std::vector<Link>& links);

        seastar::future<std::map<uint64_t, property_type_t>> RelationshipsGetPropertyPeered(const std::vector<uint64_t> &ids, const std::string& property);
        seastar::future<std::map<Link, property_type_t>> RelationshipsGetPropertyPeered(const std::vector<Link>& links, const std::string& property);
        seastar::future<std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>>> RelationshipsGetPropertiesPeered(const std::vector<uint64_t>& ids);
        seastar::future<std::map<Link, std::map<std::string, property_type_t, std::less<>>>> RelationshipsGetPropertiesPeered(const std::vector<Link>& links);

        // Relationship Properties
        seastar::future<property_type_t> RelationshipGetPropertyPeered(uint64_t id, const std::string& property);
        seastar::future<bool> RelationshipSetPropertyPeered(uint64_t id, const std::string& property, const property_type_t& value);
        seastar::future<bool> RelationshipSetPropertyFromJsonPeered(uint64_t id, const std::string& property, const std::string& value);
        seastar::future<bool> RelationshipDeletePropertyPeered(uint64_t id, const std::string& property);

        seastar::future<std::map<std::string, property_type_t, std::less<>>> RelationshipGetPropertiesPeered(uint64_t id);
        seastar::future<bool> RelationshipSetPropertiesFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> RelationshipResetPropertiesFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> RelationshipDeletePropertiesPeered(uint64_t id);

        // Node Degree
        seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key);
        seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key, Direction direction);
        seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key, const std::string& rel_type);
        seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key, Direction direction,
                                                      const std::vector<std::string>& rel_types);
        seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key,
                                                      const std::vector<std::string>& rel_types);
        seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id);
        seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, Direction direction);
        seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, Direction direction, const std::string& rel_type);
        seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, const std::string& rel_type);
        seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);
        seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, const std::vector<std::string> &rel_types);

        // Traversing
        seastar::future<std::vector<Link>> NodeGetLinksPeered(const std::string& type, const std::string& key);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(const std::string& type, const std::string& key, Direction direction);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Link>> NodeGetLinksPeered(const std::string& type, const std::string& key, const std::string& rel_type);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(const std::string& type, const std::string& key, uint16_t type_id);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Link>> NodeGetLinksPeered(uint64_t id);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(uint64_t id, Direction direction);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(uint64_t id, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(uint64_t id, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Link>> NodeGetLinksPeered(uint64_t id, const std::string& rel_type);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(uint64_t id, uint16_t type_id);
        seastar::future<std::vector<Link>> NodeGetLinksPeered(uint64_t id, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::string& rel_type);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, uint16_t type_id);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, const std::string& rel_type);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, uint16_t type_id);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, Direction direction);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, const std::string& rel_type);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, uint16_t type_id);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, const std::string& rel_type);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, uint16_t type_id);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, Direction direction);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        // Bulk Helpers
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetIncomingRelationshipsPeered(std::vector<Link> links);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetIncomingRelationshipsPeered(std::vector<Link> links, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetIncomingRelationshipsPeered(std::vector<Link> links, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetIncomingRelationshipsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types);

        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetOutgoingRelationshipsPeered(std::vector<Link> links);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetOutgoingRelationshipsPeered(std::vector<Link> links, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetOutgoingRelationshipsPeered(std::vector<Link> links, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetOutgoingRelationshipsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types);

        seastar::future<std::map<Link, std::vector<Node>>> LinksGetIncomingNeighborsPeered(std::vector<Link> links);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetIncomingNeighborsPeered(std::vector<Link> links, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetIncomingNeighborsPeered(std::vector<Link> links, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetIncomingNeighborsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types);

        seastar::future<std::map<Link, std::vector<Node>>> LinksGetOutgoingNeighborsPeered(std::vector<Link> links);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetOutgoingNeighborsPeered(std::vector<Link> links, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetOutgoingNeighborsPeered(std::vector<Link> links, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetOutgoingNeighborsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types);

        // Bulk


        seastar::future<std::map<Link, std::vector<Link>>> LinksGetLinksPeered(std::vector<Link> links);
        seastar::future<std::map<Link, std::vector<Link>>> LinksGetLinksPeered(std::vector<Link> links, Direction direction);
        seastar::future<std::map<Link, std::vector<Link>>> LinksGetLinksPeered(std::vector<Link> links, Direction direction, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Link>>> LinksGetLinksPeered(std::vector<Link> links, Direction direction, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Link>>> LinksGetLinksPeered(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::map<Link, std::vector<Link>>> LinksGetLinksPeered(std::vector<Link> links, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Link>>> LinksGetLinksPeered(std::vector<Link> links, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Link>>> LinksGetLinksPeered(std::vector<Link> links, const std::vector<std::string> &rel_types);

        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsPeered(std::vector<Link> links);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsPeered(std::vector<Link> links, Direction direction);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsPeered(std::vector<Link> links, Direction direction, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsPeered(std::vector<Link> links, Direction direction, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsPeered(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsPeered(std::vector<Link> links, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsPeered(std::vector<Link> links, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types);

        seastar::future<std::map<Link, std::vector<Node>>> LinksGetNeighborsPeered(std::vector<Link> links);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetNeighborsPeered(std::vector<Link> links, Direction direction);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetNeighborsPeered(std::vector<Link> links, Direction direction, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetNeighborsPeered(std::vector<Link> links, Direction direction, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetNeighborsPeered(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::map<Link, std::vector<Node>>> LinksGetNeighborsPeered(std::vector<Link> links, const std::string& rel_type);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetNeighborsPeered(std::vector<Link> links, uint16_t type_id);
        seastar::future<std::map<Link, std::vector<Node>>> LinksGetNeighborsPeered(std::vector<Link> links, const std::vector<std::string> &rel_types);

        // Connected
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, const std::string& rel_type);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, uint16_t type_id);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(uint64_t id, uint64_t id2);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(uint64_t id, uint64_t id2, const std::string& rel_type);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(uint64_t id, uint64_t id2, uint16_t type_id);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(uint64_t id, uint64_t id2, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(uint64_t id, uint64_t id2, Direction direction);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(uint64_t id, uint64_t id2, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(uint64_t id, uint64_t id2, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Relationship>> NodeGetConnectedPeered(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types);

        // All
        seastar::future<std::vector<uint64_t>> AllNodeIdsPeered(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> AllNodeIdsPeered(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> AllRelationshipIdsPeered(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> AllRelationshipIdsPeered(const std::string& rel_type, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        seastar::future<std::vector<Node>> AllNodesPeered(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Node>> AllNodesPeered(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Relationship>> AllRelationshipsPeered(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Relationship>> AllRelationshipsPeered(const std::string& rel_type, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        // Find
        seastar::future<uint64_t> FindNodeCountPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value);
        seastar::future<uint64_t> FindNodeCountPeered(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value);

        seastar::future<std::vector<uint64_t>> FindNodeIdsPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> FindNodeIdsPeered(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        seastar::future<std::vector<Node>> FindNodesPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Node>> FindNodesPeered(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        seastar::future<uint64_t> FindRelationshipCountPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value);
        seastar::future<uint64_t> FindRelationshipCountPeered(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value);

        seastar::future<std::vector<uint64_t>> FindRelationshipIdsPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> FindRelationshipIdsPeered(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        seastar::future<std::vector<Relationship>> FindRelationshipsPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Relationship>> FindRelationshipsPeered(uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        // Filter
        seastar::future<uint64_t> FilterNodeCountPeered(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value);
        seastar::future<uint64_t> FilterNodeCountPeered(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value);
        seastar::future<std::vector<uint64_t>> FilterNodeIdsPeered(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> FilterNodeIdsPeered(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Node>> FilterNodesPeered(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Node>> FilterNodesPeered(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<uint64_t> FilterRelationshipCountPeered(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value);
        seastar::future<uint64_t> FilterRelationshipCountPeered(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value);
        seastar::future<std::vector<uint64_t>> FilterRelationshipIdsPeered(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> FilterRelationshipIdsPeered(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Relationship>> FilterRelationshipsPeered(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Relationship>> FilterRelationshipsPeered(std::vector<uint64_t> ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip = SKIP, uint64_t limit = LIMIT);


        // *****************************************************************************************************************************
        //                                                              Via Lua
        // *****************************************************************************************************************************

        // Helpers
        property_type_t SolObjectToProperty(const sol::object& o);
        sol::object PropertyToSolObject(const property_type_t value);
        sol::table PropertiesToSolObject(const std::map<std::string, property_type_t, std::less<>> properties);

          // Relationship Types
        uint16_t RelationshipTypesGetCountViaLua();
        uint64_t RelationshipTypesGetCountByTypeViaLua(const std::string& type);
        uint64_t RelationshipTypesGetCountByIdViaLua(uint16_t type_id);
        sol::as_table_t<std::set<std::string>> RelationshipTypesGetViaLua();
        sol::as_table_t<std::map<std::string, std::string, std::less<>>> RelationshipTypeGetViaLua(const std::string& type);

        // Relationship Type
        std::string RelationshipTypeGetTypeViaLua(uint16_t type_id);
        uint16_t RelationshipTypeGetTypeIdViaLua(const std::string& type);
        uint16_t RelationshipTypeInsertViaLua(const std::string& type);

        // Node Types
        uint16_t NodeTypesGetCountViaLua();
        uint64_t NodeTypesGetCountByTypeViaLua(const std::string& type);
        uint64_t NodeTypesGetCountByIdViaLua(uint16_t type_id);
        sol::as_table_t<std::set<std::string>> NodeTypesGetViaLua();
        sol::as_table_t<std::map<std::string, std::string, std::less<>>> NodeTypeGetViaLua(const std::string& type);

        // Node Type
        std::string NodeTypeGetTypeViaLua(uint16_t type_id);
        uint16_t NodeTypeGetTypeIdViaLua(const std::string& type);
        uint16_t NodeTypeInsertViaLua(const std::string& type);

        //Node
        uint64_t NodeAddEmptyViaLua(const std::string& type, const std::string& key);
        uint64_t NodeAddViaLua(const std::string& type, const std::string& key, const std::string& properties);
        std::vector<uint64_t> NodeAddManyPeeredViaLua(const std::string &type, const std::vector<std::string> keys, const std::vector<std::string> properties);
        uint64_t NodeGetIdViaLua(const std::string& type, const std::string& key);

        Node NodeGetViaLua(const std::string& type, const std::string& key);
        Node NodeGetByIdViaLua(uint64_t id);
        bool NodeRemoveViaLua(const std::string& type, const std::string& key);
        bool NodeRemoveByIdViaLua(uint64_t id);
        uint16_t NodeGetTypeIdViaLua(uint64_t id);
        std::string NodeGetTypeViaLua(uint64_t id);
        std::string NodeGetKeyViaLua(uint64_t id);

        // Nodes
        /*
         * Lua or LuaJit has a problem here with returning maps that have keys which are large numbers.
         * Since the internal nodes and relationship ids are rather large, this is a problem.
         * Options are: 1. Return values in order received.
         *              2. Return values in ascending order (they come back this way already due to std::map being ordered).
         *              3. Convert number to String (yuck).
         * Since these will most likely be called from roaring bitmaps which will be in order anyway, I'm going with number 2 for now.
         * Same deal with relationships
         */
        sol::as_table_t<std::vector<Node>> NodesGetViaLua(const std::vector<uint64_t>& ids);
        sol::as_table_t<std::vector<Node>> NodesGetByLinksViaLua(const std::vector<Link>& links);
        sol::as_table_t<std::vector<std::string>> NodesGetKeyViaLua(const std::vector<uint64_t>& ids);
        sol::as_table_t<std::map<Link, std::string>> NodesGetKeyByLinksViaLua(const std::vector<Link>& links);
        sol::as_table_t<std::vector<std::string>> NodesGetTypeViaLua(const std::vector<uint64_t>& ids);
        sol::as_table_t<std::map<Link, std::string>> NodesGetTypeByLinksViaLua(const std::vector<Link>& links);
        sol::as_table_t<std::vector<uint16_t>> NodesGetTypeIdViaLua(const std::vector<uint64_t>& ids);
        sol::as_table_t<std::map<Link, uint16_t>> NodesGetTypeIdByLinksViaLua(const std::vector<Link>& links);
        sol::as_table_t<std::vector<sol::object>> NodesGetPropertyViaLua(const std::vector<uint64_t>& ids, const std::string& property);
        sol::as_table_t<std::map<Link, sol::object>> NodesGetPropertyByLinksViaLua(const std::vector<Link>& links, const std::string& property);
        sol::as_table_t<std::vector<sol::object>> NodesGetPropertiesViaLua(const std::vector<uint64_t>& ids);
        sol::as_table_t<std::map<Link, sol::object>> NodesGetPropertiesByLinksViaLua(const std::vector<Link>& links);

        // Property Types
        uint8_t NodePropertyTypeAddViaLua(const std::string& node_type, const std::string& key, const std::string& type);
        uint8_t RelationshipPropertyTypeAddViaLua(const std::string& relationship_type, const std::string& key, const std::string& type);
        bool NodePropertyTypeDeleteViaLua(const std::string& type, const std::string& key);
        bool RelationshipPropertyTypeDeleteViaLua(const std::string& type, const std::string& key);

        // Node Properties
        sol::object NodeGetPropertyViaLua(const std::string& type, const std::string& key, const std::string& property);
        sol::object NodeGetPropertyByIdViaLua(uint64_t id, const std::string& property);
        sol::object NodeGetPropertiesViaLua(const std::string& type, const std::string& key);
        sol::object NodeGetPropertiesByIdViaLua(uint64_t id);
        bool NodeSetPropertyViaLua(const std::string& type, const std::string& key, const std::string& property, const sol::object& value);
        bool NodeSetPropertyByIdViaLua(uint64_t id, const std::string& property, const sol::object& value);
        bool NodeSetPropertyFromJsonViaLua(const std::string& type, const std::string& key, const std::string& property, const std::string& value);
        bool NodeSetPropertyFromJsonByIdViaLua(uint64_t id, const std::string& property, const std::string& value);
        bool NodeSetPropertiesFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value);
        bool NodeSetPropertiesFromJsonByIdViaLua(uint64_t id, const std::string& value);
        bool NodeResetPropertiesFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value);
        bool NodeResetPropertiesFromJsonByIdViaLua(uint64_t id, const std::string& value);
        bool NodeDeletePropertyViaLua(const std::string& type, const std::string& key, const std::string& property);
        bool NodeDeletePropertyByIdViaLua(uint64_t id, const std::string& property);
        bool NodeDeletePropertiesViaLua(const std::string& type, const std::string& key);
        bool NodeDeletePropertiesByIdViaLua(uint64_t id);

        // Relationship
        uint64_t RelationshipAddEmptyViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                            const std::string& type2, const std::string& key2);
        uint64_t RelationshipAddEmptyByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
        uint64_t RelationshipAddEmptyByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2);
        uint64_t RelationshipAddViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                       const std::string& type2, const std::string& key2, const std::string& properties);
        uint64_t RelationshipAddByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties);
        uint64_t RelationshipAddByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        Relationship RelationshipGetViaLua(uint64_t id);
        bool RelationshipRemoveViaLua(uint64_t id);
        std::string RelationshipGetTypeViaLua(uint64_t id);
        uint16_t RelationshipGetTypeIdViaLua(uint64_t id);
        uint64_t RelationshipGetStartingNodeIdViaLua(uint64_t id);
        uint64_t RelationshipGetEndingNodeIdViaLua(uint64_t id);

        // Relationships
        sol::as_table_t<std::vector<Relationship>> RelationshipsGetViaLua(const std::vector<uint64_t> &ids);
        sol::as_table_t<std::vector<Relationship>> RelationshipsGetByLinksViaLua(const std::vector<Link>& links);
        sol::as_table_t<std::vector<std::string>> RelationshipsGetTypeViaLua(const std::vector<uint64_t>& ids);
        sol::as_table_t<std::map<Link, std::string>> RelationshipsGetTypeByLinksViaLua(const std::vector<Link>& links);
        sol::as_table_t<std::vector<uint16_t>> RelationshipsGetTypeIdViaLua(const std::vector<uint64_t>& ids);
        sol::as_table_t<std::map<Link, uint16_t>> RelationshipsGetTypeIdByLinksViaLua(const std::vector<Link>& links);
        sol::as_table_t<std::vector<sol::object>> RelationshipsGetPropertyViaLua(const std::vector<uint64_t>& ids, const std::string& property);
        sol::as_table_t<std::map<Link, sol::object>> RelationshipsGetPropertyByLinksViaLua(const std::vector<Link>& links, const std::string& property);
        sol::as_table_t<std::vector<sol::object>> RelationshipsGetPropertiesViaLua(const std::vector<uint64_t>& ids);
        sol::as_table_t<std::map<Link, sol::object>> RelationshipsGetPropertiesByLinksViaLua(const std::vector<Link>& links);

        // Relationship Properties
        sol::object RelationshipGetPropertyViaLua(uint64_t id, const std::string& property);
        sol::object RelationshipGetPropertiesViaLua(uint64_t id);
        bool RelationshipSetPropertyViaLua(uint64_t id, const std::string& property, const sol::object& value);
        bool RelationshipSetPropertyFromJsonViaLua(uint64_t id, const std::string& property, const std::string& value);
        bool RelationshipDeletePropertyViaLua(uint64_t id, const std::string& property);
        bool RelationshipSetPropertiesFromJsonViaLua(uint64_t id, const std::string &value);
        bool RelationshipResetPropertiesFromJsonViaLua(uint64_t id, const std::string &value);
        bool RelationshipDeletePropertiesViaLua(uint64_t id);

        // Node Degree
        uint64_t NodeGetDegreeViaLua(const std::string& type, const std::string& key);
        uint64_t NodeGetDegreeForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
        uint64_t NodeGetDegreeForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        uint64_t NodeGetDegreeForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
        uint64_t NodeGetDegreeForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction,
                                                         const std::vector<std::string>& rel_types);
        uint64_t NodeGetDegreeForTypesViaLua(const std::string& type, const std::string& key,
                                             const std::vector<std::string>& rel_types);
        uint64_t NodeGetDegreeByIdViaLua(uint64_t id);
        uint64_t NodeGetDegreeByIdForDirectionViaLua(uint64_t id, Direction direction);
        uint64_t NodeGetDegreeByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
        uint64_t NodeGetDegreeByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
        uint64_t NodeGetDegreeByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);
        uint64_t NodeGetDegreeByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

        // Traversing
        sol::as_table_t<std::vector<Link>> NodeGetLinksViaLua(const std::string& type, const std::string& key);
        sol::as_table_t<std::vector<Link>> NodeGetLinksForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
        sol::as_table_t<std::vector<Link>> NodeGetLinksForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        sol::as_table_t<std::vector<Link>> NodeGetLinksForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        sol::as_table_t<std::vector<Link>> NodeGetLinksForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Link>> NodeGetLinksForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
        sol::as_table_t<std::vector<Link>> NodeGetLinksForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id);
        sol::as_table_t<std::vector<Link>> NodeGetLinksForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Link>> NodeGetLinksByIdViaLua(uint64_t id);
        sol::as_table_t<std::vector<Link>> NodeGetLinksByIdForDirectionViaLua(uint64_t id, Direction direction);
        sol::as_table_t<std::vector<Link>> NodeGetLinksByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
        sol::as_table_t<std::vector<Link>> NodeGetLinksByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id);
        sol::as_table_t<std::vector<Link>> NodeGetLinksByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Link>> NodeGetLinksByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
        sol::as_table_t<std::vector<Link>> NodeGetLinksByIdForTypeIdViaLua(uint64_t id, uint16_t type_id);
        sol::as_table_t<std::vector<Link>> NodeGetLinksByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsViaLua(const std::string& type, const std::string& key);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdViaLua(uint64_t id);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForDirectionViaLua(uint64_t id, Direction direction);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id);
        sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Node>> NodeGetNeighborsViaLua(const std::string& type, const std::string& key);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdViaLua(uint64_t id);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Node>> NodeGetNeighborsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForDirectionViaLua(uint64_t id, Direction direction);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id);
        sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        // Bulk
        sol::nested<std::map<Link, std::vector<Link>>> LinksGetLinksViaLua(std::vector<Link> links);
        sol::nested<std::map<Link, std::vector<Link>>> LinksGetLinksForDirectionViaLua(std::vector<Link> links, Direction direction);
        sol::nested<std::map<Link, std::vector<Link>>> LinksGetLinksForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type);
        sol::nested<std::map<Link, std::vector<Link>>> LinksGetLinksForDirectionForTypeIdViaLua(std::vector<Link> links, Direction direction, uint16_t type_id);
        sol::nested<std::map<Link, std::vector<Link>>> LinksGetLinksForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types);

        sol::nested<std::map<Link, std::vector<Link>>> LinksGetLinksForTypeViaLua(std::vector<Link> links, const std::string& rel_type);
        sol::nested<std::map<Link, std::vector<Link>>> LinksGetLinksForTypeIdViaLua(std::vector<Link> links, uint16_t type_id);
        sol::nested<std::map<Link, std::vector<Link>>> LinksGetLinksForTypesViaLua(std::vector<Link> links, const std::vector<std::string> &rel_types);

        sol::nested<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsViaLua(std::vector<Link> links);
        sol::nested<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsForDirectionViaLua(std::vector<Link> links, Direction direction);
        sol::nested<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type);
        sol::nested<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsForDirectionForTypeIdViaLua(std::vector<Link> links, Direction direction, uint16_t type_id);
        sol::nested<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types);

        sol::nested<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsForTypeViaLua(std::vector<Link> links, const std::string& rel_type);
        sol::nested<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsForTypeIdViaLua(std::vector<Link> links, uint16_t type_id);
        sol::nested<std::map<Link, std::vector<Relationship>>> LinksGetRelationshipsForTypesViaLua(std::vector<Link> links, const std::vector<std::string> &rel_types);

        sol::nested<std::map<Link, std::vector<Node>>> LinksGetNeighborsViaLua(std::vector<Link> links);
        sol::nested<std::map<Link, std::vector<Node>>> LinksGetNeighborsForDirectionViaLua(std::vector<Link> links, Direction direction);
        sol::nested<std::map<Link, std::vector<Node>>> LinksGetNeighborsForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type);
        sol::nested<std::map<Link, std::vector<Node>>> LinksGetNeighborsForDirectionForTypeIdViaLua(std::vector<Link> links, Direction direction, uint16_t type_id);
        sol::nested<std::map<Link, std::vector<Node>>> LinksGetNeighborsForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types);

        sol::nested<std::map<Link, std::vector<Node>>> LinksGetNeighborsForTypeViaLua(std::vector<Link> links, const std::string& rel_type);
        sol::nested<std::map<Link, std::vector<Node>>> LinksGetNeighborsForTypeIdViaLua(std::vector<Link> links, uint16_t type_id);
        sol::nested<std::map<Link, std::vector<Node>>> LinksGetNeighborsForTypesViaLua(std::vector<Link> links, const std::vector<std::string> &rel_types);

        // All
        sol::as_table_t<std::vector<uint64_t>> AllNodeIdsViaLua(sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<uint64_t>> AllNodeIdsForTypeViaLua(const std::string& type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<uint64_t>> AllRelationshipIdsViaLua(sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<uint64_t>> AllRelationshipIdsForTypeViaLua(const std::string& rel_type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);

        sol::as_table_t<std::vector<Node>> AllNodesViaLua(sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<Node>> AllNodesForTypeViaLua(const std::string& type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<Relationship>> AllRelationshipsViaLua(sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<Relationship>> AllRelationshipsForTypeViaLua(const std::string& rel_type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);

        // Find
        uint64_t FindNodeCountViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object);
        sol::as_table_t<std::vector<uint64_t>> FindNodeIdsViaLua(const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<Node>> FindNodesViaLua(const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);

        uint64_t FindRelationshipCountViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object);
        sol::as_table_t<std::vector<uint64_t>> FindRelationshipIdsViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<Relationship>> FindRelationshipsViaLua(const std::string& type, const std::string& property, const Operation& operation, const sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);

        // Filter
        uint64_t FilterNodeCountViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const sol::object& object);
        sol::as_table_t<std::vector<uint64_t>> FilterNodeIdsViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<Node>> FilterNodesViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        uint64_t FilterRelationshipCountViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const sol::object& object);
        sol::as_table_t<std::vector<uint64_t>> FilterRelationshipIdsViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);
        sol::as_table_t<std::vector<Relationship>> FilterRelationshipsViaLua(std::vector<uint64_t> ids, const std::string& type, const std::string& property, const Operation& operation, const  sol::object& object, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit);


        // Partition by Shards
        std::map<uint16_t, std::vector<std::tuple<std::string, std::string>>> PartitionNodesByNodeTypeKeys(const std::string& type, const std::vector<std::string> &keys, const std::vector<std::string> &properties);
        std::map<uint16_t, std::vector<uint64_t>> PartitionIdsByShardId(const std::vector<uint64_t> &ids) const;
        std::map<uint16_t, std::vector<uint64_t>> PartitionNodeIdsByShardId(const std::vector<Link> &links) const;
        std::map<uint16_t, std::vector<uint64_t>> PartitionRelationshipIdsByShardId(const std::vector<Link> &links) const;
        std::map<uint16_t, std::vector<Link>> PartitionLinksByNodeShardId(const std::vector<Link> &links) const;
        std::map<uint16_t, std::vector<Link>> PartitionLinksByRelationshipShardId(const std::vector<Link> &links) const;

        [[maybe_unused]] void insert_sorted(uint64_t id1, uint64_t external_id, std::vector<Link> &group) const;
  };
}


#endif //RAGEDB_SHARD_H

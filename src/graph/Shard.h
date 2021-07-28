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

#include <any>
#include <seastar/core/sharded.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/thread.hh>
#include <simdjson.h>
#include <sol/sol.hpp>
#include <tsl/sparse_map.h>
#include "Direction.h"
#include "Node.h"
#include "Relationship.h"
#include "NodeTypes.h"
#include "RelationshipTypes.h"

namespace ragedb {

    class Shard : public seastar::peering_sharded_service<Shard> {

    private:
        uint cpus;
        uint shard_id;
        

        seastar::rwlock rel_type_lock;                  // Global lock to keep Relationship Type ids in sync
        seastar::rwlock node_type_lock;                 // Global lock to keep Node Type ids in sync
        seastar::rwlock lua_lock;                       // Per Shard lock to run a single Lua script each

        sol::state lua;                                 // Lua State

        NodeTypes node_types;                           // Store string and id of node types
        RelationshipTypes relationship_types;           // Store string and id of relationship types

        inline static const uint64_t SKIP = 0;
        inline static const uint64_t LIMIT = 100;
        inline static const std::string EXCEPTION = "An exception has occurred: ";

    public:
        explicit Shard(uint _cpus);

        static seastar::future<> stop();
        void Clear();

        seastar::future<std::string> RunLua(const std::string &script);

        // Ids
        uint64_t internalToExternal(uint16_t type_id, uint64_t internal_id) const;
        static uint64_t externalToInternal(uint64_t id);
        static uint16_t externalToTypeId(uint64_t id);
        static uint16_t CalculateShardId(uint64_t id);
        uint16_t CalculateShardId(const std::string &type, const std::string &key) const;
        bool ValidNodeId(uint64_t id);
        bool ValidRelationshipId(uint64_t id);

        // *****************************************************************************************************************************
        //                                               Single Shard
        // *****************************************************************************************************************************

        static seastar::future<std::string> HealthCheck();

        // Node Types
        uint16_t NodeTypesGetCount();
        uint64_t NodeTypesGetCount(uint16_t type_id);
        uint64_t NodeTypesGetCount(const std::string& type);
        std::set<std::string> NodeTypesGet();
        std::map<std::string, std::string> NodeTypeGet(const std::string& type);

        // Relationship Types
        uint16_t RelationshipTypesGetCount();
        uint64_t RelationshipTypesGetCount(uint16_t type_id);
        uint64_t RelationshipTypesGetCount(const std::string& type);
        std::set<std::string> RelationshipTypesGet();
        std::map<std::string, std::string> RelationshipTypeGet(const std::string& type);

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

        // Nodes
        uint64_t NodeAddEmpty(uint16_t type_id, const std::string& key);
        uint64_t NodeAdd(uint16_t type_id, const std::string& key, const std::string& properties);
        uint64_t NodeGetID(const std::string& type, const std::string& key);
        std::vector<Node> NodesGet(const std::vector<uint64_t>&);
        Node NodeGet(uint64_t id);
        Node NodeGet(const std::string& type, const std::string& key);
        static uint16_t NodeGetTypeId(uint64_t id);
        std::string NodeGetType(uint64_t id);
        std::string NodeGetKey(uint64_t id);
        bool NodeRemove(uint64_t id);
        bool NodeRemove(const std::string& type, const std::string& key);

        // Node Property
        std::any NodePropertyGet(uint64_t id, const std::string& property);
        bool NodePropertySet(uint64_t id, const std::string& property, std::any value);
        bool NodePropertySetFromJson(uint64_t id, const std::string& property, const std::string& value);
        std::any NodePropertyGet(const std::string& type, const std::string& key, const std::string& property);
        bool NodePropertySet(const std::string& type, const std::string& key, const std::string& property, std::any value);
        bool NodePropertySetFromJson(const std::string& type, const std::string& key, const std::string& property,const std::string& value);
        bool NodePropertyDelete(const std::string& type, const std::string& key, const std::string& property);
        bool NodePropertyDelete(uint64_t id, const std::string& property);

        // Node Properties
        std::map<std::string, std::any> NodePropertiesGet(const std::string& type, const std::string& key);
        std::map<std::string, std::any> NodePropertiesGet(uint64_t id);
        bool NodePropertiesSetFromJson(const std::string& type, const std::string& key, const std::string& value);
        bool NodePropertiesSetFromJson(uint64_t id, const std::string& value);
        bool NodePropertiesResetFromJson(const std::string& type, const std::string& key, const std::string& value);
        bool NodePropertiesResetFromJson(uint64_t id, const std::string& value);
        bool NodePropertiesDelete(const std::string& type, const std::string& key);
        bool NodePropertiesDelete(uint64_t id);

        // Relationships
        uint64_t RelationshipAddEmptySameShard(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
        uint64_t RelationshipAddEmptySameShard(uint16_t rel_type, const std::string& type1, const std::string& key1,
                                               const std::string& type2, const std::string& key2);
        uint64_t RelationshipAddEmptyToOutgoing(uint16_t rel_type, uint64_t id1, uint64_t id2);
        uint64_t RelationshipAddToIncoming(uint16_t rel_type, uint64_t rel_id, uint64_t id1, uint64_t id2);

        uint64_t RelationshipAddSameShard(uint16_t rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        uint64_t RelationshipAddSameShard(uint16_t rel_type, const std::string& type1, const std::string& key1,
                                          const std::string& type2, const std::string& key2, const std::string& properties);
        uint64_t RelationshipAddToOutgoing(uint16_t rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        std::vector<Relationship> RelationshipsGet(const std::vector<uint64_t>&);
        Relationship RelationshipGet(uint64_t rel_id);
        std::string RelationshipGetType(uint64_t id);
        uint16_t RelationshipGetTypeId(uint64_t id);
        uint64_t RelationshipGetStartingNodeId(uint64_t id);
        uint64_t RelationshipGetEndingNodeId(uint64_t id);

        // Relationship Property
        std::any RelationshipPropertyGet(uint64_t id, const std::string& property);
        bool RelationshipPropertySet(uint64_t id, const std::string& property, const std::any& value);
        bool RelationshipPropertySetFromJson(uint64_t id, const std::string& property, const std::string& value);
        bool RelationshipPropertyDelete(uint64_t id, const std::string& property);

        // Relationship Properties
        std::map<std::string, std::any> RelationshipPropertiesGet(uint64_t id);
        bool RelationshipPropertiesSetFromJson(uint64_t id, const std::string& value);
        bool RelationshipPropertiesResetFromJson(uint64_t id, const std::string& value);
        bool RelationshipPropertiesDelete(uint64_t id);

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
        std::vector<Link> NodeGetRelationshipsIDs(const std::string& type, const std::string& key);
        std::vector<Link> NodeGetRelationshipsIDs(const std::string& type, const std::string& key, Direction direction);
        std::vector<Link> NodeGetRelationshipsIDs(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        std::vector<Link> NodeGetRelationshipsIDs(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        std::vector<Link> NodeGetRelationshipsIDs(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);
        std::vector<Link> NodeGetRelationshipsIDs(uint64_t id);
        std::vector<Link> NodeGetRelationshipsIDs(uint64_t id, Direction direction);
        std::vector<Link> NodeGetRelationshipsIDs(uint64_t id, Direction direction, const std::string& rel_type);
        std::vector<Link> NodeGetRelationshipsIDs(uint64_t id, Direction direction, uint16_t type_id);
        std::vector<Link> NodeGetRelationshipsIDs(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

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

        // All
        std::map<uint16_t, uint64_t> AllNodeIdCounts();
        uint64_t AllNodeIdCounts(const std::string& type);
        uint64_t AllNodeIdCounts(uint16_t type_id);

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

        // *****************************************************************************************************************************
        //                                               Peered
        // *****************************************************************************************************************************

        seastar::future<std::vector<std::string>> HealthCheckPeered();

        // Node Types
        uint16_t NodeTypesGetCountPeered();
        seastar::future<uint64_t> NodeTypesGetCountPeered(uint16_t type_id);
        seastar::future<uint64_t> NodeTypesGetCountPeered(const std::string& type);
        std::set<std::string> NodeTypesGetPeered();

        // Relationship Types
        uint16_t RelationshipTypesGetCountPeered();
        seastar::future<uint64_t> RelationshipTypesGetCountPeered(uint16_t type_id);
        seastar::future<uint64_t> RelationshipTypesGetCountPeered(const std::string& type);
        std::set<std::string> RelationshipTypesGetPeered();

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

        // Nodes
        seastar::future<uint64_t> NodeAddEmptyPeered(const std::string& type, const std::string& key);
        seastar::future<uint64_t> NodeAddPeered(const std::string& type, const std::string& key, const std::string& properties);
        seastar::future<uint64_t> NodeGetIDPeered(const std::string& type, const std::string& key);

        seastar::future<Node> NodeGetPeered(const std::string& type, const std::string& key);
        seastar::future<Node> NodeGetPeered(uint64_t id);
        seastar::future<std::vector<Node>> NodesGetPeered(const std::vector<uint64_t> &ids);
        seastar::future<bool> NodeRemovePeered(const std::string& type, const std::string& key);
        seastar::future<bool> NodeRemovePeered(uint64_t id);
        seastar::future<uint16_t> NodeGetTypeIdPeered(uint64_t id);
        seastar::future<std::string> NodeGetTypePeered(uint64_t id);
        seastar::future<std::string> NodeGetKeyPeered(uint64_t id);

        // Property Types
        seastar::future<uint8_t> NodePropertyTypeInsertPeered(uint16_t type_id, const std::string& key, const std::string& type);
        seastar::future<uint8_t> RelationshipPropertyTypeInsertPeered(uint16_t type_id, const std::string& key, const std::string& type);
        seastar::future<uint8_t> NodePropertyTypeAddPeered(const std::string& node_type, const std::string& key, const std::string& type);
        seastar::future<uint8_t> RelationshipPropertyTypeAddPeered(const std::string& relationship_type, const std::string& key, const std::string& type);
        seastar::future<bool> NodePropertyTypeDeletePeered(const std::string& type, const std::string& key);
        seastar::future<bool> RelationshipPropertyTypeDeletePeered(const std::string& type, const std::string& key);

        // Node Properties
        seastar::future<std::any> NodePropertyGetPeered(const std::string& type, const std::string& key, const std::string& property);
        seastar::future<std::any> NodePropertyGetPeered(uint64_t id, const std::string& property);
        seastar::future<bool> NodePropertySetPeered(const std::string& type, const std::string& key, const std::string& property, const std::any& value);
        seastar::future<bool> NodePropertySetPeered(uint64_t id, const std::string& property, const std::any& value);
        seastar::future<bool> NodePropertySetFromJsonPeered(const std::string& type, const std::string& key, const std::string& property, const std::string& value);
        seastar::future<bool> NodePropertySetFromJsonPeered(uint64_t id, const std::string& property, const std::string& value);
        seastar::future<bool> NodePropertyDeletePeered(const std::string& type, const std::string& key, const std::string& property);
        seastar::future<bool> NodePropertyDeletePeered(uint64_t id, const std::string& property);

        seastar::future<std::map<std::string, std::any>> NodePropertiesGetPeered(const std::string& type, const std::string& key);
        seastar::future<std::map<std::string, std::any>> NodePropertiesGetPeered(uint64_t id);
        seastar::future<bool> NodePropertiesSetFromJsonPeered(const std::string& type, const std::string& key, const std::string& value);
        seastar::future<bool> NodePropertiesSetFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> NodePropertiesResetFromJsonPeered(const std::string& type, const std::string& key, const std::string& value);
        seastar::future<bool> NodePropertiesResetFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> NodePropertiesDeletePeered(const std::string& type, const std::string& key);
        seastar::future<bool> NodePropertiesDeletePeered(uint64_t id);

        // Relationships
        seastar::future<uint64_t> RelationshipAddEmptyPeered(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                                             const std::string& type2, const std::string& key2);
        seastar::future<uint64_t> RelationshipAddEmptyPeered(const std::string& rel_type, uint64_t id1, uint64_t id2);
        seastar::future<uint64_t> RelationshipAddEmptyPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
        seastar::future<uint64_t> RelationshipAddPeered(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                                        const std::string& type2, const std::string& key2, const std::string& properties);
        seastar::future<uint64_t> RelationshipAddPeered(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        seastar::future<uint64_t> RelationshipAddPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties);
        seastar::future<Relationship> RelationshipGetPeered(uint64_t id);
        seastar::future<std::vector<Relationship>> RelationshipsGetPeered(const std::vector<uint64_t> &ids);
        seastar::future<bool> RelationshipRemovePeered(uint64_t id);
        seastar::future<std::string> RelationshipGetTypePeered(uint64_t id);
        seastar::future<uint16_t> RelationshipGetTypeIdPeered(uint64_t id);
        seastar::future<uint64_t> RelationshipGetStartingNodeIdPeered(uint64_t id);
        seastar::future<uint64_t> RelationshipGetEndingNodeIdPeered(uint64_t id);

        // Relationship Properties
        seastar::future<std::any> RelationshipPropertyGetPeered(uint64_t id, const std::string& property);
        seastar::future<bool> RelationshipPropertySetPeered(uint64_t id, const std::string& property, const std::any& value);
        seastar::future<bool> RelationshipPropertySetFromJsonPeered(uint64_t id, const std::string& property, const std::string& value);
        seastar::future<bool> RelationshipPropertyDeletePeered(uint64_t id, const std::string& property);

        seastar::future<std::map<std::string, std::any>> RelationshipPropertiesGetPeered(uint64_t id);
        seastar::future<bool> RelationshipPropertiesSetFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> RelationshipPropertiesResetFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> RelationshipPropertiesDeletePeered(uint64_t id);

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
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, Direction direction);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, const std::string& rel_type);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, uint16_t type_id);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(uint64_t id);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(uint64_t id, Direction direction);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(uint64_t id, Direction direction, const std::string& rel_type);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(uint64_t id, Direction direction, uint16_t type_id);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(uint64_t id, const std::string& rel_type);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(uint64_t id, uint16_t type_id);
        seastar::future<std::vector<Link>> NodeGetRelationshipsIDsPeered(uint64_t id, const std::vector<std::string> &rel_types);

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

        // All
        seastar::future<std::vector<uint64_t>> AllNodeIdsPeered(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> AllNodeIdsPeered(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> AllRelationshipIdsPeered(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<uint64_t>> AllRelationshipIdsPeered(const std::string& rel_type, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        seastar::future<std::vector<Node>> AllNodesPeered(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Node>> AllNodesPeered(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Relationship>> AllRelationshipsPeered(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        seastar::future<std::vector<Relationship>> AllRelationshipsPeered(const std::string& rel_type, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        // *****************************************************************************************************************************
        //                                                              Via Lua
        // *****************************************************************************************************************************

        // Relationship Types
        uint16_t RelationshipTypesGetCountViaLua();
        uint64_t RelationshipTypesGetCountByTypeViaLua(const std::string& type);
        uint64_t RelationshipTypesGetCountByIdViaLua(uint16_t type_id);
        sol::as_table_t<std::set<std::string>> RelationshipTypesGetViaLua();

        // Relationship Type
        std::string RelationshipTypeGetTypeViaLua(uint16_t type_id);
        uint16_t RelationshipTypeGetTypeIdViaLua(const std::string& type);
        uint16_t RelationshipTypeInsertViaLua(const std::string& type);

        // Node Types
        uint16_t NodeTypesGetCountViaLua();
        uint64_t NodeTypesGetCountByTypeViaLua(const std::string& type);
        uint64_t NodeTypesGetCountByIdViaLua(uint16_t type_id);
        sol::as_table_t<std::set<std::string>> NodeTypesGetViaLua();

        // Node Type
        std::string NodeTypeGetTypeViaLua(uint16_t type_id);
        uint16_t NodeTypeGetTypeIdViaLua(const std::string& type);
        uint16_t NodeTypeInsertViaLua(const std::string& type);

        //Nodes
        uint64_t NodeAddEmptyViaLua(const std::string& type, const std::string& key);
        uint64_t NodeAddViaLua(const std::string& type, const std::string& key, const std::string& properties);
        uint64_t NodeGetIdViaLua(const std::string& type, const std::string& key);
        sol::as_table_t<std::vector<Node>> NodesGetViaLua(const std::vector<uint64_t>&);
        Node NodeGetViaLua(const std::string& type, const std::string& key);
        Node NodeGetByIdViaLua(uint64_t id);
        bool NodeRemoveViaLua(const std::string& type, const std::string& key);
        bool NodeRemoveByIdViaLua(uint64_t id);
        uint16_t NodeGetTypeIdViaLua(uint64_t id);
        std::string NodeGetTypeViaLua(uint64_t id);
        std::string NodeGetKeyViaLua(uint64_t id);

        // Node Properties
        sol::object NodePropertyGetViaLua(const std::string& type, const std::string& key, const std::string& property);
        sol::object NodePropertyGetByIdViaLua(uint64_t id, const std::string& property);
        bool NodePropertySetViaLua(const std::string& type, const std::string& key, const std::string& property, const sol::object& value);
        bool NodePropertySetByIdViaLua(uint64_t id, const std::string& property, const sol::object& value);
        bool NodePropertiesSetFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value);
        bool NodePropertiesSetFromJsonByIdViaLua(uint64_t id, const std::string& value);
        bool NodePropertiesResetFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value);
        bool NodePropertiesResetFromJsonByIdViaLua(uint64_t id, const std::string& value);
        bool NodePropertyDeleteViaLua(const std::string& type, const std::string& key, const std::string& property);
        bool NodePropertyDeleteByIdViaLua(uint64_t id, const std::string& property);
        bool NodePropertiesDeleteViaLua(const std::string& type, const std::string& key);
        bool NodePropertiesDeleteByIdViaLua(uint64_t id);

        // Relationships
        uint64_t RelationshipAddEmptyViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                            const std::string& type2, const std::string& key2);
        uint64_t RelationshipAddEmptyByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
        uint64_t RelationshipAddEmptyByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2);
        uint64_t RelationshipAddViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                       const std::string& type2, const std::string& key2, const std::string& properties);
        uint64_t RelationshipAddByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties);
        uint64_t RelationshipAddByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        sol::as_table_t<std::vector<Relationship>> RelationshipsGetViaLua(const std::vector<uint64_t> &ids);
        Relationship RelationshipGetViaLua(uint64_t id);
        bool RelationshipRemoveViaLua(uint64_t id);
        std::string RelationshipGetTypeViaLua(uint64_t id);
        uint16_t RelationshipGetTypeIdViaLua(uint64_t id);
        uint64_t RelationshipGetStartingNodeIdViaLua(uint64_t id);
        uint64_t RelationshipGetEndingNodeIdViaLua(uint64_t id);

        // Relationship Properties
        sol::object RelationshipPropertyGetViaLua(uint64_t id, const std::string& property);
        bool RelationshipPropertySetViaLua(uint64_t id, const std::string& property, const sol::object& value);
        bool RelationshipPropertySetFromJsonViaLua(uint64_t id, const std::string& property, const std::string& value);
        bool RelationshipPropertyDeleteViaLua(uint64_t id, const std::string& property);
        bool RelationshipPropertiesSetFromJsonViaLua(uint64_t id, const std::string &value);
        bool RelationshipPropertiesResetFromJsonViaLua(uint64_t id, const std::string &value);
        bool RelationshipPropertiesDeleteViaLua(uint64_t id);

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
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsViaLua(const std::string& type, const std::string& key);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsByIdViaLua(uint64_t id);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsByIdForDirectionViaLua(uint64_t id, Direction direction);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id);
        sol::as_table_t<std::vector<Link>> NodeGetRelationshipsIdsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

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

        // All
        sol::as_table_t<std::vector<uint64_t>> AllNodeIdsViaLua(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        sol::as_table_t<std::vector<uint64_t>> AllNodeIdsForTypeViaLua(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        sol::as_table_t<std::vector<uint64_t>> AllRelationshipIdsViaLua(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        sol::as_table_t<std::vector<uint64_t>> AllRelationshipIdsForTypeViaLua(const std::string& rel_type, uint64_t skip = SKIP, uint64_t limit = LIMIT);

        sol::as_table_t<std::vector<Node>> AllNodesViaLua(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        sol::as_table_t<std::vector<Node>> AllNodesForTypeViaLua(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
        sol::as_table_t<std::vector<Relationship>> AllRelationshipsViaLua(uint64_t skip = SKIP, uint64_t limit = LIMIT);
        sol::as_table_t<std::vector<Relationship>> AllRelationshipsForTypeViaLua(const std::string& rel_type, uint64_t skip = SKIP, uint64_t limit = LIMIT);

    };
}


#endif //RAGEDB_SHARD_H

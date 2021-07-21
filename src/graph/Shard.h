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
#include <simdjson.h>
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

        seastar::rwlock rel_type_lock;
        seastar::rwlock node_type_lock;

        NodeTypes node_types;                                                       // Store string and id of node types
        RelationshipTypes relationship_types;                                       // Store string and id of relationship types

        inline static const uint64_t SKIP = 0;
        inline static const uint64_t LIMIT = 100;

    public:
        explicit Shard(uint _cpus) : cpus(_cpus), shard_id(seastar::this_shard_id()) {}

        static seastar::future<> stop();
        void Clear();

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
        std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> NodeRemoveGetIncoming(uint64_t internal_id);
        bool NodeRemoveDeleteIncoming(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>>&grouped_relationships);
        std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> NodeRemoveGetOutgoing(uint64_t internal_id);
        bool NodeRemoveDeleteOutgoing(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>>&grouped_relationships);
        std::pair <uint16_t ,uint64_t> RelationshipRemoveGetIncoming(uint64_t internal_id);
        bool RelationshipRemoveIncoming(uint16_t rel_type_id, uint64_t external_id, uint64_t node_id);

        // Nodes
        uint64_t NodeAddEmpty(uint16_t type_id, const std::string& key);
        uint64_t NodeAdd(uint16_t type_id, const std::string& key, const std::string& properties);
        uint64_t NodeGetID(const std::string& type, const std::string& key);
        Node NodeGet(uint64_t id);
        Node NodeGet(const std::string& type, const std::string& key);
        static uint16_t NodeGetTypeId(uint64_t id);
        std::string NodeGetType(uint64_t id);
        std::string NodeGetKey(uint64_t id);
        bool NodeRemove(uint64_t id);
        bool NodeRemove(const std::string& type, const std::string& key);

        // Node Property
        std::any NodePropertyGet(uint64_t id, const std::string& property);
        bool NodePropertySetFromJson(uint64_t id, const std::string& property, const std::string& value);
        std::any NodePropertyGet(const std::string& type, const std::string& key, const std::string& property);
        bool NodePropertySetFromJson(const std::string& type, const std::string& key, const std::string& property, const std::string& value);
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

        Relationship RelationshipGet(uint64_t rel_id);
        std::string RelationshipGetType(uint64_t id);
        uint16_t RelationshipGetTypeId(uint64_t id);
        uint64_t RelationshipGetStartingNodeId(uint64_t id);
        uint64_t RelationshipGetEndingNodeId(uint64_t id);

        // Relationship Property
        std::any RelationshipPropertyGet(uint64_t id, const std::string& property);
        bool RelationshipPropertySetFromJson(uint64_t id, const std::string& property, const std::string& value);
        bool RelationshipPropertyDelete(uint64_t id, const std::string& property);

        // Relationship Properties
        std::map<std::string, std::any> RelationshipPropertiesGet(uint64_t id);
        bool RelationshipPropertiesSetFromJson(uint64_t id, const std::string& value);
        bool RelationshipPropertiesResetFromJson(uint64_t id, const std::string& value);
        bool RelationshipPropertiesDelete(uint64_t id);

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

        std::vector<Node> NodesGet(const std::vector<uint64_t>&);
        std::vector<Relationship> RelationshipsGet(const std::vector<uint64_t>&);

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
        seastar::future<uint64_t> RelationshipAddEmptyPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
        seastar::future<uint64_t> RelationshipAddEmptyPeered(const std::string& rel_type, uint64_t id1, uint64_t id2);
        seastar::future<uint64_t> RelationshipAddPeered(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                                        const std::string& type2, const std::string& key2, const std::string& properties);
        seastar::future<uint64_t> RelationshipAddPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties);
        seastar::future<uint64_t> RelationshipAddPeered(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
        seastar::future<Relationship> RelationshipGetPeered(uint64_t id);
        seastar::future<bool> RelationshipRemovePeered(uint64_t id);
        seastar::future<std::string> RelationshipGetTypePeered(uint64_t id);
        seastar::future<uint16_t> RelationshipGetTypeIdPeered(uint64_t id);
        seastar::future<uint64_t> RelationshipGetStartingNodeIdPeered(uint64_t id);
        seastar::future<uint64_t> RelationshipGetEndingNodeIdPeered(uint64_t id);

        // Relationship Properties
        seastar::future<std::any> RelationshipPropertyGetPeered(uint64_t id, const std::string& property);
        seastar::future<bool> RelationshipPropertySetFromJsonPeered(uint64_t id, const std::string& property, const std::string& value);
        seastar::future<bool> RelationshipPropertyDeletePeered(uint64_t id, const std::string& property);

        seastar::future<std::map<std::string, std::any>> RelationshipPropertiesGetPeered(uint64_t id);
        seastar::future<bool> RelationshipPropertiesSetFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> RelationshipPropertiesResetFromJsonPeered(uint64_t id, const std::string& value);
        seastar::future<bool> RelationshipPropertiesDeletePeered(uint64_t id);

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
        seastar::future<std::vector<uint64_t>> AllNodeIdsPeered(uint64_t skip = 0, uint64_t limit = 100);
        seastar::future<std::vector<uint64_t>> AllNodeIdsPeered(const std::string& type, uint64_t skip = 0, uint64_t limit = 100);
        seastar::future<std::vector<uint64_t>> AllRelationshipIdsPeered(uint64_t skip = 0, uint64_t limit = 100);
        seastar::future<std::vector<uint64_t>> AllRelationshipIdsPeered(const std::string& rel_type, uint64_t skip = 0, uint64_t limit = 100);

        seastar::future<std::vector<Node>> AllNodesPeered(uint64_t skip = 0, uint64_t limit = 100);
        seastar::future<std::vector<Node>> AllNodesPeered(const std::string& type, uint64_t skip = 0, uint64_t limit = 100);
        seastar::future<std::vector<Relationship>> AllRelationshipsPeered(uint64_t skip = 0, uint64_t limit = 100);
        seastar::future<std::vector<Relationship>> AllRelationshipsPeered(const std::string& rel_type, uint64_t skip = 0, uint64_t limit = 100);

    };
}


#endif //RAGEDB_SHARD_H

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

#include <iostream>
#include <seastar/core/when_all.hh>
#include "Shard.h"

namespace ragedb {

    static const unsigned int SHARD_BITS = 10U;
    static const unsigned int SHARD_MASK = 0x00000000000003FFU;
    static const unsigned int TYPE_BITS = 16U;
    static const unsigned int TYPE_MASK = 0x0000000003FFFFFFU;
    static const unsigned int SIXTY_FOUR = 64U;

    /**
     * Stop the Shard - Required method by Seastar for peering_sharded_service
     *
     * @return future
     */
    seastar::future<> Shard::stop() {
        std::stringstream ss;
        ss << "Stopping Shard " << seastar::this_shard_id() << '\n';
        std::cout << ss.str();
        return seastar::make_ready_future<>();
    }

    /**
     * Empty the Shard of all data
     */
    void Shard::Clear() {
        node_types.Clear();
        relationship_types.Clear();
    }

    seastar::future<std::string> Shard::HealthCheck() {
        std::stringstream message;
        message << "Shard " << seastar::this_shard_id() << " is OK";
        return seastar::make_ready_future<std::string>(message.str());
    }

    seastar::future<std::vector<std::string>> Shard::HealthCheckPeered() {
        return container().map([](Shard &local_shard) {
            return local_shard.HealthCheck();
        });
    }

    // Ids =================================================================================================================================

    // 64 bits:  10 bits for core id (1024) 16 bits for the type (65536) 38 bits for the id (274877906944)
    uint64_t Shard::internalToExternal(uint16_t type_id, uint64_t internal_id) const {
        return (((internal_id << TYPE_BITS) + type_id) << SHARD_BITS) + shard_id;
    }

    uint64_t Shard::externalToInternal(uint64_t id) {
        return (id >> (TYPE_BITS + SHARD_BITS));
    }

    uint16_t Shard::externalToTypeId(uint64_t id) {
        return (id & TYPE_MASK ) >> SHARD_BITS;
    }

    uint16_t Shard::CalculateShardId(uint64_t id) {
        if(id < SHARD_MASK) {
            return 0;
        }
        return id & SHARD_MASK;
    }

    uint16_t Shard::CalculateShardId(const std::string &type, const std::string &key) const {
        // We need to find where the node goes, so we use the type and key to create a 64 bit number
        uint64_t x64 = std::hash<std::string>()((type + '-' + key));

        // Then we bucket it into a shard depending on the number of cpus we have
        return (uint16_t)(((__uint128_t)x64 * (__uint128_t)cpus) >> SIXTY_FOUR);
    }

    bool Shard::ValidNodeId(uint64_t id) {
        // Node must be greater than zero,
        // less than maximum node id,
        // belong to this shard
        // and not deleted
        return CalculateShardId(id) == seastar::this_shard_id()
               && node_types.ValidNodeId(externalToTypeId(id), externalToInternal(id));
    }

    bool Shard::ValidRelationshipId(uint64_t id) {
        // Relationship must be greater than zero,
        // less than maximum relationship id,
        // belong to this shard
        // and not deleted
        CalculateShardId(id) == seastar::this_shard_id()
        && relationship_types.ValidRelationshipId(externalToTypeId(id), externalToInternal(id));
    }

    // *****************************************************************************************************************************
    //                                               Single Shard
    // *****************************************************************************************************************************

    // Node Types ===========================================================================================================================
    uint16_t Shard::NodeTypesGetCount() {
        return node_types.getSize();
    }

    uint64_t Shard::NodeTypesGetCount(uint16_t type_id) {
        return node_types.getCount(type_id);
    }

    uint64_t Shard::NodeTypesGetCount(const std::string &type) {
        uint16_t type_id = node_types.getTypeId(type);
        return NodeTypesGetCount(type_id);
    }

    std::set<std::string> Shard::NodeTypesGet() {
        return node_types.getTypes();
    }

    std::map<std::string, std::string> Shard::NodeTypeGet(const std::string& type) {
        uint16_t type_id = node_types.getTypeId(type);
        return node_types.getNodeTypeProperties(type_id).getPropertyTypes();
    }

    // Relationship Types ===================================================================================================================
    uint16_t Shard::RelationshipTypesGetCount() {
        return relationship_types.getSize();
    }

    uint64_t Shard::RelationshipTypesGetCount(uint16_t type_id) {
        if (relationship_types.ValidTypeId(type_id)) {
            return relationship_types.getCount(type_id);
        }
        // Not a valid Relationship Type
        return 0;
    }

    uint64_t Shard::RelationshipTypesGetCount(const std::string &type) {
        uint16_t type_id = relationship_types.getTypeId(type);
        return RelationshipTypesGetCount(type_id);
    }

    std::set<std::string> Shard::RelationshipTypesGet() {
        return relationship_types.getTypes();
    }

    std::map<std::string, std::string> Shard::RelationshipTypeGet(const std::string& type) {
        uint16_t type_id = relationship_types.getTypeId(type);
        return relationship_types.getProperties(type_id).getPropertyTypes();
    }

    // Node Type ============================================================================================================================
    std::string Shard::NodeTypeGetType(uint16_t type_id) {
        return node_types.getType(type_id);
    }

    uint16_t Shard::NodeTypeGetTypeId(const std::string &type) {
        return node_types.getTypeId(type);
    }

    bool Shard::NodeTypeInsert(const std::string& type, uint16_t type_id) {
        return node_types.addTypeId(type, type_id);
    }

    bool Shard::DeleteNodeType(const std::string& type) {
        return node_types.deleteTypeId(type);
    }

    // Relationship Type ====================================================================================================================
    std::string Shard::RelationshipTypeGetType(uint16_t type_id) {
        return relationship_types.getType(type_id);
    }

    uint16_t Shard::RelationshipTypeGetTypeId(const std::string &type) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (relationship_types.ValidTypeId(type_id)) {
            return type_id;
        }
        // Not a valid Relationship Type
        return 0;
    }

    bool Shard::RelationshipTypeInsert(const std::string& type, uint16_t type_id) {
        return relationship_types.addTypeId(type, type_id);
    }

    bool Shard::DeleteRelationshipType(const std::string& type) {
        return relationship_types.deleteTypeId(type);
    }

    // Property Types =======================================================================================================================
    uint8_t Shard::NodePropertyTypeAdd(uint16_t type_id, const std::string& key, uint8_t property_type_id) {
        return node_types.getNodeTypeProperties(type_id).setPropertyTypeId(key, property_type_id);
    }

    uint8_t Shard::RelationshipPropertyTypeAdd(uint16_t type_id, const std::string& key, uint8_t property_type_id) {
        return relationship_types.getProperties(type_id).setPropertyTypeId(key, property_type_id);
    }

    std::string Shard::NodePropertyTypeGet(const std::string& type, const std::string& key) {
       uint16_t type_id = node_types.getTypeId(type);
       return node_types.getNodeTypeProperties(type_id).getPropertyTypes()[key];
    }

    std::string Shard::RelationshipPropertyTypeGet(const std::string& type,  const std::string& key) {
        uint16_t type_id = relationship_types.getTypeId(type);
        return relationship_types.getProperties(type_id).getPropertyTypes()[key];
    }

    bool Shard::NodePropertyTypeDelete(uint16_t type_id, const std::string& key) {
        return node_types.deleteTypeProperty(type_id, key);
    }

    bool Shard::RelationshipPropertyTypeDelete(uint16_t type_id, const std::string& key) {
        return relationship_types.deleteTypeProperty(type_id, key);
    }

    // Nodes ================================================================================================================================
    uint64_t Shard::NodeAddEmpty(uint16_t type_id, const std::string &key) {
        uint64_t internal_id = node_types.getCount(type_id);
        uint64_t external_id = 0;

        // Check if the key exists
        auto key_search = node_types.getKeysToNodeId(type_id).find(key);
        if (key_search == std::end(node_types.getKeysToNodeId(type_id))) {
            // If we have deleted nodes, fill in the space by adding the new node here
            if (node_types.hasDeleted(type_id)) {
                internal_id = node_types.getDeletedIdsMinimum(type_id);
                external_id = internalToExternal(type_id, internal_id);
                // Replace the deleted node and remove it from the list
                node_types.getKeys(type_id).at(internal_id) = key;
                node_types.addId(type_id, internal_id);
            } else {
                external_id = internalToExternal(type_id, internal_id);
                // Set Metadata properties
                // Add the node to the end and prepare a place for its relationships
                node_types.getKeys(type_id).emplace_back(key);
                node_types.getOutgoingRelationships(type_id).emplace_back();
                node_types.getIncomingRelationships(type_id).emplace_back();
                node_types.addId(type_id, internal_id);
            }
            node_types.getKeysToNodeId(type_id).insert({key, external_id });
        }

        return external_id;
    }

    uint64_t Shard::NodeAdd(uint16_t type_id, const std::string &key, const std::string &properties) {
        uint64_t internal_id = node_types.getCount(type_id);
        uint64_t external_id = 0;

        // Check if the key exists
        auto key_search = node_types.getKeysToNodeId(type_id).find(key);
        if (key_search == std::end(node_types.getKeysToNodeId(type_id))) {
            // If we have deleted nodes, fill in the space by adding the new node here
            if (node_types.hasDeleted(type_id)) {
                internal_id = node_types.getDeletedIdsMinimum(type_id);
                external_id = internalToExternal(type_id, internal_id);

                // Replace the deleted node and remove it from the list
                node_types.getKeys(type_id).at(internal_id) = key;
                node_types.addId(type_id, internal_id);
                node_types.setPropertiesFromJSON(type_id, internal_id, properties);
            } else {
                external_id = internalToExternal(type_id, internal_id);
                // Add the node to the end and prepare a place for its relationships
                node_types.getKeys(type_id).emplace_back(key);
                node_types.getOutgoingRelationships(type_id).emplace_back();
                node_types.getIncomingRelationships(type_id).emplace_back();
                node_types.addId(type_id, internal_id);
                node_types.setPropertiesFromJSON(type_id, internal_id, properties);
            }
            node_types.getKeysToNodeId(type_id).insert({key, external_id });
        }

        return external_id;
    }

    uint64_t Shard::NodeGetID(const std::string &type, const std::string &key) {
        // Check if the Type exists
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id > 0) {
            return node_types.getNodeId(type_id, key);
        }
        // Invalid Type or Key
        return 0;
    }

    Node Shard::NodeGet(uint64_t id) {
        if (ValidNodeId(id)) {
            return node_types.getNode(id);
        }
        return Node();
    }

    Node Shard::NodeGet(const std::string& type, const std::string& key) {
        return NodeGet(NodeGetID(type, key));
    }

    uint16_t Shard::NodeGetTypeId(uint64_t id) {
        return externalToTypeId(id);
    }

    std::string Shard::NodeGetType(uint64_t id) {
        if (ValidNodeId(id)) {
            return node_types.getType(externalToTypeId(id));
        }
        return node_types.getType(0);
    }

    std::string Shard::NodeGetKey(uint64_t id) {
        if (ValidNodeId(id)) {
            return node_types.getKeys(externalToTypeId(id))[externalToInternal(id)];
        }
        return node_types.getType(0);
    }


    // Node Properties ======================================================================================================================
    std::any Shard::NodePropertyGet(uint64_t id, const std::string& property) {
        if (ValidNodeId(id)) {
            node_types.getNodeProperty(id, property);
        }
        return std::any();
    }

    bool Shard::NodePropertySetFromJson(uint64_t id, const std::string& property, const std::string& value) {
        if (ValidNodeId(id)) {
            return node_types.setNodeProperty(id, property, value);
        }
        return false;
    }

    std::any Shard::NodePropertyGet(const std::string& type, const std::string& key, const std::string& property) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertyGet(id, property);
    }

    bool Shard::NodePropertySetFromJson(const std::string& type, const std::string& key, const std::string& property, const std::string& value) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertySetFromJson(id, property, value);
    }

    bool Shard::NodePropertyDelete(const std::string& type, const std::string& key, const std::string& property) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertyDelete(id, property);
    }

    bool Shard::NodePropertyDelete(uint64_t id, const std::string& property) {
        if (ValidNodeId(id)) {
            return node_types.deleteNodeProperty(id, property);
        }
        return false;
    }

    std::map<std::string, std::any> Shard::NodePropertiesGet(const std::string &type, const std::string &key) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertiesGet(id);
    }

    std::map<std::string, std::any> Shard::NodePropertiesGet(uint64_t id) {
        // If the node is valid
        if (ValidNodeId(id)) {
            return node_types.getNodeProperties(externalToTypeId(id), externalToInternal(id));
        }
        return std::map<std::string, std::any>();
    }

    bool Shard::NodePropertiesSetFromJson(const std::string& type, const std::string& key, const std::string& value) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertiesSetFromJson(id, value);
    }

    bool Shard::NodePropertiesSetFromJson(uint64_t id, const std::string& value) {
        // If the node is valid
        if (ValidNodeId(id)) {
            return node_types.setPropertiesFromJSON(externalToTypeId(id), externalToInternal(id), value);
        }
        return false;
    }

    bool Shard::NodePropertiesResetFromJson(const std::string& type, const std::string& key, const std::string& value) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertiesResetFromJson(id, value);
    }

    bool Shard::NodePropertiesResetFromJson(uint64_t id, const std::string& value) {
        // If the node is valid
        if (ValidNodeId(id)) {
            node_types.deleteNodeProperties(externalToTypeId(id), externalToInternal(id));
            return node_types.setPropertiesFromJSON(externalToTypeId(id), externalToInternal(id), value);
        }
        return false;
    }

    bool Shard::NodePropertiesDelete(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertiesDelete(id);
    }

    bool Shard::NodePropertiesDelete(uint64_t id) {
        // If the node is valid
        if (ValidNodeId(id)) {
            return node_types.deleteNodeProperties(externalToTypeId(id), externalToInternal(id));
        }
    }

    // Relationships
    uint64_t Shard::RelationshipAddEmptySameShard(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
        uint64_t internal_id1 = externalToInternal(id1);
        uint64_t internal_id2 = externalToInternal(id2);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint16_t id2_type_id = externalToTypeId(id2);
        uint64_t external_id = 0;

        if (ValidNodeId(id1) && ValidNodeId(id2)) {
            uint64_t internal_id = relationship_types.getCount(rel_type_id);
            if(relationship_types.hasDeleted(rel_type_id)) {
                // If we have deleted relationships, fill in the space by reusing the new relationship
                internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
                relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
                relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
            } else {
                relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
                relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
            }
            
            external_id = internalToExternal(rel_type_id, internal_id);
            relationship_types.addId(rel_type_id, external_id);

            // Add the relationship to the outgoing node
            auto group = find_if(std::begin(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)), std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)),
                                 [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
            // See if the relationship type is already there
            if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
                group->links.emplace_back(id2, external_id);
            } else {
                // otherwise create a new type with the links
                node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(Group(rel_type_id, std::vector<Link>({Link(id2, external_id)})));
            }

            // Add the relationship to the incoming node
            group = find_if(std::begin(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)), std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)),
                            [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
            // See if the relationship type is already there
            if (group != std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2))) {
                group->links.emplace_back(id1, external_id);
            } else {
                // otherwise create a new type with the links
                node_types.getIncomingRelationships(id2_type_id).at(internal_id2).emplace_back(Group(rel_type_id, std::vector<Link>({Link(id1, external_id)})));
            }

            // Add relationship id to Types
            relationship_types.addId(rel_type_id, external_id);

            return external_id;
        }
        // Invalid node links
        return 0;
    }

    uint64_t Shard::RelationshipAddSameShard(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
        uint64_t internal_id1 = externalToInternal(id1);
        uint64_t internal_id2 = externalToInternal(id2);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint16_t id2_type_id = externalToTypeId(id2);
        uint64_t external_id = 0;

        if (ValidNodeId(id1) && ValidNodeId(id2)) {
            uint64_t internal_id = relationship_types.getCount(rel_type_id);
            if(relationship_types.hasDeleted(rel_type_id)) {
                // If we have deleted relationships, fill in the space by reusing the new relationship
                internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
                relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
                relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
            } else {
                relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
                relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
            }

            relationship_types.setPropertiesFromJSON(rel_type_id, internal_id, properties);
            external_id = internalToExternal(rel_type_id, internal_id);
            relationship_types.addId(rel_type_id, external_id);

            // Add the relationship to the outgoing node
            auto group = find_if(std::begin(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)), std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)),
                                 [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
            // See if the relationship type is already there
            if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
                group->links.emplace_back(id2, external_id);
            } else {
                // otherwise create a new type with the links
                node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(Group(rel_type_id, std::vector<Link>({Link(id2, external_id)})));
            }

            // Add the relationship to the incoming node
            group = find_if(std::begin(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)), std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)),
                            [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
            // See if the relationship type is already there
            if (group != std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2))) {
                group->links.emplace_back(id1, external_id);
            } else {
                // otherwise create a new type with the links
                node_types.getIncomingRelationships(id2_type_id).at(internal_id2).emplace_back(Group(rel_type_id, std::vector<Link>({Link(id1, external_id)})));
            }

            // Add relationship id to Types
            relationship_types.addId(rel_type_id, external_id);

            return external_id;
        }
        // Invalid node links
        return 0;
    }

    uint64_t Shard::RelationshipAddEmptySameShard(uint16_t rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2) {
        uint64_t id1 = NodeGetID(type1, key1);
        uint64_t id2 = NodeGetID(type2, key2);

        return RelationshipAddEmptySameShard(rel_type, id1, id2);
    }

    uint64_t Shard::RelationshipAddSameShard(uint16_t rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2, const std::string& properties) {
        uint64_t id1 = NodeGetID(type1, key1);
        uint64_t id2 = NodeGetID(type2, key2);

        return RelationshipAddSameShard(rel_type, id1, id2, properties);
    }

    uint64_t Shard::RelationshipAddEmptyToOutgoing(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
        uint64_t internal_id1 = externalToInternal(id1);
        uint64_t internal_id2 = externalToInternal(id2);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint16_t id2_type_id = externalToTypeId(id2);
        uint64_t external_id = 0;

        uint64_t internal_id = relationship_types.getCount(rel_type_id);
        if(relationship_types.hasDeleted(rel_type_id)) {
            // If we have deleted relationships, fill in the space by reusing the new relationship
            internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
            relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
            relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
        } else {
            relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
            relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
        }

        external_id = internalToExternal(rel_type_id, internal_id);
        relationship_types.addId(rel_type_id, external_id);

        // Add the relationship to the outgoing node
        auto group = find_if(std::begin(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)), std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)),
                             [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
        // See if the relationship type is already there
        if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
            group->links.emplace_back(id2, external_id);
        } else {
            // otherwise create a new type with the links
            node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(Group(rel_type_id, std::vector<Link>({Link(id2, external_id)})));
        }

        // Add relationship id to Types
        relationship_types.addId(rel_type_id, external_id);

        return external_id;
    }

    uint64_t Shard::RelationshipAddToOutgoing(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
        uint64_t internal_id1 = externalToInternal(id1);
        uint64_t internal_id2 = externalToInternal(id2);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint16_t id2_type_id = externalToTypeId(id2);
        uint64_t external_id = 0;

        uint64_t internal_id = relationship_types.getCount(rel_type_id);
        if(relationship_types.hasDeleted(rel_type_id)) {
            // If we have deleted relationships, fill in the space by reusing the new relationship
            internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
            relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
            relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
        } else {
            relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
            relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
        }

        relationship_types.setPropertiesFromJSON(rel_type_id, internal_id, properties);
        external_id = internalToExternal(rel_type_id, internal_id);
        relationship_types.addId(rel_type_id, external_id);

        // Add the relationship to the outgoing node
        auto group = find_if(std::begin(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)), std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)),
                             [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
        // See if the relationship type is already there
        if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
            group->links.emplace_back(id2, external_id);
        } else {
            // otherwise create a new type with the links
            node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(Group(rel_type_id, std::vector<Link>({Link(id2, external_id)})));
        }

        // Add relationship id to Types
        relationship_types.addId(rel_type_id, external_id);

        return external_id;
    }

    uint64_t Shard::RelationshipAddToIncoming(uint16_t rel_type_id, uint64_t rel_id, uint64_t id1, uint64_t id2) {
        uint64_t internal_id2 = externalToInternal(id2);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint16_t id2_type_id = externalToTypeId(id2);
        // Add the relationship to the incoming node
        auto group = find_if(std::begin(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)), std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)),
                        [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
        // See if the relationship type is already there
        if (group != std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2))) {
            group->links.emplace_back(id1, rel_id);
        } else {
            // otherwise create a new type with the links
            node_types.getIncomingRelationships(id2_type_id).at(internal_id2).emplace_back(Group(rel_type_id, std::vector<Link>({Link(id1, rel_id)})));
        }

        return rel_id;
    }

    Relationship Shard::RelationshipGet(uint64_t rel_id) {
        if (ValidRelationshipId(rel_id)) {
            return relationship_types.getRelationship(rel_id);
        }       // Invalid Relationship
        return Relationship();
    }

    std::string Shard::RelationshipGetType(uint64_t id) {
        if (ValidRelationshipId(id)) {
            return relationship_types.getType(externalToTypeId(id));
        }
        // Invalid Relationship Id
        return relationship_types.getType(0);
    }

    uint16_t Shard::RelationshipGetTypeId(uint64_t id) {
        if (ValidRelationshipId(id)) {
            return externalToTypeId(id);
        }
        // Invalid Relationship Id
        return 0;
    }

    uint64_t Shard::RelationshipGetStartingNodeId(uint64_t id) {
        if (ValidRelationshipId(id)) {
            return relationship_types.getStartingNodeId(externalToTypeId(id), externalToInternal(id));
        }
        // Invalid Relationship Id
        return 0;
    }

    uint64_t Shard::RelationshipGetEndingNodeId(uint64_t id) {
        if (ValidRelationshipId(id)) {
            return relationship_types.getEndingNodeId(externalToTypeId(id), externalToInternal(id));
        }
        // Invalid Relationship Id
        return 0;
    }

    // Traversing
    std::vector<Link> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetRelationshipsIDs(id);
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key, Direction direction) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetRelationshipsIDs(id, direction);
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetRelationshipsIDs(id, direction, rel_type);
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key, Direction direction, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetRelationshipsIDs(id, direction, type_id);
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetRelationshipsIDs(id, direction, rel_types);
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(uint64_t id) {
        if (ValidNodeId(id)) {
            uint64_t internal_id = externalToInternal(id);
            uint16_t type_id = externalToTypeId(id);
            std::vector<Link> ids;
            for (const auto &[type, list] : node_types.getOutgoingRelationships(type_id).at(internal_id)) {
                std::copy(std::begin(list), std::end(list), std::back_inserter(ids));
            }
            for (const auto &[type, list] : node_types.getIncomingRelationships(type_id).at(internal_id)) {
                std::copy(std::begin(list), std::end(list), std::back_inserter(ids));
            }
            return ids;
        }
        return std::vector<Link>();
    }

    std::vector<Node> Shard::NodesGet(const std::vector<uint64_t>& node_ids) {
        std::vector<Node> sharded_nodes;

        for(uint64_t id : node_ids) {
            sharded_nodes.push_back(node_types.getNode(id));
        }

        return sharded_nodes;
    }

    std::vector<Relationship> Shard::RelationshipsGet(const std::vector<uint64_t>& rel_ids) {
        std::vector<Relationship> sharded_relationships;

        for(uint64_t id : rel_ids) {
            sharded_relationships.push_back(relationship_types.getRelationship(id));
        }

        return sharded_relationships;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id, type_id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id, rel_types);
    }


    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }

            for (auto &types : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                for (Link link : types.links) {
                    sharded_relationships_ids.at(shard_id).emplace_back(link.rel_id);
                }
            }

            for (auto &types : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                for (Link link : types.links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if (sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id, const std::string& rel_type) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(type_id).at(internal_id))) {
                for(Link link : group->links) {
                    sharded_relationships_ids.at(shard_id).emplace_back(link.rel_id);
                }
            }

            group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                            [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if (sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id, uint16_t type_id) {
        if (ValidNodeId(id)) {
            uint64_t internal_id = externalToInternal(id);
            uint16_t node_type_id = externalToTypeId(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    sharded_relationships_ids.at(shard_id).emplace_back(link.rel_id);
                }
            }

            group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                            [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id,const std::vector<std::string> &rel_types) {
        if (ValidNodeId(id)) {
            uint64_t internal_id = externalToInternal(id);
            uint16_t node_type_id = externalToTypeId(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }

            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id > 0) {

                    auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            sharded_relationships_ids.at(shard_id).emplace_back(link.rel_id);
                        }
                    }

                    group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                    [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            uint16_t node_shard_id = CalculateShardId(link.node_id);
                            sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                        }
                    }

                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedNodeIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedNodeIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedNodeIDs(id, type_id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedNodeIDs(id, rel_types);
    }


    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id) {
        if (ValidNodeId(id)) {
            uint64_t internal_id = externalToInternal(id);
            uint16_t node_type_id = externalToTypeId(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            for (auto &types : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                for (Link link : types.links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
                }
            }

            for (auto &types : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                for (Link link : types.links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if (sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id, const std::string& rel_type) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    sharded_nodes_ids.at(shard_id).emplace_back(link.node_id);
                }
            }

            group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                            [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if (sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id, uint16_t type_id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    sharded_nodes_ids.at(shard_id).emplace_back(link.node_id);
                }
            }

            group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                            [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id,const std::vector<std::string> &rel_types) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id > 0) {
                    auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            sharded_nodes_ids.at(shard_id).emplace_back(link.node_id);
                        }
                    }

                    group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                    [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            uint16_t node_shard_id = CalculateShardId(link.node_id);
                            sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                        }
                    }
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetOutgoingRelationships(id);
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetOutgoingRelationships(id, rel_type);
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetOutgoingRelationships(id, type_id);
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetOutgoingRelationships(id, rel_types);
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id) {
        std::vector<Relationship> node_relationships;
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            for (auto &types : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                for (Link link : types.links) {
                    node_relationships.emplace_back(relationship_types.getRelationship(link.rel_id));
                }
            }
        }
        return node_relationships;
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, const std::string& rel_type) {
        std::vector<Relationship> node_relationships;
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            uint64_t internal_id = externalToInternal(id);

            auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    node_relationships.emplace_back(relationship_types.getRelationship(link.rel_id));
                }
            }

        }
        return node_relationships;
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, uint16_t type_id) {
        std::vector<Relationship> node_relationships;
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);

            auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    node_relationships.emplace_back(relationship_types.getRelationship(link.rel_id));
                }
            }

        }
        return node_relationships;
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, const std::vector<std::string> &rel_types) {
        std::vector<Relationship> node_relationships;
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id > 0) {
                    auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            node_relationships.emplace_back(relationship_types.getRelationship(link.rel_id));
                        }
                    }
                }
            }
        }
        return node_relationships;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingRelationshipIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingRelationshipIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingRelationshipIDs(id, type_id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingRelationshipIDs(id, rel_types);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }

            for (auto &types : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                for (Link link : types.links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_relationships_ids.at(node_shard_id).push_back(link.rel_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::string& rel_type) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, uint16_t type_id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::vector<std::string> &rel_types) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id > 0) {
                    auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            uint16_t node_shard_id = CalculateShardId(link.node_id);
                            sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                        }
                    }
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingNodeIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingNodeIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingNodeIDs(id, type_id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingNodeIDs(id, rel_types);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            for (auto &types : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                for (Link link : types.links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, const std::string& rel_type) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, uint16_t type_id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id > 0) {
                    auto group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            uint16_t node_shard_id = CalculateShardId(link.node_id);
                            sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                        }
                    }
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedOutgoingNodeIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedOutgoingNodeIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedOutgoingNodeIDs(id, type_id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedOutgoingNodeIDs(id, rel_types);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            for (auto &types : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                for (Link link : types.links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::string& rel_type) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    sharded_nodes_ids.at(shard_id).emplace_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, uint16_t type_id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }

            auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    sharded_nodes_ids.at(shard_id).emplace_back(link.node_id);
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
            for (int i = 0; i < cpus; i++) {
                sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
            }
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id > 0) {
                    auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            sharded_nodes_ids.at(shard_id).emplace_back(link.node_id);
                        }
                    }
                }
            }

            for (int i = 0; i < cpus; i++) {
                if(sharded_nodes_ids.at(i).empty()) {
                    sharded_nodes_ids.erase(i);
                }
            }

            return sharded_nodes_ids;
        }
        return std::map<uint16_t , std::vector<uint64_t>>();
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(uint64_t id, Direction direction) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::vector<Link> ids;
            // Use the two ifs to handle ALL for a direction
            if (direction != IN) {
                for (const auto &[type, list] : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
                    std::copy(std::begin(list), std::end(list), std::back_inserter(ids));
                }
            }
            // Use the two ifs to handle ALL for a direction
            if (direction != OUT) {
                for (const auto &[type, list] : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
                    std::copy(std::begin(list), std::end(list), std::back_inserter(ids));
                }
            }
            return ids;
        }
        return std::vector<Link>();
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(uint64_t id, Direction direction, const std::string &rel_type) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            uint64_t internal_id = externalToInternal(id);
            std::vector<Link> ids;
            uint64_t size = 0;
            // Use the two ifs to handle ALL for a direction
            auto out_group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                     [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                size += out_group->links.size();
            }

            auto in_group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                    [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                size += in_group->links.size();
            }

            ids.reserve(size);

            // Use the two ifs to handle ALL for a direction
            if (direction != IN) {
                std::copy(std::begin(out_group->links), std::end(out_group->links), std::back_inserter(ids));
            }
            if (direction != OUT) {
                std::copy(std::begin(in_group->links), std::end(in_group->links), std::back_inserter(ids));
            }
            return ids;
        }
        return std::vector<Link>();
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(uint64_t id, Direction direction, uint16_t type_id) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            if (direction == IN) {

                auto in_group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                        [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                    return in_group->links;
                }
            }

            if (direction == OUT) {
                auto out_group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                         [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                    return out_group->links;
                }
            }

            std::vector<Link> ids;
            uint64_t size = 0;
            auto out_group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                     [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                size += out_group->links.size();
            }

            auto in_group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                    [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                size += in_group->links.size();
            }

            ids.reserve(size);

            std::copy(std::begin(out_group->links), std::end(out_group->links), std::back_inserter(ids));
            std::copy(std::begin(in_group->links), std::end(in_group->links), std::back_inserter(ids));

            return ids;
        }
        return std::vector<Link>();
    }

    std::vector<Link> Shard::NodeGetRelationshipsIDs(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        if (ValidNodeId(id)) {
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::vector<Link> ids;
            // Use the two ifs to handle ALL for a direction
            if (direction != IN) {
                // For each requested type sum up the values
                for (const auto &rel_type : rel_types) {
                    uint16_t type_id = relationship_types.getTypeId(rel_type);
                    if (type_id > 0) {
                        auto out_group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                        if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                            std::copy(std::begin(out_group->links), std::end(out_group->links), std::back_inserter(ids));
                        }
                    }
                }
            }
            // Use the two ifs to handle ALL for a direction
            if (direction != OUT) {
                // For each requested type sum up the values
                for (const auto &rel_type : rel_types) {
                    uint16_t type_id = relationship_types.getTypeId(rel_type);
                    if (type_id > 0) {
                        auto in_group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
                                                [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                        if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                            std::copy(std::begin(in_group->links), std::end(in_group->links), std::back_inserter(ids));
                        }
                    }
                }
            }
            return ids;
        }
        return std::vector<Link>();
    }

    // Helpers
    std::pair <uint16_t, uint64_t> Shard::RelationshipRemoveGetIncoming(uint64_t external_id) {

        uint16_t rel_type_id = externalToTypeId(external_id);
        uint64_t internal_id = externalToInternal(external_id);

        uint64_t id1 = relationship_types.getStartingNodeId(rel_type_id, internal_id);
        uint64_t id2 = relationship_types.getEndingNodeId(rel_type_id, internal_id);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint16_t id2_type_id = externalToTypeId(id2);

        // Update the relationship type counts
        relationship_types.removeId(rel_type_id, external_id);
        uint64_t internal_id1 = externalToInternal(id1);

        // Add to deleted relationships bitmap
        relationship_types.removeId(rel_type_id, internal_id);

        // Remove relationship from Node 1
        auto group = find_if(std::begin(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)),
                             std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)),
                             [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
        if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
            auto rel_to_delete = find_if(std::begin(group->links), std::end(group->links), [external_id](Link entry) {
                return entry.rel_id == external_id;
            });
            if (rel_to_delete != std::end(group->links)) {
                group->links.erase(rel_to_delete);
            }
        }

        // Clear the relationship
        relationship_types.setStartingNodeId(rel_type_id, internal_id, 0);
        relationship_types.setEndingNodeId(rel_type_id, internal_id, 0);

        // Return the rel_type and other node Id
        return std::pair <uint16_t ,uint64_t> (rel_type_id, id2);
    }

    bool Shard::RelationshipRemoveIncoming(uint16_t rel_type_id, uint64_t external_id, uint64_t node_id) {
        // Remove relationship from Node 2
        uint64_t internal_id2 = externalToInternal(node_id);
        uint16_t id2_type_id = externalToTypeId(node_id);

        auto group = find_if(std::begin(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)),
                             std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)),
                             [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

        auto rel_to_delete = find_if(std::begin(group->links), std::end(group->links), [external_id](Link entry) {
            return entry.rel_id == external_id;
        });
        if (rel_to_delete != std::end(group->links)) {
            group->links.erase(rel_to_delete);
        }

        return true;
    }

    // Counts
    std::map<uint16_t, uint64_t> Shard::AllNodeIdCounts() {
        return node_types.getCounts();
    }

    uint64_t Shard::AllNodeIdCounts(const std::string &type) {
        uint16_t type_id = node_types.getTypeId(type);
        return AllNodeIdCounts(type_id);
    }

    uint64_t Shard::AllNodeIdCounts(uint16_t type_id) {
        return node_types.getCount(type_id);
    }

    std::vector<uint64_t> Shard::AllNodeIds(uint64_t skip, uint64_t limit ) {
        return node_types.getIds(skip, limit);
    }

    std::vector<uint64_t> Shard::AllNodeIds(const std::string& type, uint64_t skip, uint64_t limit) {
        return AllNodeIds(node_types.getTypeId(type), skip, limit);
    }

    std::vector<uint64_t> Shard::AllNodeIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
        return node_types.getIds(type_id, skip, limit);
    }

    std::vector<Node> Shard::AllNodes(uint64_t skip, uint64_t limit) {
    // TODO
    }

    std::vector<Node> Shard::AllNodes(const std::string& type, uint64_t skip, uint64_t limit) {
        uint16_t type_id = node_types.getTypeId(type);
        return AllNodes(type_id, skip, limit);
    }

    std::vector<Node> Shard::AllNodes(uint16_t type_id, uint64_t skip, uint64_t limit) {
    // TODO
    }

    std::vector<uint64_t> Shard::AllRelationshipIds(uint64_t skip, uint64_t limit) {
        return relationship_types.getIds(skip, limit);
    }

    std::vector<uint64_t> Shard::AllRelationshipIds(const std::string& rel_type, uint64_t skip, uint64_t limit) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        return AllRelationshipIds(type_id, skip, limit);
    }

    std::vector<uint64_t> Shard::AllRelationshipIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
        return relationship_types.getIds(type_id, skip, limit);
    }

    std::vector<Relationship> Shard::AllRelationships(uint64_t skip, uint64_t limit) {
        return relationship_types.getRelationships(skip, limit);
    }

    std::vector<Relationship> Shard::AllRelationships(const std::string& rel_type, uint64_t skip, uint64_t limit) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        return AllRelationships(type_id, skip, limit);
    }

    std::vector<Relationship> Shard::AllRelationships(uint16_t type_id, uint64_t skip, uint64_t limit) {
        return relationship_types.getRelationships(type_id, skip, limit);
    }

    std::map<uint16_t, uint64_t> Shard::AllRelationshipIdCounts() {
        return relationship_types.getCounts();
    }

    uint64_t Shard::AllRelationshipIdCounts(const std::string &type) {
        uint16_t type_id = relationship_types.getTypeId(type);
        return AllRelationshipIdCounts(type_id);
    }

    uint64_t Shard::AllRelationshipIdCounts(uint16_t type_id) {
        return relationship_types.getCount(type_id);
    }


    // *****************************************************************************************************************************
    //                                               Peered
    // *****************************************************************************************************************************

    // Node Type ===========================================================================================================================
    std::string Shard::NodeTypeGetTypePeered(uint16_t type_id) {
        return node_types.getType(type_id);
    }

    uint16_t Shard::NodeTypeGetTypeIdPeered(const std::string &type) {
        return node_types.getTypeId(type);
    }

    seastar::future<uint16_t> Shard::NodeTypeInsertPeered(const std::string &type) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) {
            // type_id is global so unfortunately we need to lock here
            this->node_type_lock.for_write().lock().get();
            // The node type was not found and must therefore be new, add it to all shards.
            type_id = node_types.insertOrGetTypeId(type);
            return container().invoke_on_all([type, type_id](Shard &local_shard) {
                        local_shard.NodeTypeInsert(type, type_id);
                    })
                    .then([type_id, this] {
                        this->node_type_lock.for_write().unlock();
                        return seastar::make_ready_future<uint16_t>(type_id);
                    });
        }

        return seastar::make_ready_future<uint16_t>(type_id);
    }
    seastar::future<bool> Shard::DeleteNodeTypePeered(const std::string& type) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) {
            // type_id is global so unfortunately we need to lock here
            this->node_type_lock.for_write().lock().get();
            // The type was found and must therefore be deleted on all shards.
            return container().invoke_on_all([type](Shard &local_shard) {
                        local_shard.DeleteNodeType(type);
                    })
                    .then([type_id, this] {
                        this->node_type_lock.for_write().unlock();
                        return seastar::make_ready_future<bool>(true);
                    });
        }

        return seastar::make_ready_future<bool>(false);
    }

    // Relationship Type ====================================================================================================================
    std::string Shard::RelationshipTypeGetTypePeered(uint16_t type_id) {
        return relationship_types.getType(type_id);
    }

    uint16_t Shard::RelationshipTypeGetTypeIdPeered(const std::string &type) {
        return relationship_types.getTypeId(type);
    }

    seastar::future<uint16_t> Shard::RelationshipTypeInsertPeered(const std::string &rel_type) {
        // rel_type_id is global, so we need to calculate it here
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
        if (rel_type_id == 0) {
            // rel_type_id is global so unfortunately we need to lock here
            this->rel_type_lock.for_write().lock().get();

            // The relationship type was not found and must therefore be new, add it to all shards.
            rel_type_id = relationship_types.insertOrGetTypeId(rel_type);
            this->rel_type_lock.for_write().unlock();
            return container().invoke_on_all([rel_type, rel_type_id](Shard &local_shard) {
                        local_shard.RelationshipTypeInsert(rel_type, rel_type_id);
                    })
                    .then([rel_type_id] {
                        return seastar::make_ready_future<uint16_t>(rel_type_id);
                    });
        }

        return seastar::make_ready_future<uint16_t>(rel_type_id);
    }

    seastar::future<bool> Shard::DeleteRelationshipTypePeered(const std::string& type) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) {
            // type_id is global so unfortunately we need to lock here
            this->rel_type_lock.for_write().lock().get();
            // The type was found and must therefore be deleted on all shards.
            return container().invoke_on_all([type](Shard &local_shard) {
                        local_shard.DeleteRelationshipType(type);
                    })
                    .then([type_id, this] {
                        this->rel_type_lock.for_write().unlock();
                        return seastar::make_ready_future<bool>(true);
                    });
        }

        return seastar::make_ready_future<bool>(false);
    }

    // Nodes ===============================================================================================================================

    seastar::future<uint64_t> Shard::NodeAddEmptyPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);
        uint16_t type_id = node_types.getTypeId(type);

        // The node type exists, so continue on
        if (type_id > 0) {
            return container().invoke_on(node_shard_id, [type_id, key](Shard &local_shard) {
                return local_shard.NodeAddEmpty(type_id, key);
            });
        }

        // The node type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [node_shard_id, type, key, this] (Shard &local_shard) {
            return local_shard.NodeTypeInsertPeered(type).then([node_shard_id, type, key, this] (uint16_t node_type_id) {
                return container().invoke_on(node_shard_id, [node_type_id, key](Shard &local_shard) {
                    return local_shard.NodeAddEmpty(node_type_id, key);
                });
            });
        });
    }

    seastar::future<uint64_t> Shard::NodeAddPeered(const std::string &type, const std::string &key, const std::string &properties) {
        uint16_t node_shard_id = CalculateShardId(type, key);
        uint16_t node_type_id = node_types.getTypeId(type);

        // The node type exists, so continue on
        if (node_type_id > 0) {
            return container().invoke_on(node_shard_id, [node_type_id, key, properties](Shard &local_shard) {
                return local_shard.NodeAdd(node_type_id, key, properties);
            });
        }

        // The node type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [node_shard_id, type, key, properties, this](Shard &local_shard) {
            return local_shard.NodeTypeInsertPeered(type).then([node_shard_id, key, properties, this](uint16_t node_type_id) {
                return container().invoke_on(node_shard_id, [node_type_id, key, properties](Shard &local_shard) {
                    return local_shard.NodeAdd(node_type_id, key, properties);
                });
            });
        });
    }

    seastar::future<uint64_t> Shard::NodeGetIDPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key] (Shard &local_shard) {
           return local_shard.NodeGetID(type, key);
        });
    }

    seastar::future<Node> Shard::NodeGetPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodeGet(type, key);
        });
    }

    seastar::future<Node> Shard::NodeGetPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGet(id);
        });
    }

    seastar::future<bool> Shard::NodeRemovePeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key] (Shard &local_shard) {
            return local_shard.NodeGetID(type, key);
        }).then([this] (uint64_t external_id) {
            return NodeRemovePeered(external_id);
        });
    }

    seastar::future<bool> Shard::NodeRemovePeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);
        //TODO
        return seastar::make_ready_future<bool>(false);
    }

    seastar::future<uint16_t> Shard::NodeGetTypeIdPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGetTypeId(id);
        });
    }

    seastar::future<std::string> Shard::NodeGetTypePeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGetType(id);
        });
    }

    seastar::future<std::string> Shard::NodeGetKeyPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGetKey(id);
        });
    }

    // Property Types ======================================================================================================================

    seastar::future<uint8_t> Shard::NodePropertyTypeInsertPeered(uint16_t type_id, const std::string &key, const std::string &type) {
        node_types.getNodeTypeProperties(type_id).property_type_lock.for_write().lock().get();

        uint8_t property_type_id = node_types.getNodeTypeProperties(type_id).setPropertyType(key, type);

        return container().invoke_on_all([type_id, key, property_type_id](Shard &all_shards) {
            all_shards.NodePropertyTypeAdd(type_id, key, property_type_id);
        }).then([type_id, property_type_id, this] {
            this->node_types.getNodeTypeProperties(type_id).property_type_lock.for_write().unlock();
            return seastar::make_ready_future<uint8_t>(property_type_id);
        });
    }

    seastar::future<uint8_t> Shard::RelationshipPropertyTypeInsertPeered(uint16_t type_id, const std::string &key, const std::string &type) {
        relationship_types.getProperties(type_id).property_type_lock.for_write().lock().get();

        uint8_t property_type_id = relationship_types.getProperties(type_id).setPropertyType(key, type);

        return container().invoke_on_all([type_id, key, property_type_id](Shard &all_shards) {
            all_shards.RelationshipPropertyTypeAdd(type_id, key, property_type_id);
        }).then([type_id, property_type_id, this] {
            this->relationship_types.getProperties(type_id).property_type_lock.for_write().unlock();
            return seastar::make_ready_future<uint8_t>(property_type_id);
        });
    }

    seastar::future<uint8_t> Shard::NodePropertyTypeAddPeered(const std::string& node_type, const std::string& key, const std::string& type) {
        uint16_t node_type_id = node_types.getTypeId(type);
        if (node_type_id == 0) {
            return container().invoke_on(0, [node_type, key, type, this] (Shard &local_shard) {
                return local_shard.NodeTypeInsertPeered(node_type).then([node_type, key, type, this](uint16_t node_type_id) {
                    return container().invoke_on(0, [node_type_id, key, type] (Shard &local_shard) {
                        return local_shard.NodePropertyTypeInsertPeered(node_type_id, key, type);
                    });
                });
            });
        }

        return container().invoke_on(0, [node_type_id, key, type] (Shard &local_shard) {
            return local_shard.NodePropertyTypeInsertPeered(node_type_id, key, type);
        });
    }

    seastar::future<uint8_t> Shard::RelationshipPropertyTypeAddPeered(const std::string& node_type, const std::string& key, const std::string& type) {
        uint16_t relationship_type_id = relationship_types.getTypeId(type);
        if (relationship_type_id == 0) {
            return container().invoke_on(0, [node_type, key, type, this] (Shard &local_shard) {
                return local_shard.RelationshipTypeInsertPeered(node_type).then([node_type, key, type, this](uint16_t node_type_id) {
                    return container().invoke_on(0, [node_type_id, key, type] (Shard &local_shard) {
                        return local_shard.RelationshipPropertyTypeInsertPeered(node_type_id, key, type);
                    });
                });
            });
        }

        return container().invoke_on(0, [relationship_type_id, key, type] (Shard &local_shard) {
            return local_shard.RelationshipPropertyTypeInsertPeered(relationship_type_id, key, type);
        });
    }

    seastar::future<bool> Shard::NodePropertyTypeDeletePeered(const std::string& type, const std::string& key) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, key] (Shard &local_shard) {
            return local_shard.NodePropertyTypeDelete(type_id, key);
        }).then([](const std::vector<bool>& deletes) {
            bool successful = true;
            for (auto && i : deletes) {
                successful = successful && i;
            }
            return seastar::make_ready_future<bool>(successful);
        });
    }

    seastar::future<bool> Shard::RelationshipPropertyTypeDeletePeered(const std::string& type, const std::string& key) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, key] (Shard &local_shard) {
            return local_shard.RelationshipPropertyTypeDelete(type_id, key);
        }).then([](const std::vector<bool>& deletes) {
            bool successful = true;
            for (auto && i : deletes) {
                successful = successful && i;
            }
            return seastar::make_ready_future<bool>(successful);
        });
    }

    // Node Properties ==========================================================================================================================
    seastar::future<std::any> Shard::NodePropertyGetPeered(const std::string &type, const std::string &key, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
            return local_shard.NodePropertyGet(type, key, property);
        });
    }

    seastar::future<std::any> Shard::NodePropertyGetPeered(uint64_t id, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
            return local_shard.NodePropertyGet(id, property);
        });
    }

    seastar::future<bool> Shard::NodePropertySetFromJsonPeered(const std::string &type, const std::string &key, const std::string &property, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
            return local_shard.NodePropertySetFromJson(type, key, property, value);
        });
    }

    seastar::future<bool> Shard::NodePropertySetFromJsonPeered(uint64_t id, const std::string &property, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
            return local_shard.NodePropertySetFromJson(id, property, value);
        });
    }

    seastar::future<bool> Shard::NodePropertyDeletePeered(const std::string &type, const std::string &key, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
            return local_shard.NodePropertyDelete(type, key, property);
        });
    }

    seastar::future<bool> Shard::NodePropertyDeletePeered(uint64_t id, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
            return local_shard.NodePropertyDelete(id, property);
        });
    }
    seastar::future<std::map<std::string, std::any>> Shard::NodePropertiesGetPeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodePropertiesGet(type, key);
        });
    }

    seastar::future<std::map<std::string, std::any>> Shard::NodePropertiesGetPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        if(node_shard_id == seastar::this_shard_id()) {
            return seastar::make_ready_future<std::map<std::string, std::any>>(NodePropertiesGet(id));
        }

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodePropertiesGet(id);
        });
    }

    seastar::future<bool> Shard::NodePropertiesSetFromJsonPeered(const std::string &type, const std::string &key, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) {
            return local_shard.NodePropertiesSetFromJson(type, key, value);
        });
    }

    seastar::future<bool> Shard::NodePropertiesSetFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        if(node_shard_id == seastar::this_shard_id()) {
            return seastar::make_ready_future<bool>(NodePropertiesSetFromJson(id, value));
        }

        return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
            return local_shard.NodePropertiesSetFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::NodePropertiesResetFromJsonPeered(const std::string &type, const std::string &key, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) {
            return local_shard.NodePropertiesResetFromJson(type, key, value);
        });
    }

    seastar::future<bool> Shard::NodePropertiesResetFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
            return local_shard.NodePropertiesResetFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::NodePropertiesDeletePeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodePropertiesDelete(type, key);
        });
    }

    seastar::future<bool> Shard::NodePropertiesDeletePeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodePropertiesDelete(id);
        });
    }

    // Relationships ==========================================================================================================================
    seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(const std::string &rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2) {
        uint16_t shard_id1 = CalculateShardId(type1, key1);
        uint16_t shard_id2 = CalculateShardId(type2, key2);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

        // The rel type exists, continue on
        if (rel_type_id > 0) {
            if(shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2](Shard &local_shard) {
                    return local_shard.RelationshipAddEmptySameShard(rel_type_id, type1, key1, type2, key2);
                });
            }

            // Get node id1, get node id 2 and call add Empty with links
            seastar::future<uint64_t> getId1 = container().invoke_on(shard_id1, [type1, key1](Shard &local_shard) {
                return local_shard.NodeGetID(type1, key1);
            });

            seastar::future<uint64_t> getId2 = container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                return local_shard.NodeGetID(type2, key2);
            });

            std::vector<seastar::future<uint64_t>> futures;
            futures.push_back(std::move(getId1));
            futures.push_back(std::move(getId2));
            auto p = make_shared(std::move(futures));

            return seastar::when_all_succeed(p->begin(), p->end())
                    .then([rel_type_id, this] (const std::vector<uint64_t>& ids) {
                        if(ids.at(0) > 0 && ids.at(1) > 0) {
                            return RelationshipAddEmptyPeered(rel_type_id, ids.at(0), ids.at(1));
                        }
                        // Invalid node links
                        return seastar::make_ready_future<uint64_t>(uint64_t (0));
                    });
        }

        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, type1, key1, type2, key2, this] (Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, rel_type, type1, key1, type2, key2, this] (uint16_t rel_type_id) {
                        if(shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2](Shard &local_shard) {
                                return local_shard.RelationshipAddEmptySameShard(rel_type_id, type1, key1, type2, key2);
                            });
                        }

                        // Get node id1, get node id 2 and call add Empty with links
                        seastar::future<uint64_t> getId1 = container().invoke_on(shard_id1, [type1, key1](Shard &local_shard) {
                            return local_shard.NodeGetID(type1, key1);
                        });

                        seastar::future<uint64_t> getId2 = container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                            return local_shard.NodeGetID(type2, key2);
                        });

                        std::vector<seastar::future<uint64_t>> futures;
                        futures.push_back(std::move(getId1));
                        futures.push_back(std::move(getId2));
                        auto p = make_shared(std::move(futures));

                        return seastar::when_all_succeed(p->begin(), p->end())
                                .then([rel_type_id, this] (const std::vector<uint64_t>& ids) {
                                    if(ids.at(0) > 0 && ids.at(1) > 0) {
                                        return RelationshipAddEmptyPeered(rel_type_id, ids.at(0), ids.at(1));
                                    }
                                    // Invalid node links
                                    return seastar::make_ready_future<uint64_t>(uint64_t (0));
                                });

                    });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddPeered(const std::string &rel_type, const std::string &type1, const std::string &key1,
                                                           const std::string &type2, const std::string &key2, const std::string& properties) {
        uint16_t shard_id1 = CalculateShardId(type1, key1);
        uint16_t shard_id2 = CalculateShardId(type2, key2);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

        // The rel type exists, continue on
        if (rel_type_id > 0) {
            if(shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2, properties](Shard &local_shard) {
                    return local_shard.RelationshipAddSameShard(rel_type_id, type1, key1, type2, key2, properties);
                });
            }

            // Get node id1, get node id 2 and call add Empty with links
            seastar::future<uint64_t> getId1 = container().invoke_on(shard_id1, [type1, key1](Shard &local_shard) {
                return local_shard.NodeGetID(type1, key1);
            });

            seastar::future<uint64_t> getId2 = container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                return local_shard.NodeGetID(type2, key2);
            });

            std::vector<seastar::future<uint64_t>> futures;
            futures.push_back(std::move(getId1));
            futures.push_back(std::move(getId2));
            auto p = make_shared(std::move(futures));

            return seastar::when_all_succeed(p->begin(), p->end())
                    .then([rel_type_id, properties, this] (const std::vector<uint64_t> ids) {
                        if(ids.at(0) > 0 && ids.at(1) > 0) {
                            return RelationshipAddPeered(rel_type_id, ids.at(0), ids.at(1), properties);
                        }
                        // Invalid node links
                        return seastar::make_ready_future<uint64_t>(uint64_t (0));
                    });
        }

        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, type1, key1, type2, key2, properties, this] (Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, rel_type, type1, key1, type2, key2, properties, this] (uint16_t rel_type_id) {
                        if(shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2, properties](Shard &local_shard) {
                                return local_shard.RelationshipAddSameShard(rel_type_id, type1, key1, type2, key2, properties);
                            });
                        }

                        // Get node id1, get node id 2 and call add Empty with links
                        seastar::future<uint64_t> getId1 = container().invoke_on(shard_id1, [type1, key1](Shard &local_shard) {
                            return local_shard.NodeGetID(type1, key1);
                        });

                        seastar::future<uint64_t> getId2 = container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                            return local_shard.NodeGetID(type2, key2);
                        });

                        std::vector<seastar::future<uint64_t>> futures;
                        futures.push_back(std::move(getId1));
                        futures.push_back(std::move(getId2));
                        auto p = make_shared(std::move(futures));

                        return seastar::when_all_succeed(p->begin(), p->end())
                                .then([rel_type_id, properties, this] (const std::vector<uint64_t>& ids) {
                                    if(ids.at(0) > 0 && ids.at(1) > 0) {
                                        return RelationshipAddPeered(rel_type_id, ids.at(0), ids.at(1), properties);
                                    }
                                    // Invalid node links
                                    return seastar::make_ready_future<uint64_t>(uint64_t (0));
                                });

                    });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(const std::string &rel_type, uint64_t id1, uint64_t id2) {
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
        // The rel type exists, continue on
        if (rel_type_id > 0) {
            if (shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard) {
                    return local_shard.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
                });
            }

            return RelationshipAddEmptyPeered(rel_type_id, id1, id2);
        }
        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, id1, id2, this](Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, rel_type, id1, id2, this](uint16_t rel_type_id) {
                        if (shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard) {
                                return local_shard.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
                            });
                        }

                        // Get node id1, get node id 2 and call add Empty with links
                        seastar::future<bool> validateId1 = container().invoke_on(shard_id1, [id1](Shard &local_shard) {
                            return local_shard.ValidNodeId(id1);
                        });

                        seastar::future<bool> validateId2 = container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                            return local_shard.ValidNodeId(id2);
                        });

                        std::vector<seastar::future<bool>> futures;
                        futures.push_back(std::move(validateId1));
                        futures.push_back(std::move(validateId2));
                        auto p = make_shared(std::move(futures));

                        // if they are valid, call outgoing on shard 1, get the rel_id and use it to call incoming on shard 2
                        return seastar::when_all_succeed(p->begin(), p->end())
                                .then([rel_type_id, shard_id1, shard_id2, id1, id2, this](const std::vector<bool>& valid) {
                                    if (valid.at(0) && valid.at(1)) {
                                        return container().invoke_on(shard_id1, [rel_type_id, shard_id2, id1, id2, this](Shard &local_shard) {
                                            return seastar::make_ready_future<uint64_t>(local_shard.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2))
                                                    .then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                                        return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                                            return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                                        });
                                                    });
                                        });
                                    }
                                    // Invalid node links
                                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                });
                    });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddPeered(const std::string &rel_type, uint64_t id1, uint64_t id2, const std::string& properties) {
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);

        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
        // The rel type exists, continue on
        if (rel_type_id > 0) {
            if (shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard) {
                    return local_shard.RelationshipAddSameShard(rel_type_id, id1, id2, properties);
                });
            }

            return RelationshipAddPeered(rel_type_id, id1, id2, properties);
        }

        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, id1, id2, properties, this](Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, rel_type, id1, id2, properties, this](uint16_t rel_type_id) {
                        if (shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard) {
                                return local_shard.RelationshipAddSameShard(rel_type_id, id1, id2, properties);
                            });
                        }

                        // Get node id1, get node id 2 and call add Empty with links
                        seastar::future<bool> validateId1 = container().invoke_on(shard_id1, [id1](Shard &local_shard) {
                            return local_shard.ValidNodeId(id1);
                        });

                        seastar::future<bool> validateId2 = container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                            return local_shard.ValidNodeId(id2);
                        });

                        std::vector<seastar::future<bool>> futures;
                        futures.push_back(std::move(validateId1));
                        futures.push_back(std::move(validateId2));
                        auto p = make_shared(std::move(futures));

                        // if they are valid, call outgoing on shard 1, get the rel_id and use it to call incoming on shard 2
                        return seastar::when_all_succeed(p->begin(), p->end())
                                .then([rel_type_id, shard_id1, shard_id2, id1, id2, properties, this](const std::vector<bool>& valid) {
                                    if (valid.at(0) && valid.at(1)) {
                                        return container().invoke_on(shard_id1, [rel_type_id, shard_id2, id1, id2, properties, this](Shard &local_shard) {
                                            return seastar::make_ready_future<uint64_t>(local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties))
                                                    .then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                                        return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                                            return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                                        });
                                                    });
                                        });
                                    }
                                    // Invalid node links
                                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                });
                    });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);

        if (relationship_types.ValidTypeId(rel_type_id)) {
            // Get node id1, get node id 2 and call add Empty with links
            seastar::future<bool> validateId1 = container().invoke_on(shard_id1, [id1] (Shard &local_shard) {
                return local_shard.ValidNodeId(id1);
            });

            seastar::future<bool> validateId2 = container().invoke_on(shard_id2, [id2] (Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            });

            std::vector<seastar::future<bool>> futures;
            futures.push_back(std::move(validateId1));
            futures.push_back(std::move(validateId2));
            auto p = make_shared(std::move(futures));

            // if they are valid, call outgoing on shard 1, get the rel_id and use it to call incoming on shard 2
            return seastar::when_all_succeed(p->begin(), p->end())
                    .then([rel_type_id, shard_id1, shard_id2, id1, id2, this] (const std::vector<bool>& valid) {
                        if(valid.at(0) && valid.at(1)) {
                            return container().invoke_on(shard_id1, [rel_type_id, shard_id2, id1, id2, this] (Shard &local_shard) {
                                return seastar::make_ready_future<uint64_t>(local_shard.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2))
                                        .then([rel_type_id, shard_id2, id1, id2, this] (uint64_t rel_id) {
                                            return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id] (Shard &local_shard) {
                                                return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                            });
                                        });
                            });
                        }
                        // Invalid node links
                        return seastar::make_ready_future<uint64_t>(uint64_t (0));
                    });
        }

        // Invalid rel type id
        return seastar::make_ready_future<uint64_t>(uint64_t (0));
    }

    seastar::future<uint64_t> Shard::RelationshipAddPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);
        if (relationship_types.ValidTypeId(rel_type_id)) {
            // Get node id1, get node id 2 and call add with links
            seastar::future<bool> validateId1 = container().invoke_on(shard_id1, [id1](Shard &local_shard) {
                return local_shard.ValidNodeId(id1);
            });

            seastar::future<bool> validateId2 = container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            });

            std::vector<seastar::future<bool>> futures;
            futures.push_back(std::move(validateId1));
            futures.push_back(std::move(validateId2));
            auto p = make_shared(std::move(futures));

            // if they are valid, call outgoing on shard 1, get the rel_id and use it to call incoming on shard 2
            return seastar::when_all_succeed(p->begin(), p->end())
                    .then([rel_type_id, shard_id1, shard_id2, properties, id1, id2, this](const std::vector<bool> valid) {
                        if (valid.at(0) && valid.at(1)) {
                            return container().invoke_on(shard_id1, [rel_type_id, shard_id2, id1, id2, properties, this](Shard &local_shard) {
                                return seastar::make_ready_future<uint64_t>(local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties))
                                        .then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                            return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                                return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                            });
                                        });
                            });
                        }
                        // Invalid node links
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    });
        }

        // Invalid rel type id
        return seastar::make_ready_future<uint64_t>(uint64_t(0));
    }

    seastar::future<Relationship> Shard::RelationshipGetPeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
            return local_shard.RelationshipGet(id);
        });
    }

    seastar::future<bool> Shard::RelationshipRemovePeered(uint64_t external_id) {
        uint16_t rel_shard_id = CalculateShardId(external_id);

        return container().invoke_on(rel_shard_id, [external_id] (Shard &local_shard) {
            return local_shard.ValidRelationshipId(external_id);
        }).then([rel_shard_id, external_id, this] (bool valid) {
            if(valid) {
                return container().invoke_on(rel_shard_id, [external_id] (Shard &local_shard) {
                    return local_shard.RelationshipRemoveGetIncoming(external_id);
                }).then([external_id, this] (std::pair <uint16_t, uint64_t> rel_type_incoming_node_id) {

                    uint16_t shard_id2 = CalculateShardId(rel_type_incoming_node_id.second);
                    return container().invoke_on(shard_id2, [rel_type_incoming_node_id, external_id] (Shard &local_shard) {
                        return local_shard.RelationshipRemoveIncoming(rel_type_incoming_node_id.first, external_id, rel_type_incoming_node_id.second);
                    });
                });
            }
            return seastar::make_ready_future<bool>(false);
        });
    }

    seastar::future<std::string> Shard::RelationshipGetTypePeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
            return local_shard.RelationshipGetType(id);
        });
    }

    seastar::future<uint16_t> Shard::RelationshipGetTypeIdPeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
            return local_shard.RelationshipGetTypeId(id);
        });
    }

    seastar::future<uint64_t> Shard::RelationshipGetStartingNodeIdPeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
            return local_shard.RelationshipGetStartingNodeId(id);
        });
    }

    seastar::future<uint64_t> Shard::RelationshipGetEndingNodeIdPeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
            return local_shard.RelationshipGetEndingNodeId(id);
        });
    }

    // Traversing
    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(type, key);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(type, key, direction);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, rel_type](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(type, key, direction, rel_type);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, Direction direction, uint16_t type_id) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, type_id](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(type, key, direction, type_id);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, rel_types](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(type, key, direction, rel_types);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, rel_type](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(type, key, BOTH, rel_type);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, uint16_t type_id) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, type_id](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(type, key, BOTH, type_id);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(type, key, BOTH, rel_types);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(external_id);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(external_id, direction);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, Direction direction, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction, rel_type](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(external_id, direction, rel_type);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, Direction direction, uint16_t type_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction, type_id](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(external_id, direction, type_id);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction, rel_types](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(external_id, direction, rel_types);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, rel_type](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(external_id, BOTH, rel_type);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, uint16_t type_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, type_id](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(external_id, BOTH, type_id);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
            return local_shard.NodeGetRelationshipsIDs(external_id, BOTH, rel_types);
        });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                    return local_shard.NodeGetShardedRelationshipIDs(type, key); })
                .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                    std::vector<seastar::future<std::vector<Relationship>>> futures;
                    for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                        auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                            return local_shard.RelationshipsGet(grouped_rel_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                        std::vector<Relationship> combined;

                        for(const std::vector<Relationship>& sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

        if (rel_type_id > 0) {
            return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(type, key, rel_type_id); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                        std::vector<seastar::future<std::vector<Relationship>>> futures;
                        for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                return local_shard.RelationshipsGet(grouped_rel_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                            std::vector<Relationship> combined;

                            for (const std::vector<Relationship> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, uint16_t rel_type_id) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        if (rel_type_id > 0) {
            return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(type, key, rel_type_id); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                        std::vector<seastar::future<std::vector<Relationship>>> futures;
                        for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                return local_shard.RelationshipsGet(grouped_rel_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                            std::vector<Relationship> combined;

                            for (const std::vector<Relationship> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(type, key, rel_types); })
                .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                    std::vector<seastar::future<std::vector<Relationship>>> futures;
                    for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                        auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                            return local_shard.RelationshipsGet(grouped_rel_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                        std::vector<Relationship> combined;

                        for (const std::vector<Relationship> &sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                    return local_shard.NodeGetShardedRelationshipIDs(external_id); })
                .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                    std::vector<seastar::future<std::vector<Relationship>>> futures;
                    for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                        auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                            return local_shard.RelationshipsGet(grouped_rel_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                        std::vector<Relationship> combined;

                        for(const std::vector<Relationship>& sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

        if (rel_type_id > 0) {
            return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(external_id, rel_type_id); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                        std::vector<seastar::future<std::vector<Relationship>>> futures;
                        for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                return local_shard.RelationshipsGet(grouped_rel_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                            std::vector<Relationship> combined;

                            for (const std::vector<Relationship> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id,  uint16_t rel_type_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        if (rel_type_id > 0) {
            return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(external_id, rel_type_id); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                        std::vector<seastar::future<std::vector<Relationship>>> futures;
                        for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                return local_shard.RelationshipsGet(grouped_rel_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                            std::vector<Relationship> combined;

                            for (const std::vector<Relationship> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(external_id, rel_types); })
                .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                    std::vector<seastar::future<std::vector<Relationship>>> futures;
                    for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                        auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                            return local_shard.RelationshipsGet(grouped_rel_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                        std::vector<Relationship> combined;

                        for (const std::vector<Relationship> &sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        switch(direction) {
            case OUT: {
                return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                    return local_shard.NodeGetOutgoingRelationships(type, key); });
            }
            case IN: {
                return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                            std::vector<seastar::future<std::vector<Relationship>>> futures;
                            for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                                    return local_shard.RelationshipsGet(grouped_rel_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                                std::vector<Relationship> combined;

                                for(const std::vector<Relationship>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetRelationshipsPeered(type, key);
        }
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
        if (rel_type_id != 0) {
            switch (direction) {
                case OUT: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(type, key, rel_type_id); });
                }
                case IN: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                                std::vector<seastar::future<std::vector<Relationship>>> futures;
                                for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                        return local_shard.RelationshipsGet(grouped_rel_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>>& results) {
                                    std::vector<Relationship> combined;

                                    for (const std::vector<Relationship>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetRelationshipsPeered(type, key, rel_type_id);
            }
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t rel_type_id) {
        uint16_t node_shard_id = CalculateShardId(type, key);
        if (rel_type_id != 0) {
            switch (direction) {
                case OUT: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(type, key, rel_type_id); });
                }
                case IN: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                                std::vector<seastar::future<std::vector<Relationship>>> futures;
                                for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                        return local_shard.RelationshipsGet(grouped_rel_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>>& results) {
                                    std::vector<Relationship> combined;

                                    for (const std::vector<Relationship>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetRelationshipsPeered(type, key, rel_type_id);
            }
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        switch(direction) {
            case OUT: {
                return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
                    return local_shard.NodeGetOutgoingRelationships(type, key, rel_types); });
            }
            case IN: {
                return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key, rel_types); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                            std::vector<seastar::future<std::vector<Relationship>>> futures;
                            for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                                    return local_shard.RelationshipsGet(grouped_rel_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                                std::vector<Relationship> combined;

                                for(const std::vector<Relationship>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetRelationshipsPeered(type, key, rel_types);
        }
    }


    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        switch(direction) {
            case OUT: {
                return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                    return local_shard.NodeGetOutgoingRelationships(external_id); });
            }
            case IN: {
                return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                            std::vector<seastar::future<std::vector<Relationship>>> futures;
                            for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                                    return local_shard.RelationshipsGet(grouped_rel_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                                std::vector<Relationship> combined;

                                for(const std::vector<Relationship>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetRelationshipsPeered(external_id);
        }
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
        if (rel_type_id != 0) {
            switch (direction) {
                case OUT: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(external_id, rel_type_id); });
                }
                case IN: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                                std::vector<seastar::future<std::vector<Relationship>>> futures;
                                for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                        return local_shard.RelationshipsGet(grouped_rel_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>>& results) {
                                    std::vector<Relationship> combined;

                                    for (const std::vector<Relationship>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetRelationshipsPeered(external_id, rel_type_id);
            }
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction, uint16_t rel_type_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);
        if (rel_type_id != 0) {
            switch (direction) {
                case OUT: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(external_id, rel_type_id); });
                }
                case IN: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                                std::vector<seastar::future<std::vector<Relationship>>> futures;
                                for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                        return local_shard.RelationshipsGet(grouped_rel_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>>& results) {
                                    std::vector<Relationship> combined;

                                    for (const std::vector<Relationship>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetRelationshipsPeered(external_id, rel_type_id);
            }
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        switch(direction) {
            case OUT: {
                return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
                    return local_shard.NodeGetOutgoingRelationships(external_id, rel_types); });
            }
            case IN: {
                return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id, rel_types); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                            std::vector<seastar::future<std::vector<Relationship>>> futures;
                            for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                                    return local_shard.RelationshipsGet(grouped_rel_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                                std::vector<Relationship> combined;

                                for(const std::vector<Relationship>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetRelationshipsPeered(external_id, rel_types);
        }
    }

    // All
    seastar::future<std::vector<Node>> Shard::AllNodesPeered(uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.AllNodeIdCounts();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            uint64_t current = 0;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
            for (const auto& map : results) {
                std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
                for (auto entry : map) {
                    uint64_t next = current + entry.second;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                        threaded_requests.insert({ entry.first, pair });
                        if (next <= max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                requests.insert({current_shard_id++, threaded_requests});
            }

            std::vector<seastar::future<std::vector<Node>>> futures;

            for (const auto& request : requests) {
                for (auto entry : request.second) {
                    auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                        return local_shard.AllNodes(entry.first, entry.second.first, entry.second.second);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<Node>>& results) {
                std::vector<Node> requested_nodes;
                requested_nodes.reserve(limit);
                for (auto result : results) {
                    requested_nodes.insert(std::end(requested_nodes), std::begin(result), std::end(result));
                }
                return requested_nodes;
            });
        });
    }

    seastar::future<std::vector<Node>> Shard::AllNodesPeered(const std::string &type, uint64_t skip, uint64_t limit) {
        uint16_t node_type_id = node_types.getTypeId(type);
        uint64_t max = skip + limit;

        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [node_type_id] (Shard &local_shard) mutable {
                return local_shard.AllNodeIdCounts(node_type_id);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([node_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
            for (const auto& count : results) {
                uint64_t next = current + count;
                if (next > skip) {
                    std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                    requests.insert({ current_shard_id++, pair });
                    if (next <= max) {
                        break; // We have everything we need
                    }
                }
                current = next;
            }

            std::vector<seastar::future<std::vector<Node>>> futures;

            for (const auto& request : requests) {
                auto future = container().invoke_on(request.first, [node_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllNodes(node_type_id, request.second.first, request.second.second);
                });
                futures.push_back(std::move(future));

            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<Node>>& results) {
                std::vector<Node> requested_nodes;
                requested_nodes.reserve(limit);
                for (auto result : results) {
                    requested_nodes.insert(std::end(requested_nodes), std::begin(result), std::end(result));
                }
                return requested_nodes;
            });
        });
    }


    seastar::future<std::vector<uint64_t>> Shard::AllRelationshipIdsPeered(uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.AllRelationshipIdCounts();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            uint64_t current = 0;
            uint64_t next = 0;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
            for (const auto& map : results) {
                std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
                for (auto entry : map) {
                    next = current + entry.second;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                        threaded_requests.insert({ entry.first, pair });
                        if (next <= max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                requests.insert({current_shard_id++, threaded_requests});
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& request : requests) {
                for (auto entry : request.second) {
                    auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                        return local_shard.AllRelationshipIds(entry.first, entry.second.first, entry.second.second);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<uint64_t>>& results) {
                std::vector<uint64_t> ids;
                ids.reserve(limit);
                for (auto result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                }
                return ids;
            });
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::AllRelationshipIdsPeered(const std::string &rel_type, uint64_t skip, uint64_t limit) {
        uint16_t relationship_type_id = relationship_types.getTypeId(rel_type);
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [relationship_type_id] (Shard &local_shard) mutable {
                return local_shard.AllRelationshipIdCounts(relationship_type_id);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([relationship_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            uint64_t next = 0;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
            for (const auto& count : results) {
                next = current + count;
                if (next > skip) {
                    std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                    requests.insert({ current_shard_id++, pair });
                    if (next <= max) {
                        break; // We have everything we need
                    }
                }
                current = next;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& request : requests) {
                auto future = container().invoke_on(request.first, [relationship_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllRelationshipIds(relationship_type_id, request.second.first, request.second.second);
                });
                futures.push_back(std::move(future));

            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<uint64_t>>& results) {
                std::vector<uint64_t> ids;
                ids.reserve(limit);
                for (auto result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                }
                return ids;
            });
        });
    }

    seastar::future<std::vector<Relationship>> Shard::AllRelationshipsPeered(uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.AllRelationshipIdCounts();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            uint64_t current = 0;
            uint64_t next = 0;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
            for (const auto& map : results) {
                std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
                for (auto entry : map) {
                    next = current + entry.second;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                        threaded_requests.insert({ entry.first, pair });
                        if (next <= max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                requests.insert({current_shard_id++, threaded_requests});
            }

            std::vector<seastar::future<std::vector<Relationship>>> futures;

            for (const auto& request : requests) {
                for (auto entry : request.second) {
                    auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                        return local_shard.AllRelationships(entry.first, entry.second.first, entry.second.second);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<Relationship>>& results) {
                std::vector<Relationship> requested_relationships;
                requested_relationships.reserve(limit);
                for (auto result : results) {
                    requested_relationships.insert(std::end(requested_relationships), std::begin(result), std::end(result));
                }
                return requested_relationships;
            });
        });
    }

    seastar::future<std::vector<Relationship>> Shard::AllRelationshipsPeered(const std::string &rel_type, uint64_t skip, uint64_t limit) {
        uint16_t relationship_type_id = node_types.getTypeId(rel_type);
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [relationship_type_id] (Shard &local_shard) mutable {
                return local_shard.AllRelationshipIdCounts(relationship_type_id);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([relationship_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            uint64_t next = 0;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
            for (const auto& count : results) {
                next = current + count;
                if (next > skip) {
                    std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                    requests.insert({ current_shard_id++, pair });
                    if (next <= max) {
                        break; // We have everything we need
                    }
                }
                current = next;
            }

            std::vector<seastar::future<std::vector<Relationship>>> futures;

            for (const auto& request : requests) {
                auto future = container().invoke_on(request.first, [relationship_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllRelationships(relationship_type_id, request.second.first, request.second.second);
                });
                futures.push_back(std::move(future));

            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<Relationship>>& results) {
                std::vector<Relationship> requested_relationships;
                requested_relationships.reserve(limit);
                for (auto result : results) {
                    requested_relationships.insert(std::end(requested_relationships), std::begin(result), std::end(result));
                }
                return requested_relationships;
            });
        });
    }

}
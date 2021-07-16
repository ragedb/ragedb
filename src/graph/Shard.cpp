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
        return relationship_types.getRelationshipTypeProperties(type_id).getPropertyTypes();
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

    // Property Types =======================================================================================================================
    uint8_t Shard::NodePropertyTypeAdd(uint16_t type_id, const std::string& key, uint8_t property_type_id) {
        return node_types.getNodeTypeProperties(type_id).setPropertyTypeId(key, property_type_id);
    }

    uint8_t Shard::RelationshipPropertyTypeAdd(uint16_t type_id, const std::string& key, uint8_t property_type_id) {
        return relationship_types.getRelationshipTypeProperties(type_id).setPropertyTypeId(key, property_type_id);
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
                node_types.setProperties(type_id, internal_id, properties);
            } else {
                external_id = internalToExternal(type_id, internal_id);
                // Add the node to the end and prepare a place for its relationships
                node_types.getKeys(type_id).emplace_back(key);
                node_types.getOutgoingRelationships(type_id).emplace_back();
                node_types.getIncomingRelationships(type_id).emplace_back();
                node_types.addId(type_id, internal_id);
                node_types.setProperties(type_id, internal_id, properties);
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

    }

    std::vector<Node> Shard::AllNodes(const std::string& type, uint64_t skip, uint64_t limit) {
        uint16_t type_id = node_types.getTypeId(type);
        return AllNodes(type_id, skip, limit);
    }

    std::vector<Node> Shard::AllNodes(uint16_t type_id, uint64_t skip, uint64_t limit) {

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
        uint16_t node_type_id = node_types.getTypeId(type);
        if (node_type_id == 0) {
            // node_type_id is global so unfortunately we need to lock here
            this->node_type_lock.for_write().lock().get();
            // The node type was not found and must therefore be new, add it to all shards.
            node_type_id = node_types.insertOrGetTypeId(type);
            return container().invoke_on_all([type, node_type_id](Shard &local_shard) {
                        local_shard.NodeTypeInsert(type, node_type_id);
                    })
                    .then([node_type_id, this] {
                        this->node_type_lock.for_write().unlock();
                        return seastar::make_ready_future<uint16_t>(node_type_id);
                    });
        }

        return seastar::make_ready_future<uint16_t>(node_type_id);
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

    seastar::future<uint8_t> Shard::RelationshipPropertyTypeInsertPeered(uint16_t type_id, const std::string &key, const std::string &type) {
        relationship_types.getRelationshipTypeProperties(type_id).property_type_lock.for_write().lock().get();

        uint8_t property_type_id = relationship_types.getRelationshipTypeProperties(type_id).setPropertyType(key, type);

        return container().invoke_on_all([type_id, key, property_type_id](Shard &all_shards) {
            all_shards.RelationshipPropertyTypeAdd(type_id, key, property_type_id);
        }).then([type_id, property_type_id, this] {
            this->relationship_types.getRelationshipTypeProperties(type_id).property_type_lock.for_write().unlock();
            return seastar::make_ready_future<uint8_t>(property_type_id);
        });
    }


    //
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


}
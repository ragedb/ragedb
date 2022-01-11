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

#include "../Shard.h"

namespace ragedb {

    uint16_t Shard::NodeTypesGetCountPeered() {
        return node_types.getSize();
    }

    seastar::future<uint64_t> Shard::NodeTypesGetCountPeered(uint16_t type_id) {
        seastar::future<std::vector<uint64_t>> v = container().map([type_id] (Shard &local) {
            return local.NodeTypesGetCount(type_id);
        });

        return v.then([] (std::vector<uint64_t> counts) {
            return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
        });
    }

    seastar::future<uint64_t> Shard::NodeTypesGetCountPeered(const std::string &type) {
        seastar::future<std::vector<uint64_t>> v = container().map([type] (Shard &local) {
            return local.NodeTypesGetCount(type);
        });

        return v.then([] (std::vector<uint64_t> counts) {
            return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
        });
    }

    std::set<std::string> Shard::NodeTypesGetPeered() {
        return node_types.getTypes();
    }

    std::map<std::string, std::string> Shard::NodeTypeGetPeered(const std::string& type) {
      return container().local().NodeTypeGet(type);
    }

    std::map<std::string, std::string> Shard::RelationshipTypeGetPeered(const std::string& type) {
      return container().local().RelationshipTypeGet(type);
    }

    uint16_t Shard::RelationshipTypesGetCountPeered() {
        return relationship_types.getSize();
    }

    seastar::future<uint64_t> Shard::RelationshipTypesGetCountPeered(uint16_t type_id) {
        seastar::future<std::vector<uint64_t>> v = container().map([type_id] (Shard &local) {
            return local.RelationshipTypesGetCount(type_id);
        });

        return v.then([] (std::vector<uint64_t> counts) {
            return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
        });
    }

    seastar::future<uint64_t> Shard::RelationshipTypesGetCountPeered(const std::string &type) {
        seastar::future<std::vector<uint64_t>> v = container().map([type] (Shard &local) {
            return local.RelationshipTypesGetCount(type);
        });

        return v.then([] (std::vector<uint64_t> counts) {
            return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
        });
    }

    std::set<std::string> Shard::RelationshipTypesGetPeered() {
        return relationship_types.getTypes();
    }

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
        if (type_id != 0) {
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

    std::string Shard::RelationshipTypeGetTypePeered(uint16_t type_id) {
        return relationship_types.getType(type_id);
    }

    uint16_t Shard::RelationshipTypeGetTypeIdPeered(const std::string &type) {
        return relationship_types.getTypeId(type);
    }

    seastar::future<uint16_t> Shard::RelationshipTypeInsertPeered(const std::string &type) {
        // type_id is global, so we need to calculate it here
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) {
            // type_id is global so unfortunately we need to lock here
            this->rel_type_lock.for_write().lock().get();

            // The relationship type was not found and must therefore be new, add it to all shards.
            type_id = relationship_types.insertOrGetTypeId(type);
            this->rel_type_lock.for_write().unlock();
            return container().invoke_on_all([type, type_id](Shard &local_shard) {
                        local_shard.RelationshipTypeInsert(type, type_id);
                    })
                    .then([type_id] {
                        return seastar::make_ready_future<uint16_t>(type_id);
                    });
        }

        return seastar::make_ready_future<uint16_t>(type_id);
    }

    seastar::future<bool> Shard::DeleteRelationshipTypePeered(const std::string& type) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id != 0) {
            // type_id is global so unfortunately we need to lock here
            this->rel_type_lock.for_write().lock().get();
            // The type was found and must therefore be deleted on all shards.
            return container().invoke_on_all([type](Shard &local_shard) {
                        local_shard.DeleteRelationshipType(type);
                    })
                    .then([this] {
                        this->rel_type_lock.for_write().unlock();
                        return seastar::make_ready_future<bool>(true);
                    });
        }

        return seastar::make_ready_future<bool>(false);
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
        uint16_t node_type_id = node_types.getTypeId(node_type);
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

    seastar::future<uint8_t> Shard::RelationshipPropertyTypeAddPeered(const std::string& relationship_type, const std::string& key, const std::string& type) {
        uint16_t relationship_type_id = relationship_types.getTypeId(relationship_type);
        if (relationship_type_id == 0) {
            return container().invoke_on(0, [relationship_type, key, type, this] (Shard &local_shard) {
                return local_shard.RelationshipTypeInsertPeered(relationship_type).then([relationship_type, key, type, this](uint16_t node_type_id) {
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

}
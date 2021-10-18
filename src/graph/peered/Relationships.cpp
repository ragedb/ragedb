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

    seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(const std::string &rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2) {
        uint16_t shard_id1 = CalculateShardId(type1, key1);
        uint16_t shard_id2 = CalculateShardId(type2, key2);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

        // The rel type exists, continue on
        if (rel_type_id > 0) {
            // if the shards are the same, then handle this special case
            if(shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2](Shard &local_shard) {
                    return local_shard.RelationshipAddEmptySameShard(rel_type_id, type1, key1, type2, key2);
                });
            }
            // we need to get id2 on its shard
            return container().invoke_on(shard_id2, [type1, key1, type2, key2](Shard &local_shard) {
                return local_shard.NodeGetID(type2, key2);
            }).then([shard_id1, shard_id2, rel_type_id, type1, key1, this] (uint64_t id2) {
                // if id2 is valid, we need to get id1 on its shard
                if (id2 > 0) {
                    return container()
                    .invoke_on(shard_id1,[rel_type_id, type1, key1, id2](Shard &local_shard) {
                        std::vector<uint64_t> ids;
                        uint64_t id1 = local_shard.NodeGetID(type1, key1);
                        // if id1 is valid, we need to try to create the relationship
                        if (id1 > 0) {
                            uint64_t rel_id = local_shard.RelationshipAddEmptyToOutgoing(
                                    rel_type_id, id1, id2);
                            if (rel_id > 0) {
                                ids.push_back(id1);
                                ids.push_back(rel_id);
                            }
                        }
                        // We return a vector because we need both the node id and the relationship id
                        return ids;
                    }).then([rel_type_id, shard_id2, id2, this](std::vector<uint64_t> ids) {
                        // if the relationship is valid (and by extension both nodes) then add the second part
                        if (!ids.empty()) {
                            return container()
                            .invoke_on(shard_id2, [rel_type_id, id1 = ids[0], id2, rel_id = ids[1]](
                                    Shard &local_shard) {
                                return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                            });
                        }
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    });
                }
                // Invalid node id 2
                return seastar::make_ready_future<uint64_t>(uint64_t(0));
            });
        }

        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, type1, key1, type2, key2, this] (Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, rel_type, type1, key1, type2, key2, this] (uint16_t rel_type_id) {
                        // if the shards are the same, then handle this special case
                        if(shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2](Shard &local_shard) {
                                return local_shard.RelationshipAddEmptySameShard(rel_type_id, type1, key1, type2, key2);
                            });
                        }
                        // we need to get id2 on its shard
                        return container().invoke_on(shard_id2, [type1, key1, type2, key2](Shard &local_shard) {
                            return local_shard.NodeGetID(type2, key2);
                        }).then([shard_id1, shard_id2, rel_type_id, type1, key1, this] (uint64_t id2) {
                            // if id2 is valid, we need to get id1 on its shard
                            if (id2 > 0) {
                                return container()
                                        .invoke_on(shard_id1,[rel_type_id, type1, key1, id2](Shard &local_shard) {
                                            std::vector<uint64_t> ids;
                                            uint64_t id1 = local_shard.NodeGetID(type1, key1);
                                            // if id1 is valid, we need to try to create the relationship
                                            if (id1 > 0) {
                                                uint64_t rel_id = local_shard.RelationshipAddEmptyToOutgoing(
                                                        rel_type_id, id1, id2);
                                                if (rel_id > 0) {
                                                    ids.push_back(id1);
                                                    ids.push_back(rel_id);
                                                }
                                            }
                                            // We return a vector because we need both the node id and the relationship id
                                            return ids;
                                        }).then([rel_type_id, shard_id2, id2, this](std::vector<uint64_t> ids) {
                                    // if the relationship is valid (and by extension both nodes) then add the second part
                                    if (!ids.empty()) {
                                        return container()
                                                .invoke_on(shard_id2, [rel_type_id, id1 = ids[0], id2, rel_id = ids[1]](
                                                        Shard &local_shard) {
                                                    return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                                });
                                    }
                                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                });
                            }
                            // Invalid node id 2
                            return seastar::make_ready_future<uint64_t>(uint64_t(0));
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
            // if the shards are the same, then handle this special case
            if(shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2, properties](Shard &local_shard) {
                    return local_shard.RelationshipAddSameShard(rel_type_id, type1, key1, type2, key2, properties);
                });
            }
            // we need to get id2 on its shard
            return container().invoke_on(shard_id2, [type1, key1, type2, key2](Shard &local_shard) {
                return local_shard.NodeGetID(type2, key2);
            }).then([shard_id1, shard_id2, rel_type_id, type1, key1, properties, this] (uint64_t id2) {
                // if id2 is valid, we need to get id1 on its shard
                if (id2 > 0) {
                    return container()
                            .invoke_on(shard_id1,[rel_type_id, type1, key1, id2, properties](Shard &local_shard) {
                                std::vector<uint64_t> ids;
                                uint64_t id1 = local_shard.NodeGetID(type1, key1);
                                // if id1 is valid, we need to try to create the relationship
                                if (id1 > 0) {
                                    uint64_t rel_id = local_shard.RelationshipAddToOutgoing(
                                            rel_type_id, id1, id2, properties);
                                    if (rel_id > 0) {
                                        ids.push_back(id1);
                                        ids.push_back(rel_id);
                                    }
                                }
                                // We return a vector because we need both the node id and the relationship id
                                return ids;
                            }).then([rel_type_id, shard_id2, id2, this](std::vector<uint64_t> ids) {
                        // if the relationship is valid (and by extension both nodes) then add the second part
                        if (!ids.empty()) {
                            return container()
                                    .invoke_on(shard_id2, [rel_type_id, id1 = ids[0], id2, rel_id = ids[1]](
                                            Shard &local_shard) {
                                        return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                    });
                        }
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    });
                }
                // Invalid node id 2
                return seastar::make_ready_future<uint64_t>(uint64_t(0));
            });
        }

        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, type1, key1, type2, key2, properties, this] (Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, rel_type, type1, key1, type2, key2, properties, this] (uint16_t rel_type_id) {
                        // if the shards are the same, then handle this special case
                        if (shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2, properties](
                                    Shard &local_shard) {
                                return local_shard.RelationshipAddSameShard(rel_type_id, type1, key1, type2, key2,
                                                                            properties);
                            });
                        }
                        // we need to get id2 on its shard
                        return container().invoke_on(shard_id2, [type1, key1, type2, key2](Shard &local_shard) {
                            return local_shard.NodeGetID(type2, key2);
                        }).then([shard_id1, shard_id2, rel_type_id, type1, key1, properties, this](uint64_t id2) {
                            // if id2 is valid, we need to get id1 on its shard
                            if (id2 > 0) {
                                return container()
                                        .invoke_on(shard_id1,
                                                   [rel_type_id, type1, key1, id2, properties](Shard &local_shard) {
                                                       std::vector<uint64_t> ids;
                                                       uint64_t id1 = local_shard.NodeGetID(type1, key1);
                                                       // if id1 is valid, we need to try to create the relationship
                                                       if (id1 > 0) {
                                                           uint64_t rel_id = local_shard.RelationshipAddToOutgoing(
                                                                   rel_type_id, id1, id2, properties);
                                                           if (rel_id > 0) {
                                                               ids.push_back(id1);
                                                               ids.push_back(rel_id);
                                                           }
                                                       }
                                                       // We return a vector because we need both the node id and the relationship id
                                                       return ids;
                                                   }).then(
                                        [rel_type_id, shard_id2, id2, this](std::vector<uint64_t> ids) {
                                            // if the relationship is valid (and by extension both nodes) then add the second part
                                            if (!ids.empty()) {
                                                return container()
                                                        .invoke_on(shard_id2,
                                                                   [rel_type_id, id1 = ids[0], id2, rel_id = ids[1]](
                                                                           Shard &local_shard) {
                                                                       return local_shard.RelationshipAddToIncoming(
                                                                               rel_type_id, rel_id, id1, id2);
                                                                   });
                                            }
                                            return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                        });
                            }
                            // Invalid node id 2
                            return seastar::make_ready_future<uint64_t>(uint64_t(0));
                        });
                    });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(const std::string &rel_type, uint64_t id1, uint64_t id2) {
        // Get the shard ids and check if the type exists
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);
        uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
        // The rel type exists, continue on
        if (rel_type_id > 0) {
            // if the shards are the same, then handle this special case
            if (shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard) {
                    return local_shard.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
                });
            }
            // we need to validate id2 on its shard
            return container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            }).then([shard_id1, shard_id2, rel_type_id, id1, id2, this] (bool valid) {
                // if id2 is valid, we need to validate id1 on its shard
                if (valid) {
                    return container().invoke_on(shard_id1,[rel_type_id, id1, id2](Shard &local_shard) {
                        if (local_shard.ValidNodeId(id1)) {
                            // if both nodes are valid, then go ahead and try to create the relationship
                            return local_shard.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2);
                        }
                        // Invalid id1
                        return uint64_t(0);
                            }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                // if the relationship is valid (and by extension both nodes) then add the second part
                                if (rel_id > 0) {
                                 return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                        return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                 });
                                }
                                return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    });
                }
                // Invalid id2
                return seastar::make_ready_future<uint64_t>(uint64_t(0));
            });
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

                        // we need to validate id2 on its shard
                        return container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                            return local_shard.ValidNodeId(id2);
                        }).then([shard_id1, shard_id2, rel_type_id, id1, id2, this] (bool valid) {
                            // if id2 is valid, we need to validate id1 on its shard
                            if (valid) {
                                return container().invoke_on(shard_id1,[rel_type_id, id1, id2](Shard &local_shard) {
                                    if (local_shard.ValidNodeId(id1)) {
                                        // if both nodes are valid, then go ahead and try to create the relationship
                                        return local_shard.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2);
                                    }
                                    // Invalid id1
                                    return uint64_t(0);
                                }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                    // if the relationship is valid (and by extension both nodes) then add the second part
                                    if (rel_id > 0) {
                                        return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                            return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                        });
                                    }
                                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                });
                            }
                            // Invalid id2
                            return seastar::make_ready_future<uint64_t>(uint64_t(0));
                        });
                    });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
        // Get the shard ids and check if the type exists
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);

        // The rel type exists, continue on
        if (relationship_types.ValidTypeId(rel_type_id)) {
            // if the shards are the same, then handle this special case
            if (shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard) {
                    return local_shard.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
                });
            }
            // we need to validate id2 on its shard
            return container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            }).then([shard_id1, shard_id2, rel_type_id, id1, id2, this] (bool valid) {
                // if id2 is valid, we need to validate id1 on its shard
                if (valid) {
                    return container().invoke_on(shard_id1,[rel_type_id, id1, id2](Shard &local_shard) {
                        if (local_shard.ValidNodeId(id1)) {
                            // if both nodes are valid, then go ahead and try to create the relationship
                            return local_shard.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2);
                        }
                        // Invalid id1
                        return uint64_t(0);
                    }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                        // if the relationship is valid (and by extension both nodes) then add the second part
                        if (rel_id > 0) {
                            return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                            });
                        }
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    });
                }
                // Invalid id2
                return seastar::make_ready_future<uint64_t>(uint64_t(0));
            });
        }
        // Invalid Relationship type id
        return seastar::make_ready_future<uint64_t>(uint64_t(0));
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

            // we need to validate id2 on its shard
            return container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            }).then([shard_id1, shard_id2, rel_type_id, id1, id2, properties, this] (bool valid) {
                // if id2 is valid, we need to validate id1 on its shard
                if (valid) {
                    return container().invoke_on(shard_id1,[rel_type_id, id1, id2, properties](Shard &local_shard) {
                        if (local_shard.ValidNodeId(id1)) {
                            // if both nodes are valid, then go ahead and try to create the relationship
                            return local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties);
                        }
                        // Invalid id1
                        return uint64_t(0);
                    }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                        // if the relationship is valid (and by extension both nodes) then add the second part
                        if (rel_id > 0) {
                            return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                            });
                        }
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    });
                }
                // Invalid id2
                return seastar::make_ready_future<uint64_t>(uint64_t(0));
            });
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

                        // we need to validate id2 on its shard
                        return container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                            return local_shard.ValidNodeId(id2);
                        }).then([shard_id1, shard_id2, rel_type_id, id1, id2, properties, this] (bool valid) {
                            // if id2 is valid, we need to validate id1 on its shard
                            if (valid) {
                                return container().invoke_on(shard_id1,[rel_type_id, id1, id2, properties](Shard &local_shard) {
                                    if (local_shard.ValidNodeId(id1)) {
                                        // if both nodes are valid, then go ahead and try to create the relationship
                                        return local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties);
                                    }
                                    // Invalid id1
                                    return uint64_t(0);
                                }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                    // if the relationship is valid (and by extension both nodes) then add the second part
                                    if (rel_id > 0) {
                                        return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                            return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                        });
                                    }
                                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                });
                            }
                            // Invalid id2
                            return seastar::make_ready_future<uint64_t>(uint64_t(0));
                        });
                    });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);

        // The rel type exists, continue on
        if (relationship_types.ValidTypeId(rel_type_id)) {
            if (shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard) {
                    return local_shard.RelationshipAddSameShard(rel_type_id, id1, id2, properties);
                });
            }

            // we need to validate id2 on its shard
            return container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            }).then([shard_id1, shard_id2, rel_type_id, id1, id2, properties, this] (bool valid) {
                // if id2 is valid, we need to validate id1 on its shard
                if (valid) {
                    return container().invoke_on(shard_id1,[rel_type_id, id1, id2, properties](Shard &local_shard) {
                        if (local_shard.ValidNodeId(id1)) {
                            // if both nodes are valid, then go ahead and try to create the relationship
                            return local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties);
                        }
                        // Invalid id1
                        return uint64_t(0);
                    }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                        // if the relationship is valid (and by extension both nodes) then add the second part
                        if (rel_id > 0) {
                            return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                            });
                        }
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    });
                }
                // Invalid id2
                return seastar::make_ready_future<uint64_t>(uint64_t(0));
            });
        }

        // Invalid Relationship type id
        return seastar::make_ready_future<uint64_t>(uint64_t(0));
    }

    seastar::future<Relationship> Shard::RelationshipGetPeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
            return local_shard.RelationshipGet(id);
        });
    }

    seastar::future<std::vector<Relationship>> Shard::RelationshipsGetPeered(const std::vector<uint64_t> &ids) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationship_ids;
        for (int i = 0; i < cpus; i++) {
            sharded_relationship_ids.insert({i, std::vector<uint64_t>() });
        }
        for (auto id : ids) {
            sharded_relationship_ids[CalculateShardId(id)].emplace_back(id);
        }

        std::vector<seastar::future<std::vector<Relationship>>> futures;
        for (auto const& [their_shard, grouped_relationship_ids] : sharded_relationship_ids ) {
            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_relationship_ids] (Shard &local_shard) {
                return local_shard.RelationshipsGet(grouped_node_ids);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
            std::vector<Relationship> combined;

            for(const std::vector<Relationship>& sharded : results) {
                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
            }
            return combined;
        });
    }

    seastar::future<std::vector<Relationship>> Shard::RelationshipsGetPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationship_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationship_ids.insert({i, std::vector<uint64_t>() });
      }
      for(Link link : links) {
        sharded_relationship_ids[CalculateShardId(Link::rel_id(link))].emplace_back(Link::rel_id(link));
      }

      std::vector<seastar::future<std::vector<Relationship>>> futures;
      for (auto const& [their_shard, grouped_relationship_ids] : sharded_relationship_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_relationship_ids] (Shard &local_shard) {
          return local_shard.RelationshipsGet(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
        std::vector<Relationship> combined;

        for(const std::vector<Relationship>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
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
        return seastar::make_ready_future<std::string>(RelationshipGetType(id));
    }

    seastar::future<uint16_t> Shard::RelationshipGetTypeIdPeered(uint64_t id) {
        return seastar::make_ready_future<uint16_t>(RelationshipGetTypeId(id));
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

    seastar::future<std::any> Shard::RelationshipPropertyGetPeered(uint64_t id, const std::string &property) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
            return local_shard.RelationshipPropertyGet(id, property);
        });
    }

    seastar::future<bool> Shard::RelationshipPropertySetPeered(uint64_t id, const std::string& property, const std::any& value) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
            return local_shard.RelationshipPropertySet(id, property, value);
        });
    }

    seastar::future<bool> Shard::RelationshipPropertySetFromJsonPeered(uint64_t id, const std::string &property, const std::string &value) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
            return local_shard.RelationshipPropertySetFromJson(id, property, value);
        });
    }

    seastar::future<bool> Shard::RelationshipPropertyDeletePeered(uint64_t id, const std::string &property) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
            return local_shard.RelationshipPropertyDelete(id, property);
        });
    }

    seastar::future<std::map<std::string, std::any>> Shard::RelationshipPropertiesGetPeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id](Shard &local_shard) {
            return local_shard.RelationshipPropertiesGet(id);
        });
    }

    seastar::future<bool> Shard::RelationshipPropertiesSetFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, value](Shard &local_shard) {
            return local_shard.RelationshipPropertiesSetFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::RelationshipPropertiesResetFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, value](Shard &local_shard) {
            return local_shard.RelationshipPropertiesResetFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::RelationshipPropertiesDeletePeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id](Shard &local_shard) {
            return local_shard.RelationshipPropertiesDelete(id);
        });
    }

}
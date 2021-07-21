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

}
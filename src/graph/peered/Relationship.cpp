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

    // Helpers
    std::pair <uint16_t, uint64_t> Shard::RelationshipRemoveGetIncoming(uint64_t external_id) {

      uint16_t rel_type_id = externalToTypeId(external_id);
      uint64_t internal_id = externalToInternal(external_id);

      uint64_t id1 = relationship_types.getStartingNodeId(rel_type_id, internal_id);
      uint64_t id2 = relationship_types.getEndingNodeId(rel_type_id, internal_id);
      uint16_t id1_type_id = externalToTypeId(id1);

      // Add to deleted relationships bitmap
      relationship_types.removeId(rel_type_id, internal_id);

      uint64_t internal_id1 = externalToInternal(id1);

      // Remove relationship from Node 1
      auto group = std::ranges::find_if(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1),
        [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

      if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
          auto rel_to_delete = std::ranges::find_if(group->links, [external_id](Link entry) {
          return entry.rel_id == external_id;
        });
        if (rel_to_delete != std::end(group->links)) {
          group->links.erase(rel_to_delete);
        }
      }

      // Clear the relationship
      relationship_types.setStartingNodeId(rel_type_id, internal_id, 0);
      relationship_types.setEndingNodeId(rel_type_id, internal_id, 0);
      relationship_types.deleteProperties(rel_type_id, internal_id);

      // Return the rel_type and other node Id
      return std::pair <uint16_t ,uint64_t> (rel_type_id, id2);
    }

    bool Shard::RelationshipRemoveIncoming(uint16_t rel_type_id, uint64_t external_id, uint64_t node_id) {
      // Remove relationship from Node 2
      uint64_t internal_id2 = externalToInternal(node_id);
      uint16_t id2_type_id = externalToTypeId(node_id);

      auto group = std::ranges::find_if(node_types.getIncomingRelationships(id2_type_id).at(internal_id2),
        [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

      auto rel_to_delete = std::ranges::find_if(group->links, [external_id](Link entry) {
        return entry.rel_id == external_id;
      });
      if (rel_to_delete != std::end(group->links)) {
        group->links.erase(rel_to_delete);
      }

      return true;
    }

    seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(const std::string &rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2) {
        uint16_t shard_id1 = CalculateShardId(type1, key1);
        uint16_t shard_id2 = CalculateShardId(type2, key2);

        // The rel type exists, continue on
        if (uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id > 0) [[likely]] {
            // if the shards are the same, then handle this special case
            if(shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2](Shard &local_shard) {
                    return local_shard.RelationshipAddEmptySameShard(rel_type_id, type1, key1, type2, key2);
                });
            }
            // we need to get id2 on its shard
            return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                return local_shard.NodeGetID(type2, key2);
            }).then([shard_id1, shard_id2, rel_type_id, type1, key1, this] (uint64_t id2) {
                if (id2 == 0) {
                    // Invalid node id 2
                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                }
                // if id2 is valid, we need to get id1 on its shard
                return container().invoke_on(shard_id1,[rel_type_id, type1, key1, id2](Shard &local_shard2) {
                    std::vector<uint64_t> ids;
                    // if id1 is valid, we need to try to create the relationship
                    if (uint64_t id1 = local_shard2.NodeGetID(type1, key1);
                        id1 > 0) {
                        uint64_t rel_id = local_shard2.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2);
                        if (rel_id > 0) {
                            ids.push_back(id1);
                            ids.push_back(rel_id);
                        }
                    }
                    // We return a vector because we need both the node id and the relationship id
                    return ids;
                }).then([rel_type_id, shard_id2, id2, this](std::vector<uint64_t> ids) {
                    // if the relationship is valid (and by extension both nodes) then add the second part
                    if (ids.empty()) {
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    }
                    return container().invoke_on(shard_id2, [rel_type_id, id1 = ids[0], id2, rel_id = ids[1]](Shard &local_shard) {
                        return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                    });
                });
            });
        }

        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, type1, key1, type2, key2, this] (Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, type1, key1, type2, key2, this] (uint16_t rel_type_id) {
                        // if the shards are the same, then handle this special case
                        if(shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2](Shard &local_shard2) {
                                return local_shard2.RelationshipAddEmptySameShard(rel_type_id, type1, key1, type2, key2);
                            });
                        }
                        // we need to get id2 on its shard
                        return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                            return local_shard.NodeGetID(type2, key2);
                        }).then([shard_id1, shard_id2, rel_type_id, type1, key1, this] (uint64_t id2) {
                            if (id2 == 0) {
                                // Invalid node id 2
                                return seastar::make_ready_future<uint64_t>(uint64_t(0));
                            }
                            // if id2 is valid, we need to get id1 on its shard
                            return container().invoke_on(shard_id1,[rel_type_id, type1, key1, id2](Shard &local_shard2) {
                                std::vector<uint64_t> ids;
                                // if id1 is valid, we need to try to create the relationship
                                if (uint64_t id1 = local_shard2.NodeGetID(type1, key1);
                                    id1 > 0) {
                                    uint64_t rel_id = local_shard2.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2);
                                    if (rel_id > 0) {
                                        ids.push_back(id1);
                                        ids.push_back(rel_id);
                                    }
                                }
                                // We return a vector because we need both the node id and the relationship id
                                return ids;
                            }).then([rel_type_id, shard_id2, id2, this](std::vector<uint64_t> ids) {
                            // if the relationship is valid (and by extension both nodes) then add the second part
                            if (ids.empty()) {
                                return seastar::make_ready_future<uint64_t>(uint64_t(0));
                            }
                            return container().invoke_on(shard_id2, [rel_type_id, id1 = ids[0], id2, rel_id = ids[1]](Shard &local_shard) {
                                return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                            });
                        });
                    });
                });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddPeered(const std::string &rel_type, const std::string &type1, const std::string &key1,
                                                           const std::string &type2, const std::string &key2, const std::string& properties) {
        uint16_t shard_id1 = CalculateShardId(type1, key1);
        uint16_t shard_id2 = CalculateShardId(type2, key2);

        // The rel type exists, continue on
        if ( uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id > 0) [[likely]] {
            // if the shards are the same, then handle this special case
            if(shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2, properties](Shard &local_shard) {
                    return local_shard.RelationshipAddSameShard(rel_type_id, type1, key1, type2, key2, properties);
                });
            }
            // we need to get id2 on its shard
            return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                return local_shard.NodeGetID(type2, key2);
            }).then([shard_id1, shard_id2, rel_type_id, type1, key1, properties, this] (uint64_t id2) {
                if (id2 == 0) {
                    // Invalid node id 2
                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                }
                // if id2 is valid, we need to get id1 on its shard
                return container()
                        .invoke_on(shard_id1,[rel_type_id, type1, key1, id2, properties](Shard &local_shard) {
                            std::vector<uint64_t> ids;
                            // if id1 is valid, we need to try to create the relationship
                            if (uint64_t id1 = local_shard.NodeGetID(type1, key1);
                                id1 > 0) {
                                uint64_t rel_id = local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties);
                                if (rel_id > 0) {
                                    ids.push_back(id1);
                                    ids.push_back(rel_id);
                                }
                            }
                            // We return a vector because we need both the node id and the relationship id
                            return ids;
                        }).then([rel_type_id, shard_id2, id2, this](std::vector<uint64_t> ids) {
                    // if the relationship is valid (and by extension both nodes) then add the second part
                    if (ids.empty()) {
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    }
                    return container().invoke_on(shard_id2, [rel_type_id, id1 = ids[0], id2, rel_id = ids[1]](Shard &local_shard) {
                        return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                    });
                });
            });
        }

        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, type1, key1, type2, key2, properties, this] (Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, type1, key1, type2, key2, properties, this] (uint16_t rel_type_id) {
                        // if the shards are the same, then handle this special case
                        if (shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2, properties](
                                    Shard &local_shard) {
                                return local_shard.RelationshipAddSameShard(rel_type_id, type1, key1, type2, key2,
                                                                            properties);
                            });
                        }
                        // we need to get id2 on its shard
                        return container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                            return local_shard.NodeGetID(type2, key2);
                        }).then([shard_id1, shard_id2, rel_type_id, type1, key1, properties, this](uint64_t id2) {
                            if (id2 == 0) {
                                // Invalid node id 2
                                return seastar::make_ready_future<uint64_t>(uint64_t(0));
                            }
                                // if id2 is valid, we need to get id1 on its shard
                                return container().invoke_on(shard_id1,[rel_type_id, type1, key1, id2, properties](Shard &local_shard2) {
                                   std::vector<uint64_t> ids;
                                   // if id1 is valid, we need to try to create the relationship
                                   if (uint64_t id1 = local_shard2.NodeGetID(type1, key1);
                                       id1 > 0) {
                                       uint64_t rel_id = local_shard2.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties);
                                       if (rel_id > 0) {
                                           ids.push_back(id1);
                                           ids.push_back(rel_id);
                                       }
                                   }
                                   // We return a vector because we need both the node id and the relationship id
                                   return ids;
                               }).then([rel_type_id, shard_id2, id2, this](std::vector<uint64_t> ids) {
                                    // if the relationship is valid (and by extension both nodes) then add the second part
                                    if (ids.empty()) {
                                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                    }
                                    return container().invoke_on(shard_id2,[rel_type_id, id1 = ids[0], id2, rel_id = ids[1]](Shard &local_shard3) {
                                       return local_shard3.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                   });
                                });
                          });
              });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(const std::string &rel_type, uint64_t id1, uint64_t id2) {
        // Get the shard ids and check if the type exists
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);

        // The rel type exists, continue on
        if (uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id > 0) [[likely]] {
            // if the shards are the same, then handle this special case
            if (shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard) {
                    return local_shard.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
                });
            }
            // we need to validate id2 on its shard
            return container().invoke_on(shard_id2, [id2](const Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            }).then([shard_id1, shard_id2, rel_type_id, id1, id2, this] (bool valid) {
                if (!valid) {
                    // Invalid id2
                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                }
                // if id2 is valid, we need to validate id1 on its shard
                return container().invoke_on(shard_id1,[rel_type_id, id1, id2](Shard &local_shard) {
                    if (local_shard.ValidNodeId(id1)) {
                        // if both nodes are valid, then go ahead and try to create the relationship
                        return local_shard.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2);
                    }
                    // Invalid id1
                    return uint64_t(0);
                }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                    // if the relationship is valid (and by extension both nodes) then add the second part
                    if (rel_id == 0) {
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    }
                     return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                            return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                     });
                  });
            });
        }
        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, id1, id2, this](Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, id1, id2, this](uint16_t rel_type_id) {
                        if (shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard2) {
                                return local_shard2.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
                            });
                        }

                        // we need to validate id2 on its shard
                        return container().invoke_on(shard_id2, [id2](const Shard &local_shard) {
                            return local_shard.ValidNodeId(id2);
                        }).then([shard_id1, shard_id2, rel_type_id, id1, id2, this] (bool valid) {
                            if (valid) {
                                // Invalid id2
                                return seastar::make_ready_future<uint64_t>(uint64_t(0));
                            }
                            // if id2 is valid, we need to validate id1 on its shard
                            return container().invoke_on(shard_id1,[rel_type_id, id1, id2](Shard &local_shard2) {
                                if (local_shard2.ValidNodeId(id1)) {
                                    // if both nodes are valid, then go ahead and try to create the relationship
                                    return local_shard2.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2);
                                }
                                // Invalid id1
                                return uint64_t(0);
                            }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                // if the relationship is valid (and by extension both nodes) then add the second part
                                if (rel_id == 0) {
                                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                }
                                return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard3) {
                                    return local_shard3.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                });
                            });
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
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard2) {
                    return local_shard2.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
                });
            }
            // we need to validate id2 on its shard
            return container().invoke_on(shard_id2, [id2](const Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            }).then([shard_id1, shard_id2, rel_type_id, id1, id2, this] (bool valid) {
                if (!valid) {
                    // Invalid id2
                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                }
                // if id2 is valid, we need to validate id1 on its shard
                return container().invoke_on(shard_id1,[rel_type_id, id1, id2](Shard &local_shard2) {
                    if (local_shard2.ValidNodeId(id1)) {
                        // if both nodes are valid, then go ahead and try to create the relationship
                        return local_shard2.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2);
                    }
                    // Invalid id1
                    return uint64_t(0);
                }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                    // if the relationship is valid (and by extension both nodes) then add the second part
                    if (rel_id == 0) {
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    }
                    return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard3) {
                        return local_shard3.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                    });
                });
            });
        }
        // Invalid Relationship type id
        return seastar::make_ready_future<uint64_t>(uint64_t(0));
    }

    seastar::future<uint64_t> Shard::RelationshipAddPeered(const std::string &rel_type, uint64_t id1, uint64_t id2, const std::string& properties) {
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);

        // The rel type exists, continue on
        if ( uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id > 0) [[likely]] {
            if (shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard) {
                    return local_shard.RelationshipAddSameShard(rel_type_id, id1, id2, properties);
                });
            }

            // we need to validate id2 on its shard
            return container().invoke_on(shard_id2, [id2](const Shard &local_shard) {
                return local_shard.ValidNodeId(id2);
            }).then([shard_id1, shard_id2, rel_type_id, id1, id2, properties, this] (bool valid) {
                if (!valid) {
                    // Invalid id2
                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                }
                // if id2 is valid, we need to validate id1 on its shard
                return container().invoke_on(shard_id1,[rel_type_id, id1, id2, properties](Shard &local_shard) {
                    if (local_shard.ValidNodeId(id1)) {
                        // if both nodes are valid, then go ahead and try to create the relationship
                        return local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties);
                    }
                    // Invalid id1
                    return uint64_t(0);
                }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                    // if the relationship is valid (and by extension both nodes) then add the second part
                    if (rel_id == 0) {
                        return seastar::make_ready_future<uint64_t>(uint64_t(0));
                    }
                    return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                        return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                    });
                });
            });
        }

        // The relationship type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [shard_id1, shard_id2, rel_type, id1, id2, properties, this](Shard &local_shard) {
            return local_shard.RelationshipTypeInsertPeered(rel_type)
                    .then([shard_id1, shard_id2, id1, id2, properties, this](uint16_t rel_type_id) {
                        if (shard_id1 == shard_id2) {
                            return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard2) {
                                return local_shard2.RelationshipAddSameShard(rel_type_id, id1, id2, properties);
                            });
                        }

                        // we need to validate id2 on its shard
                        return container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                            return local_shard.ValidNodeId(id2);
                        }).then([shard_id1, shard_id2, rel_type_id, id1, id2, properties, this] (bool valid) {
                            if (!valid) {
                                // Invalid id2
                                return seastar::make_ready_future<uint64_t>(uint64_t(0));
                            }
                            // if id2 is valid, we need to validate id1 on its shard
                            return container().invoke_on(shard_id1,[rel_type_id, id1, id2, properties](Shard &local_shard2) {
                                if (local_shard2.ValidNodeId(id1)) {
                                    // if both nodes are valid, then go ahead and try to create the relationship
                                    return local_shard2.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties);
                                }
                                // Invalid id1
                                return uint64_t(0);
                            }).then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                // if the relationship is valid (and by extension both nodes) then add the second part
                                if (rel_id == 0) {
                                    return seastar::make_ready_future<uint64_t>(uint64_t(0));
                                }
                                return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard3) {
                                    return local_shard3.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                });
                            });
                        });
                    });
        });
    }

    seastar::future<uint64_t> Shard::RelationshipAddPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties)
    {
        uint16_t shard_id1 = CalculateShardId(id1);
        uint16_t shard_id2 = CalculateShardId(id2);

        // The rel type exists, continue on
        if (relationship_types.ValidTypeId(rel_type_id)) [[likely]] {
            if (shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard) {
                    return local_shard.RelationshipAddSameShard(rel_type_id, id1, id2, properties);
                });
            }

            // we need to validate id2 on its shard
            return container().invoke_on(shard_id2, [id2](const Shard &local_shard) {
                                  return local_shard.ValidNodeId(id2);
                              })
              .then([shard_id1, shard_id2, rel_type_id, id1, id2, properties, this](bool valid) {
                  if (!valid) {
                      // Invalid id2
                      return seastar::make_ready_future<uint64_t>(uint64_t(0));
                  }
                  // if id2 is valid, we need to validate id1 on its shard
                  return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard) {
                                        if (local_shard.ValidNodeId(id1)) {
                                            // if both nodes are valid, then go ahead and try to create the relationship
                                            return local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties);
                                        }
                                        // Invalid id1
                                        return uint64_t(0);
                                    })
                    .then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                        // if the relationship is valid (and by extension both nodes) then add the second part
                        if (rel_id == 0) {
                            return seastar::make_ready_future<uint64_t>(uint64_t(0));
                        }
                        return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                            return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                        });
                    });
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

    seastar::future<bool> Shard::RelationshipRemovePeered(uint64_t external_id) {
        uint16_t rel_shard_id = CalculateShardId(external_id);

        return container().invoke_on(rel_shard_id, [external_id] (const Shard &local_shard) {
            return local_shard.ValidRelationshipId(external_id);
        }).then([rel_shard_id, external_id, this] (bool valid) {
            if(!valid) {
                return seastar::make_ready_future<bool>(false);
            }
            return container().invoke_on(rel_shard_id, [external_id] (Shard &local_shard) {
                return local_shard.RelationshipRemoveGetIncoming(external_id);
            }).then([external_id, this] (std::pair <uint16_t, uint64_t> rel_type_incoming_node_id) {
                uint16_t shard_id2 = CalculateShardId(rel_type_incoming_node_id.second);
                return container().invoke_on(shard_id2, [rel_type_incoming_node_id, external_id] (Shard &local_shard) {
                    return local_shard.RelationshipRemoveIncoming(rel_type_incoming_node_id.first, external_id, rel_type_incoming_node_id.second);
                });
            });
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

    seastar::future<property_type_t> Shard::RelationshipGetPropertyPeered(uint64_t id, const std::string &property) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
            return local_shard.RelationshipGetProperty(id, property);
        });
    }

    seastar::future<bool> Shard::RelationshipSetPropertyPeered(uint64_t id, const std::string& property, const property_type_t& value) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
            return local_shard.RelationshipSetProperty(id, property, value);
        });
    }

    seastar::future<bool> Shard::RelationshipSetPropertyFromJsonPeered(uint64_t id, const std::string &property, const std::string &value) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
            return local_shard.RelationshipSetPropertyFromJson(id, property, value);
        });
    }

    seastar::future<bool> Shard::RelationshipDeletePropertyPeered(uint64_t id, const std::string &property) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
            return local_shard.RelationshipDeleteProperty(id, property);
        });
    }

    seastar::future<std::map<std::string, property_type_t>> Shard::RelationshipGetPropertiesPeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id](Shard &local_shard) {
            return local_shard.RelationshipGetProperties(id);
        });
    }

    seastar::future<bool> Shard::RelationshipSetPropertiesFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, value](Shard &local_shard) {
            return local_shard.RelationshipSetPropertiesFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::RelationshipResetPropertiesFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id, value](Shard &local_shard) {
            return local_shard.RelationshipResetPropertiesFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::RelationshipDeletePropertiesPeered(uint64_t id) {
        uint16_t rel_shard_id = CalculateShardId(id);

        return container().invoke_on(rel_shard_id, [id](Shard &local_shard) {
            return local_shard.RelationshipDeleteProperties(id);
        });
    }

}
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

    seastar::future<std::vector<uint64_t>> Shard::NodeAddManyPeered(const std::string &type, const std::vector<std::string>& keys, const std::vector<std::string>& properties) {
      std::map<uint16_t, std::vector<std::tuple<std::string, std::string>>> sharded_nodes = PartitionNodesByNodeTypeKeys(type, keys, properties);
      uint16_t type_id = node_types.getTypeId(type);

      // The node type exists, so continue on
      if (type_id > 0) {
        std::vector<seastar::future<std::vector<uint64_t>>> futures;
        for (auto const& [their_shard, grouped_nodes] : sharded_nodes ) {
          auto future = container().invoke_on(their_shard, [grouped_nodes = grouped_nodes, type_id] (Shard &local_shard) {
            return local_shard.NodeAddMany(type_id, grouped_nodes);
          });
          futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));

        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint64_t>>& results) {
          std::vector<uint64_t> ids;

          for(auto sharded : results) {
            ids.insert(std::end(ids), std::begin(sharded), std::end(sharded));
          }
          return ids;
        });
      }

      // The node type needs to be set by Shard 0 and propagated
      return container().invoke_on(0, [type, sharded_nodes, this](Shard &local_shard) {
        return local_shard.NodeTypeInsertPeered(type).then([sharded_nodes, this](uint16_t type_id) {
          std::vector<seastar::future<std::vector<uint64_t>>> futures;
          for (auto const& [their_shard, grouped_nodes] : sharded_nodes ) {
            auto future = container().invoke_on(their_shard, [grouped_nodes = grouped_nodes, type_id] (Shard &local_shard) {
              return local_shard.NodeAddMany(type_id, grouped_nodes);
            });
            futures.push_back(std::move(future));
          }

          auto p = make_shared(std::move(futures));

          return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint64_t>>& results) {
            std::vector<uint64_t> ids;

            for(auto sharded : results) {
              ids.insert(std::end(ids), std::begin(sharded), std::end(sharded));
            }
            return ids;
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

    seastar::future<bool> Shard::NodeRemovePeeredIncoming(uint16_t node_shard_id, uint64_t external_id) {
        return container().invoke_on(node_shard_id, [external_id] (Shard &local_shard) {
            return local_shard.NodeRemoveGetIncoming(external_id);
        })
        .then([external_id, this] (auto sharded_grouped_rels) mutable {
            std::vector<seastar::future<bool>> futures;
            for (auto const& [their_shard, grouped_rels] : sharded_grouped_rels ) {
                auto future = container().invoke_on(their_shard, [external_id, grouped_rels = std::move(grouped_rels)] (Shard &local_shard) {
                    return local_shard.NodeRemoveDeleteIncoming(external_id, grouped_rels);
                });
                futures.push_back(std::move(future));
            }

            auto p = make_shared(std::move(futures));
            return seastar::when_all(p->begin(), p->end())
            .then([p] (std::vector<seastar::future<bool>> results) {
                if (std::any_of(results.begin(), results.end(), [] (auto& f) { return f.failed(); })) {
                    return seastar::make_ready_future<bool>(false);
                }
                return seastar::make_ready_future<bool>(true);
            });
        });
    }

    seastar::future<bool> Shard::NodeRemovePeeredOutgoing(uint16_t node_shard_id, uint64_t external_id) {
        return container().invoke_on(node_shard_id, [external_id] (Shard &local_shard) {
            return local_shard.NodeRemoveGetOutgoing(external_id);
        }).then([external_id, this] (auto sharded_grouped_rels) {
            std::vector<seastar::future<bool>> futures;
            for (auto const& [their_shard, grouped_rels] : sharded_grouped_rels ) {
                auto future = container().invoke_on(their_shard, [external_id, grouped_rels = grouped_rels] (Shard &local_shard) {
                    return local_shard.NodeRemoveDeleteOutgoing(external_id, grouped_rels);
                });
                futures.push_back(std::move(future));
            }

            auto p = make_shared(std::move(futures));
            return seastar::when_all(p->begin(), p->end())
            .then([p] (std::vector<seastar::future<bool>> results) {
                if (std::any_of(results.begin(), results.end(), [] (auto& f) { return f.failed(); })) {
                    return seastar::make_ready_future<bool>(false);
                }
                return seastar::make_ready_future<bool>(true);
            });
        });
    }

    seastar::future<bool> Shard::NodeRemovePeered(uint64_t external_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
            return local_shard.ValidNodeId(external_id);
        }).then([node_shard_id, external_id, this] (bool valid) {
            if (valid) {
                return NodeRemovePeeredIncoming(node_shard_id, external_id)
                .then([node_shard_id, external_id, this] (bool valid2) {
                    if (valid2) {
                        return NodeRemovePeeredOutgoing(node_shard_id, external_id);
                    }
                    return seastar::make_ready_future<bool>(false);
                }).then([external_id, node_shard_id, this] (auto valid3) {
                    if (valid3) {
                        return container().invoke_on(node_shard_id, [external_id, node_shard_id] (Shard &local_shard) {
                            return local_shard.NodeRemove(external_id);
                        });
                    }
                    return seastar::make_ready_future<bool>(false);
                });
            }
            return seastar::make_ready_future<bool>(false);
        });
    }

    seastar::future<uint16_t> Shard::NodeGetTypeIdPeered(uint64_t id) {
        return seastar::make_ready_future<uint16_t>(NodeGetTypeId(id));
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

    seastar::future<property_type_t> Shard::NodeGetPropertyPeered(const std::string &type, const std::string &key, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
            return local_shard.NodeGetProperty(type, key, property);
        });
    }

    seastar::future<property_type_t> Shard::NodeGetPropertyPeered(uint64_t id, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
            return local_shard.NodeGetProperty(id, property);
        });
    }

    seastar::future<bool> Shard::NodeSetPropertyPeered(const std::string& type, const std::string& key, const std::string& property, const property_type_t& value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
            return local_shard.NodeGetProperty(type, key, property, value);
        });
    }

    seastar::future<bool> Shard::NodeSetPropertyPeered(uint64_t id, const std::string& property, const property_type_t& value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
            return local_shard.NodeSetProperty(id, property, value);
        });
    }

    seastar::future<bool> Shard::NodeSetPropertyFromJsonPeered(const std::string &type, const std::string &key, const std::string &property, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
            return local_shard.NodeSetPropertyFromJson(type, key, property, value);
        });
    }

    seastar::future<bool> Shard::NodeSetPropertyFromJsonPeered(uint64_t id, const std::string &property, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
            return local_shard.NodeSetPropertyFromJson(id, property, value);
        });
    }

    seastar::future<bool> Shard::NodeDeletePropertyPeered(const std::string &type, const std::string &key, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
            return local_shard.NodeDeleteProperty(type, key, property);
        });
    }

    seastar::future<bool> Shard::NodeDeletePropertyPeered(uint64_t id, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
            return local_shard.NodeDeleteProperty(id, property);
        });
    }

    seastar::future<std::map<std::string, property_type_t>> Shard::NodeGetPropertiesPeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodeGetProperties(type, key);
        });
    }

    seastar::future<std::map<std::string, property_type_t>> Shard::NodeGetPropertiesPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGetProperties(id);
        });
    }

    seastar::future<bool> Shard::NodeSetPropertiesFromJsonPeered(const std::string &type, const std::string &key, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) {
            return local_shard.NodeSetPropertiesFromJson(type, key, value);
        });
    }

    seastar::future<bool> Shard::NodeSetPropertiesFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
            return local_shard.NodeSetPropertiesFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::NodeResetPropertiesFromJsonPeered(const std::string &type, const std::string &key, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) {
            return local_shard.NodeResetPropertiesFromJson(type, key, value);
        });
    }

    seastar::future<bool> Shard::NodeResetPropertiesFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
            return local_shard.NodeResetPropertiesFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::NodeDeletePropertiesPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodeDeleteProperties(type, key);
        });
    }

    seastar::future<bool> Shard::NodeDeletePropertiesPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeDeleteProperties(id);
        });
    }

}
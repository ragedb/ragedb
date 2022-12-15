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
#include "../../Templates.h"

namespace ragedb {

    seastar::future<std::map<std::string, uint64_t>> Shard::NodesGetIdsPeered(const std::string& type, const std::vector<std::string>& keys) {
        std::map<uint16_t, std::vector<std::string>> sharded_nodes_keys = PartitionNodesByNodeKeys(type, keys);
        uint16_t type_id = node_types.getTypeId(type);
        std::vector<seastar::future<std::map<std::string, uint64_t>>> futures;
        for (const auto& [their_shard, grouped_node_keys] : sharded_nodes_keys ) {
            co_await seastar::coroutine::maybe_yield();
            for (const auto& chunk : chunkVector(grouped_node_keys, 10000)) {
                auto future = container().invoke_on(their_shard, [grouped_node_keys = chunk, type_id](Shard &local_shard) {
                    return local_shard.NodesGetIds(type_id, grouped_node_keys);
                });
                futures.push_back(std::move(future));
            }
        }

        auto p = make_shared(std::move(futures));
        std::vector<std::map<std::string, uint64_t>> results = co_await seastar::when_all_succeed(p->begin(), p->end());
        std::map<std::string, uint64_t> combined;

        for(const std::map<std::string, uint64_t>& sharded : results) {
            combined.insert(std::begin(sharded), std::end(sharded));
        }
        co_return combined;
    }

    seastar::future<std::vector<Node>> Shard::NodesGetPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::vector<Node>>> futures;
        for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                return local_shard.NodesGet(grouped_node_ids);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
            std::vector<Node> combined;

            for(const std::vector<Node>& sharded : results) {
                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
            }
            return combined;
        });
    }

    seastar::future<std::vector<Node>> Shard::NodesGetPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByShardId(links);

      std::vector<seastar::future<std::vector<Node>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGet(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
        std::vector<Node> combined;

        for(const std::vector<Node>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<uint64_t, std::string>> Shard::NodesGetKeyPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, std::string>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetKey(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<uint64_t, std::string>>& results) {
        std::map<uint64_t, std::string> combined;

        for(const std::map<uint64_t, std::string>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::string>> Shard::NodesGetKeyPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::string>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetKey(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::string>>& results) {
        std::map<Link, std::string> combined;

        for(const std::map<Link, std::string>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<uint64_t, std::string>> Shard::NodesGetTypePeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, std::string>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetType(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<uint64_t, std::string>>& results) {
        std::map<uint64_t, std::string> combined;

        for(const std::map<uint64_t, std::string>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::string>> Shard::NodesGetTypePeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::string>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetType(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::string>>& results) {
        std::map<Link, std::string> combined;

        for(const std::map<Link, std::string>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<uint64_t, property_type_t>> Shard::NodesGetPropertyPeered(const std::vector<uint64_t> &ids, const std::string& property) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, property_type_t>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, property] (Shard &local_shard) {
          return local_shard.NodesGetProperty(grouped_node_ids, property);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<uint64_t, property_type_t>>& results) {
        std::map<uint64_t, property_type_t> combined;

        for(const std::map<uint64_t, property_type_t>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, property_type_t>> Shard::NodesGetPropertyPeered(const std::vector<Link>& links, const std::string& property) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, property_type_t>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, property] (Shard &local_shard) {
          return local_shard.NodesGetProperty(grouped_node_ids, property);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, property_type_t>>& results) {
        std::map<Link, property_type_t> combined;

        for(const std::map<Link, property_type_t>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<uint64_t, std::map<std::string, property_type_t>>> Shard::NodesGetPropertiesPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, std::map<std::string, property_type_t>>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetProperties(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<uint64_t, std::map<std::string, property_type_t>>>& results) {
        std::map<uint64_t, std::map<std::string, property_type_t>> combined;

        for(const std::map<uint64_t, std::map<std::string, property_type_t>>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::map<std::string, property_type_t>>> Shard::NodesGetPropertiesPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::map<std::string, property_type_t>>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetProperties(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::map<std::string, property_type_t>>>& results) {
        std::map<Link, std::map<std::string, property_type_t>> combined;

        for(const std::map<Link, std::map<std::string, property_type_t>>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<uint64_t, std::vector<Node>>> Shard::NodeIdsGetNeighborsPeered(const std::vector<uint64_t>& ids) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        // Steps:
        // 1 - Partition the nodes by their shard id
        // 2 - Go to each shard and get the Links of each node
        // I don't want to get nodes multiple times, so group them
        // 3 - call NodesGetPeered, create map of node_id, vector position
        // 4 - For each node Id, create a vector of their results
        // 5 - gather and return

        std::vector<seastar::future<std::map<uint64_t, std::vector<Link>>>> futures;
        for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                std::map<uint64_t, std::vector<Link>> node_links;
                for (const auto& id : grouped_node_ids) {
                    node_links.emplace(id, local_shard.NodeGetLinks(id));
                }
                return node_links;
            });
            futures.push_back(std::move(future));
        }

        // I now have the links of each Node, need to combine them and go get them.
        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, sharded_nodes_ids, this] (const std::vector<std::map<uint64_t, std::vector<Link>>> &maps_of_links) {
            std::vector<uint64_t> combined_ids;

            for (const auto& map_of_links: maps_of_links) {
                for (const auto& [node_id, links] : map_of_links) {
                    for (const auto& link : links) {
                        combined_ids.emplace_back(link.node_id);
                    }
                }
            }
            // Make the node ids unique to avoid getting them multiple times
            std::unordered_set<uint64_t> s;
            for (const auto& id : combined_ids) {
                s.insert(id);
            }
            combined_ids.assign(s.begin(), s.end());
            sort(combined_ids.begin(), combined_ids.end());

            return NodesGetPeered(combined_ids).then([maps_of_links](const std::vector<Node> &nodes) {
                std::map<uint64_t, std::vector<Node>> results;
                std::map<uint64_t, int> map_of_node_positions;
                for (auto i = 0; i < nodes.size(); i++) {
                    map_of_node_positions.emplace(nodes[i].getId(), i);
                }

                for (const auto& map_of_links: maps_of_links) {
                    for (const auto& [node_id, links] : map_of_links) {
                        std::vector<Node> my_nodes;
                        my_nodes.reserve(links.size());
                        for (const auto& link : links) {
                            my_nodes.emplace_back(nodes[map_of_node_positions[link.node_id]]);
                        }
                        results.emplace(node_id, my_nodes);
                    }
                }

                return results;
            });
        });
    }
    seastar::future<std::map<uint64_t, std::vector<Node>>> Shard::NodeIdsGetNeighborsPeered(const std::vector<uint64_t>& ids, Direction direction) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::map<uint64_t, std::vector<Link>>>> futures;
        for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [direction, grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                std::map<uint64_t, std::vector<Link>> node_links;
                for (const auto& id : grouped_node_ids) {
                    node_links.emplace(id, local_shard.NodeGetLinks(id, direction));
                }
                return node_links;
            });
            futures.push_back(std::move(future));
        }

        // I now have the links of each Node, need to combine them and go get them.
        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, sharded_nodes_ids, this] (const std::vector<std::map<uint64_t, std::vector<Link>>> &maps_of_links) {
            std::vector<uint64_t> combined_ids;

            for (const auto& map_of_links: maps_of_links) {
                for (const auto& [node_id, links] : map_of_links) {
                    for (const auto& link : links) {
                        combined_ids.emplace_back(link.node_id);
                    }
                }
            }
            // Make the node ids unique to avoid getting them multiple times
            std::unordered_set<uint64_t> s;
            for (const auto& id : combined_ids) {
                s.insert(id);
            }
            combined_ids.assign(s.begin(), s.end());
            sort(combined_ids.begin(), combined_ids.end());

            return NodesGetPeered(combined_ids).then([maps_of_links](const std::vector<Node> &nodes) {
                std::map<uint64_t, std::vector<Node>> results;
                std::map<uint64_t, int> map_of_node_positions;
                for (auto i = 0; i < nodes.size(); i++) {
                    map_of_node_positions.emplace(nodes[i].getId(), i);
                }

                for (const auto& map_of_links: maps_of_links) {
                    for (const auto& [node_id, links] : map_of_links) {
                        std::vector<Node> my_nodes;
                        my_nodes.reserve(links.size());
                        for (const auto& link : links) {
                            my_nodes.emplace_back(nodes[map_of_node_positions[link.node_id]]);
                        }
                        results.emplace(node_id, my_nodes);
                    }
                }

                return results;
            });
        });
    }
    seastar::future<std::map<uint64_t, std::vector<Node>>> Shard::NodeIdsGetNeighborsPeered(const std::vector<uint64_t>& ids, Direction direction, const std::string& rel_type) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::map<uint64_t, std::vector<Link>>>> futures;
        for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [direction, rel_type, grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                std::map<uint64_t, std::vector<Link>> node_links;
                for (const auto& id : grouped_node_ids) {
                    node_links.emplace(id, local_shard.NodeGetLinks(id, direction, rel_type));
                }
                return node_links;
            });
            futures.push_back(std::move(future));
        }

        // I now have the links of each Node, need to combine them and go get them.
        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, sharded_nodes_ids, this] (const std::vector<std::map<uint64_t, std::vector<Link>>> &maps_of_links) {
            std::vector<uint64_t> combined_ids;

            for (const auto& map_of_links: maps_of_links) {
                for (const auto& [node_id, links] : map_of_links) {
                    for (const auto& link : links) {
                        combined_ids.emplace_back(link.node_id);
                    }
                }
            }
            // Make the node ids unique to avoid getting them multiple times
            std::unordered_set<uint64_t> s;
            for (const auto& id : combined_ids) {
                s.insert(id);
            }
            combined_ids.assign(s.begin(), s.end());
            sort(combined_ids.begin(), combined_ids.end());

            return NodesGetPeered(combined_ids).then([maps_of_links](const std::vector<Node> &nodes) {
                std::map<uint64_t, std::vector<Node>> results;
                std::map<uint64_t, int> map_of_node_positions;
                for (auto i = 0; i < nodes.size(); i++) {
                    map_of_node_positions.emplace(nodes[i].getId(), i);
                }

                for (const auto& map_of_links: maps_of_links) {
                    for (const auto& [node_id, links] : map_of_links) {
                        std::vector<Node> my_nodes;
                        my_nodes.reserve(links.size());
                        for (const auto& link : links) {
                            my_nodes.emplace_back(nodes[map_of_node_positions[link.node_id]]);
                        }
                        results.emplace(node_id, my_nodes);
                    }
                }

                return results;
            });
        });
    }
    seastar::future<std::map<uint64_t, std::vector<Node>>> Shard::NodeIdsGetNeighborsPeered(const std::vector<uint64_t>& ids, Direction direction, const std::vector<std::string> &rel_types) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::map<uint64_t, std::vector<Link>>>> futures;
        for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [direction, rel_types, grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                std::map<uint64_t, std::vector<Link>> node_links;
                for (const auto& id : grouped_node_ids) {
                    node_links.emplace(id, local_shard.NodeGetLinks(id, direction, rel_types));
                }
                return node_links;
            });
            futures.push_back(std::move(future));
        }

        // I now have the links of each Node, need to combine them and go get them.
        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, sharded_nodes_ids, this] (const std::vector<std::map<uint64_t, std::vector<Link>>> &maps_of_links) {
            std::vector<uint64_t> combined_ids;

            for (const auto& map_of_links: maps_of_links) {
                for (const auto& [node_id, links] : map_of_links) {
                    for (const auto& link : links) {
                        combined_ids.emplace_back(link.node_id);
                    }
                }
            }
            // Make the node ids unique to avoid getting them multiple times
            std::unordered_set<uint64_t> s;
            for (const auto& id : combined_ids) {
                s.insert(id);
            }
            combined_ids.assign(s.begin(), s.end());
            sort(combined_ids.begin(), combined_ids.end());

            return NodesGetPeered(combined_ids).then([maps_of_links](const std::vector<Node> &nodes) {
                std::map<uint64_t, std::vector<Node>> results;
                std::map<uint64_t, int> map_of_node_positions;
                for (auto i = 0; i < nodes.size(); i++) {
                    map_of_node_positions.emplace(nodes[i].getId(), i);
                }

                for (const auto& map_of_links: maps_of_links) {
                    for (const auto& [node_id, links] : map_of_links) {
                        std::vector<Node> my_nodes;
                        my_nodes.reserve(links.size());
                        for (const auto& link : links) {
                            my_nodes.emplace_back(nodes[map_of_node_positions[link.node_id]]);
                        }
                        results.emplace(node_id, my_nodes);
                    }
                }

                return results;
            });
        });
    }
}
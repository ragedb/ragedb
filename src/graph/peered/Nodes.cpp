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

    std::map<uint16_t, std::vector<uint64_t>> Shard::PartitionNodeIdsByNodeShardId(const std::vector<uint64_t> &ids) const {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }
      for (auto id : ids) {
        sharded_nodes_ids[CalculateShardId(id)].emplace_back(id);
      }

      for (int i = 0; i < cpus; i++) {
        if (sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }
      return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::PartitionNodeIdsByNodeShardId(const std::vector<Link>& links) const {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }
      for (auto link : links) {
        sharded_nodes_ids[CalculateShardId(link.node_id)].emplace_back(link.node_id);
      }

      for (int i = 0; i < cpus; i++) {
        if (sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }
      return sharded_nodes_ids;
    }

    seastar::future<std::vector<Node>> Shard::NodesGetPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(ids);

      std::vector<seastar::future<std::vector<Node>>> futures;
        for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
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
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(links);

      std::vector<seastar::future<std::vector<Node>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
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

    seastar::future<std::vector<std::string>> Shard::NodesGetKeyPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(ids);

      std::vector<seastar::future<std::vector<std::string>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetKey(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<std::string>>& results) {
        std::vector<std::string> combined;

        for(const std::vector<std::string>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<std::string>> Shard::NodesGetKeyPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(links);

      std::vector<seastar::future<std::vector<std::string>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetKey(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<std::string>>& results) {
        std::vector<std::string> combined;

        for(const std::vector<std::string>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<std::string>> Shard::NodesGetTypePeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(ids);

      std::vector<seastar::future<std::vector<std::string>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetType(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<std::string>>& results) {
        std::vector<std::string> combined;

        for(const std::vector<std::string>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<std::string>> Shard::NodesGetTypePeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(links);

      std::vector<seastar::future<std::vector<std::string>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetType(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<std::string>>& results) {
        std::vector<std::string> combined;

        for(const std::vector<std::string>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<uint16_t>> Shard::NodesGetTypeIdPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(ids);

      std::vector<seastar::future<std::vector<uint16_t>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetTypeId(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint16_t>>& results) {
        std::vector<uint16_t> combined;

        for(const std::vector<uint16_t>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<uint16_t>> Shard::NodesGetTypeIdPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(links);

      std::vector<seastar::future<std::vector<uint16_t>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetTypeId(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint16_t>>& results) {
        std::vector<uint16_t> combined;

        for(const std::vector<uint16_t>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<property_type_t>> Shard::NodesGetPropertyPeered(const std::vector<uint64_t> &ids, const std::string& property) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(ids);

      std::vector<seastar::future<std::vector<property_type_t>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, property] (Shard &local_shard) {
          return local_shard.NodesGetProperty2(grouped_node_ids, property);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<property_type_t>>& results) {
        std::vector<property_type_t> combined;

        for(const std::vector<property_type_t>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<std::any>> Shard::NodesGetPropertyPeered(const std::vector<Link>& links, const std::string& property) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(links);

      std::vector<seastar::future<std::vector<std::any>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, property] (Shard &local_shard) {
          return local_shard.NodesGetProperty(grouped_node_ids, property);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<std::any>>& results) {
        std::vector<std::any> combined;

        for(const std::vector<std::any>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<std::map<std::string, std::any>>> Shard::NodesGetPropertiesPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(ids);

      std::vector<seastar::future<std::vector<std::map<std::string, std::any>>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetProperties(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<std::map<std::string, std::any>>>& results) {
        std::vector<std::map<std::string, std::any>> combined;

        for(const std::vector<std::map<std::string, std::any>>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::vector<std::map<std::string, std::any>>> Shard::NodesGetPropertiesPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByNodeShardId(links);

      std::vector<seastar::future<std::vector<std::map<std::string, std::any>>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetProperties(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<std::map<std::string, std::any>>>& results) {
        std::vector<std::map<std::string, std::any>> combined;

        for(const std::vector<std::map<std::string, std::any>>& sharded : results) {
          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

}
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

    seastar::future<std::vector<Node>> Shard::NodesGetPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

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
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionNodeIdsByShardId(links);

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

    seastar::future<std::map<uint64_t, std::string>> Shard::NodesGetKeyPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, std::string>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
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
      for (auto const& [their_shard, grouped_node_ids] : sharded_links ) {
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
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
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
      for (auto const& [their_shard, grouped_node_ids] : sharded_links ) {
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

    seastar::future<std::map<uint64_t, uint16_t>> Shard::NodesGetTypeIdPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, uint16_t>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetTypeId(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<uint64_t, uint16_t>>& results) {
        std::map<uint64_t, uint16_t> combined;

        for(const std::map<uint64_t, uint16_t>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, uint16_t>> Shard::NodesGetTypeIdPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, uint16_t>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetTypeId(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, uint16_t>>& results) {
        std::map<Link, uint16_t> combined;

        for(const std::map<Link, uint16_t>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<uint64_t, property_type_t>> Shard::NodesGetPropertyPeered(const std::vector<uint64_t> &ids, const std::string& property) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, property_type_t>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
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
      for (auto const& [their_shard, grouped_node_ids] : sharded_links ) {
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

    seastar::future<std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>>> Shard::NodesGetPropertiesPeered(const std::vector<uint64_t> &ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetProperties(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>>>& results) {
        std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>> combined;

        for(const std::map<uint64_t, std::map<std::string, property_type_t, std::less<>>>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

    seastar::future<std::map<Link, std::map<std::string, property_type_t, std::less<>>>> Shard::NodesGetPropertiesPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByNodeShardId(links);

      std::vector<seastar::future<std::map<Link, std::map<std::string, property_type_t, std::less<>>>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
          return local_shard.NodesGetProperties(grouped_node_ids);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::map<Link, std::map<std::string, property_type_t, std::less<>>>>& results) {
        std::map<Link, std::map<std::string, property_type_t, std::less<>>> combined;

        for(const std::map<Link, std::map<std::string, property_type_t, std::less<>>>& sharded : results) {
          combined.insert(std::begin(sharded), std::end(sharded));
        }
        return combined;
      });
    }

}
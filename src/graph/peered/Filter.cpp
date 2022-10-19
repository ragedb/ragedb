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

  seastar::future<uint64_t> Shard::FilterNodeCountPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<uint64_t>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type, property, operation, value] (Shard &local_shard) {
          return local_shard.FilterNodeCount(grouped_node_ids, type, property, operation, value);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
        uint64_t count = 0;

        for(auto sharded : results) {
          count += sharded;
        }
        return count;
      });
  }

  seastar::future<std::vector<uint64_t>> Shard::FilterNodeIdsPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<uint64_t>>> futures;
    for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type, property, operation, value, max, sortOrder] (Shard &local_shard) {
        return local_shard.FilterNodeIds(grouped_node_ids, type, property, operation, value, max, sortOrder);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<uint64_t>>& results) {
      uint64_t current = 0;

      std::vector<uint64_t> node_ids;

      for (const auto& result : results) {
        for( auto id : result) {
          if (++current > skip) {
              node_ids.push_back(id);
          }
          if (current >= max) {
            return node_ids;
          }
        }
      }
      return node_ids;
    });
  }

  seastar::future<std::vector<Node>> Shard::FilterNodesPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<Node>>> futures;
    for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type, property, operation, value, max, sortOrder] (Shard &local_shard) {
        return local_shard.FilterNodes(grouped_node_ids, type, property, operation, value, max, sortOrder);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<Node>>& results) {
      uint64_t current = 0;

      std::vector<Node> nodes;

      for (const auto& result : results) {
        for(const auto& node : result) {
          if (++current > skip) {
            nodes.push_back(node);
          }
          if (current >= max) {
            return nodes;
          }
        }
      }
      return nodes;
    });
  }

  seastar::future<std::vector<std::map<std::string, property_type_t>>> Shard::FilterNodePropertiesPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      uint64_t max = skip + limit;

      std::vector<seastar::future<std::vector<std::map<std::string, property_type_t>>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
          auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type, property, operation, value, max, sortOrder] (Shard &local_shard) {
              return local_shard.FilterNodeProperties(grouped_node_ids, type, property, operation, value, max, sortOrder);
          });
          futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max, property, sortOrder] (const std::vector<std::vector<std::map<std::string, property_type_t>>>& results) {

          std::vector<std::map<std::string, property_type_t>> nodes;
          for (const auto& result : results) {
              for (const auto &node : result) {
                  nodes.push_back(node);
              }
          }

          // Skip more than we have
          if (skip > nodes.size()) {
              return std::vector<std::map<std::string, property_type_t>>();
          }

          // Sort the combined core results
          if(sortOrder == Sort::ASC) {
              sort(nodes.begin(), nodes.end(), [&, property](const std::map<std::string, property_type_t> &k1, const std::map<std::string, property_type_t> &k2)-> bool {
                  return k1.at(property) < k2.at(property);
              });
          }
          if(sortOrder == Sort::DESC) {
              sort(nodes.begin(), nodes.end(), [&, property](const std::map<std::string, property_type_t> &k1, const std::map<std::string, property_type_t> &k2)-> bool {
                  return k1.at(property) > k2.at(property);
              });
          }

          std::vector<std::map<std::string, property_type_t>> smaller;
          uint64_t current = 1;

          for (const auto &node : nodes) {
              if (current++ > skip) {
                  smaller.push_back(node);
              }
              if (current >= max) {
                  return smaller;
              }
          }

          return smaller;
      });
  }

  seastar::future<uint64_t> Shard::FilterRelationshipCountPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

    std::vector<seastar::future<uint64_t>> futures;
    for (auto const& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type, property, operation, value] (Shard &local_shard) {
        return local_shard.FilterRelationshipCount(grouped_relationship_ids, type, property, operation, value);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
      uint64_t count = 0;

      for(auto sharded : results) {
        count += sharded;
      }
      return count;
    });
  }

  seastar::future<std::vector<uint64_t>> Shard::FilterRelationshipIdsPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<uint64_t>>> futures;
    for (auto const& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type, property, operation, value, max] (Shard &local_shard) {
        return local_shard.FilterRelationshipIds(grouped_relationship_ids, type, property, operation, value, 0, max);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<uint64_t>>& results) {
      uint64_t current = 0;

      std::vector<uint64_t> relationship_ids;

      for (const auto& result : results) {
        for(auto id : result) {
          if (++current > skip) {
              relationship_ids.push_back(id);
          }
          if (current >= max) {
            return relationship_ids;
          }
        }
      }
      return relationship_ids;
    });
  }

  seastar::future<std::vector<Relationship>> Shard::FilterRelationshipsPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<Relationship>>> futures;
    for (auto const& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type, property, operation, value, max] (Shard &local_shard) {
        return local_shard.FilterRelationships(grouped_relationship_ids, type, property, operation, value, 0, max);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<Relationship>>& results) {
      uint64_t current = 0;

      std::vector<Relationship> relationships;

      for (const auto& result : results) {
        for(const auto& relationship : result) {
          if (++current > skip) {
            relationships.push_back(relationship);
          }
          if (current >= max) {
            return relationships;
          }
        }
      }
      return relationships;
    });
  }
}
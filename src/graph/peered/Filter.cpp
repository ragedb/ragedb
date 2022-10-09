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
    uint16_t type_id = node_types.getTypeId(type);
    return FilterNodeCountPeered(ids, type_id, property, operation, value);
  }

  seastar::future<uint64_t> Shard::FilterNodeCountPeered(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<uint64_t>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type_id, property, operation, value] (Shard &local_shard) {
          return local_shard.FilterNodeCount(grouped_node_ids, type_id, property, operation, value);
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

  seastar::future<std::vector<uint64_t>> Shard::FilterNodeIdsPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    uint16_t type_id = node_types.getTypeId(type);
    return FilterNodeIdsPeered(ids, type_id, property, operation, value, skip, limit);
  }

  seastar::future<std::vector<uint64_t>> Shard::FilterNodeIdsPeered(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<uint64_t>>> futures;
    for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type_id, property, operation, value, max] (Shard &local_shard) {
        return local_shard.FilterNodeIds(grouped_node_ids, type_id, property, operation, value, 0, max);
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

  seastar::future<std::vector<Node>> Shard::FilterNodesPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    uint16_t type_id = node_types.getTypeId(type);
    return FilterNodesPeered(ids, type_id, property, operation, value, skip, limit);
  }

  seastar::future<std::vector<Node>> Shard::FilterNodesPeered(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<Node>>> futures;
    for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type_id, property, operation, value, max] (Shard &local_shard) {
        return local_shard.FilterNodes(grouped_node_ids, type_id, property, operation, value, 0, max);
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

  seastar::future<std::vector<std::map<std::string, property_type_t>>> Shard::FilterNodePropertiesPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
      uint16_t type_id = node_types.getTypeId(type);
      return FilterNodePropertiesPeered(ids, type_id, property, operation, value, skip, limit);
  }

  seastar::future<std::vector<std::map<std::string, property_type_t>>> Shard::FilterNodePropertiesPeered(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      uint64_t max = skip + limit;

      std::vector<seastar::future<std::vector<std::map<std::string, property_type_t>>>> futures;
      for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
          auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type_id, property, operation, value, max] (Shard &local_shard) {
              return local_shard.FilterNodeProperties(grouped_node_ids, type_id, property, operation, value, 0, max);
          });
          futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<std::map<std::string, property_type_t>>>& results) {
          uint64_t current = 0;

          std::vector<std::map<std::string, property_type_t>> nodes;

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

  seastar::future<uint64_t> Shard::FilterRelationshipCountPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
    uint16_t type_id = relationship_types.getTypeId(type);
    return FilterRelationshipCountPeered(ids, type_id, property, operation, value);
  }

  seastar::future<uint64_t> Shard::FilterRelationshipCountPeered(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

    std::vector<seastar::future<uint64_t>> futures;
    for (auto const& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type_id, property, operation, value] (Shard &local_shard) {
        return local_shard.FilterRelationshipCount(grouped_relationship_ids, type_id, property, operation, value);
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
    uint16_t type_id = relationship_types.getTypeId(type);
    return FilterRelationshipIdsPeered(ids, type_id, property, operation, value, skip, limit);
  }

  seastar::future<std::vector<uint64_t>> Shard::FilterRelationshipIdsPeered(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<uint64_t>>> futures;
    for (auto const& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type_id, property, operation, value, max] (Shard &local_shard) {
        return local_shard.FilterRelationshipIds(grouped_relationship_ids, type_id, property, operation, value, 0, max);
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
    uint16_t type_id = relationship_types.getTypeId(type);
    return FilterRelationshipsPeered(ids, type_id, property, operation, value, skip, limit);
  }

  seastar::future<std::vector<Relationship>> Shard::FilterRelationshipsPeered(const std::vector<uint64_t>& ids, uint16_t type_id, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<Relationship>>> futures;
    for (auto const& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type_id, property, operation, value, max] (Shard &local_shard) {
        return local_shard.FilterRelationships(grouped_relationship_ids, type_id, property, operation, value, 0, max);
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
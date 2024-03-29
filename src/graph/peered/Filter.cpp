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
      for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type, property, operation, value] (Shard &local_shard) {
          return local_shard.FilterNodeCount(grouped_node_ids, type, property, operation, value);
        });
        futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
        uint64_t count = 0;

        for(const auto& sharded : results) {
          count += sharded;
        }
        return count;
      });
  }

  seastar::future<std::vector<uint64_t>> Shard::FilterNodeIdsPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
      if (sortOrder == Sort::NONE) {
          std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

          uint64_t max = skip + limit;

          std::vector<seastar::future<std::vector<uint64_t>>> futures;
          for (const auto &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
              auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type, property, operation, value, max, sortOrder](Shard &local_shard) {
                  return local_shard.FilterNodeIds(grouped_node_ids, type, property, operation, value, max, sortOrder);
              });
              futures.push_back(std::move(future));
          }

          auto p = make_shared(std::move(futures));
          return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max](const std::vector<std::vector<uint64_t>> &results) {
              uint64_t current = 0;

              std::vector<uint64_t> node_ids;

              for (const auto& result : results) {
                  for (const auto& id : result) {
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
      } else {
          return container().local().FilterNodesPeered(ids, type, property, operation, value, skip, limit, sortOrder).then([] (std::vector<Node> nodes) {
              std::vector<uint64_t> ids;
              for (const auto &node : nodes) {
                  ids.push_back(node.getId());
              }
              return ids;
          });
      }
  }

  seastar::future<std::vector<Node>> Shard::FilterNodesPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<Node>>> futures;
    for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, type, property, operation, value, max, sortOrder] (Shard &local_shard) {
        return local_shard.FilterNodes(grouped_node_ids, type, property, operation, value, max, sortOrder);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max, property, sortOrder] (const std::vector<std::vector<Node>>& results) {

        std::vector<Node> nodes;
        for (const auto& result : results) {
            for (const auto &node : result) {
                nodes.push_back(node);
            }
        }

        // Skip more than we have
        if (skip > nodes.size()) {
            return std::vector<Node>();
        }

        // Sort the combined core results
        if(sortOrder == Sort::ASC) {
            sort(nodes.begin(), nodes.end(), [&, property](const Node &k1, const Node &k2)-> bool {
                return k1.getProperty(property) < k2.getProperty(property);
            });
        }
        if(sortOrder == Sort::DESC) {
            sort(nodes.begin(), nodes.end(), [&, property](const Node &k1, const Node &k2)-> bool {
                return k1.getProperty(property) > k2.getProperty(property);
            });
        }

        std::vector<Node> smaller;
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

  seastar::future<std::vector<std::map<std::string, property_type_t>>> Shard::FilterNodePropertiesPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

      uint64_t max = skip + limit;

      std::vector<seastar::future<std::vector<std::map<std::string, property_type_t>>>> futures;
      for (const auto& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
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
    for (const auto& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type, property, operation, value] (Shard &local_shard) {
        return local_shard.FilterRelationshipCount(grouped_relationship_ids, type, property, operation, value);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
      uint64_t count = 0;

      for (const auto& sharded : results) {
        count += sharded;
      }
      return count;
    });
  }

  seastar::future<std::vector<uint64_t>> Shard::FilterRelationshipIdsPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
      if (sortOrder == Sort::NONE) {
          std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

          uint64_t max = skip + limit;

          std::vector<seastar::future<std::vector<uint64_t>>> futures;
          for (const auto &[their_shard, grouped_relationship_ids] : sharded_relationships_ids) {
              auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type, property, operation, value, max, sortOrder](Shard &local_shard) {
                  return local_shard.FilterRelationshipIds(grouped_relationship_ids, type, property, operation, value, max, sortOrder);
              });
              futures.push_back(std::move(future));
          }

          auto p = make_shared(std::move(futures));
          return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max](const std::vector<std::vector<uint64_t>> &results) {
              uint64_t current = 0;

              std::vector<uint64_t> relationship_ids;

              for (const auto& result : results) {
                  for (const auto& id : result) {
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
      } else {
          return container().local().FilterRelationshipsPeered(ids, type, property, operation, value, skip, limit, sortOrder).then([] (std::vector<Relationship> relationships) {
              std::vector<uint64_t> ids;
              for (const auto &relationship : relationships) {
                  ids.push_back(relationship.getId());
              }
              return ids;
          });
      }
  }

  seastar::future<std::vector<Relationship>> Shard::FilterRelationshipsPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
    std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

    uint64_t max = skip + limit;

    std::vector<seastar::future<std::vector<Relationship>>> futures;
    for (const auto& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
      auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type, property, operation, value, max, sortOrder] (Shard &local_shard) {
        return local_shard.FilterRelationships(grouped_relationship_ids, type, property, operation, value, max, sortOrder);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max, property, sortOrder] (const std::vector<std::vector<Relationship>>& results) {

        std::vector<Relationship> relationships;
        for (const auto& result : results) {
            for (const auto &relationship : result) {
                relationships.push_back(relationship);
            }
        }

        // Skip more than we have
        if (skip > relationships.size()) {
            return std::vector<Relationship>();
        }

        // Sort the combined core results
        if(sortOrder == Sort::ASC) {
            sort(relationships.begin(), relationships.end(), [&, property](const Relationship &k1, const Relationship &k2)-> bool {
                return k1.getProperty(property) < k2.getProperty(property);
            });
        }
        if(sortOrder == Sort::DESC) {
            sort(relationships.begin(), relationships.end(), [&, property](const Relationship &k1, const Relationship &k2)-> bool {
                return k1.getProperty(property) > k2.getProperty(property);
            });
        }

        std::vector<Relationship> smaller;
        uint64_t current = 1;

        for (const auto &relationship : relationships) {
            if (current++ > skip) {
                smaller.push_back(relationship);
            }
            if (current >= max) {
                return smaller;
            }
        }

        return smaller;
    });
  }

  seastar::future<std::vector<std::map<std::string, property_type_t>>> Shard::FilterRelationshipPropertiesPeered(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit, Sort sortOrder) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids = PartitionIdsByShardId(ids);

      uint64_t max = skip + limit;

      std::vector<seastar::future<std::vector<std::map<std::string, property_type_t>>>> futures;
      for (const auto& [their_shard, grouped_relationship_ids] : sharded_relationships_ids ) {
          auto future = container().invoke_on(their_shard, [grouped_relationship_ids = grouped_relationship_ids, type, property, operation, value, max, sortOrder] (Shard &local_shard) {
              return local_shard.FilterRelationshipProperties(grouped_relationship_ids, type, property, operation, value, max, sortOrder);
          });
          futures.push_back(std::move(future));
      }

      auto p = make_shared(std::move(futures));
      return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max, property, sortOrder] (const std::vector<std::vector<std::map<std::string, property_type_t>>>& results) {

          std::vector<std::map<std::string, property_type_t>> relationships;
          for (const auto& result : results) {
              for (const auto &relationship : result) {
                  relationships.push_back(relationship);
              }
          }

          // Skip more than we have
          if (skip > relationships.size()) {
              return std::vector<std::map<std::string, property_type_t>>();
          }

          // Sort the combined core results
          if(sortOrder == Sort::ASC) {
              sort(relationships.begin(), relationships.end(), [&, property](const std::map<std::string, property_type_t> &k1, const std::map<std::string, property_type_t> &k2)-> bool {
                  return k1.at(property) < k2.at(property);
              });
          }
          if(sortOrder == Sort::DESC) {
              sort(relationships.begin(), relationships.end(), [&, property](const std::map<std::string, property_type_t> &k1, const std::map<std::string, property_type_t> &k2)-> bool {
                  return k1.at(property) > k2.at(property);
              });
          }

          std::vector<std::map<std::string, property_type_t>> smaller;
          uint64_t current = 1;

          for (const auto &relationship : relationships) {
              if (current++ > skip) {
                  smaller.push_back(relationship);
              }
              if (current >= max) {
                  return smaller;
              }
          }

          return smaller;
      });
  }
}
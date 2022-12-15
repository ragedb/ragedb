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

    seastar::future<std::vector<Relationship>> Shard::RelationshipsGetPeered(const std::vector<uint64_t> &ids) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationship_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::vector<Relationship>>> futures;
        for (const auto& [their_shard, grouped_relationship_ids] : sharded_relationship_ids ) {
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
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationship_ids = PartitionRelationshipIdsByShardId(links);

      std::vector<seastar::future<std::vector<Relationship>>> futures;
      for (const auto& [their_shard, grouped_relationship_ids] : sharded_relationship_ids ) {
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

    seastar::future<std::map<uint64_t, std::string>> Shard::RelationshipsGetTypePeered(const std::vector<uint64_t>& ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, std::string>>> futures;
      for (const auto& [their_shard, grouped_ids] : sharded_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_ids = grouped_ids] (Shard &local_shard) {
          return local_shard.RelationshipsGetType(grouped_ids);
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

    seastar::future<std::map<Link, std::string>> Shard::RelationshipsGetTypePeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByRelationshipShardId(links);

      std::vector<seastar::future<std::map<Link, std::string>>> futures;
      for (const auto& [their_shard, grouped_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_ids = grouped_ids] (Shard &local_shard) {
          return local_shard.RelationshipsGetType(grouped_ids);
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

    seastar::future<std::map<uint64_t, uint16_t>> Shard::RelationshipsGetTypeIdPeered(const std::vector<uint64_t>& ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, uint16_t>>> futures;
      for (const auto& [their_shard, grouped_ids] : sharded_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_ids = grouped_ids] (Shard &local_shard) {
          return local_shard.RelationshipsGetTypeId(grouped_ids);
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

    seastar::future<std::map<Link, uint16_t>> Shard::RelationshipsGetTypeIdPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByRelationshipShardId(links);

      std::vector<seastar::future<std::map<Link, uint16_t>>> futures;
      for (const auto& [their_shard, grouped_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_ids = grouped_ids] (Shard &local_shard) {
          return local_shard.RelationshipsGetTypeId(grouped_ids);
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

    seastar::future<std::map<uint64_t, property_type_t>> Shard::RelationshipsGetPropertyPeered(const std::vector<uint64_t> &ids, const std::string& property) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, property_type_t>>> futures;
      for (const auto& [their_shard, grouped_ids] : sharded_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_ids = grouped_ids, property] (Shard &local_shard) {
          return local_shard.RelationshipsGetProperty(grouped_ids, property);
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

    seastar::future<std::map<Link, property_type_t>> Shard::RelationshipsGetPropertyPeered(const std::vector<Link>& links, const std::string& property) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByRelationshipShardId(links);

      std::vector<seastar::future<std::map<Link, property_type_t>>> futures;
      for (const auto& [their_shard, grouped_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_ids = grouped_ids, property] (Shard &local_shard) {
          return local_shard.RelationshipsGetProperty(grouped_ids, property);
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

    seastar::future<std::map<uint64_t, std::map<std::string, property_type_t>>> Shard::RelationshipsGetPropertiesPeered(const std::vector<uint64_t>& ids) {
      std::map<uint16_t, std::vector<uint64_t>> sharded_ids = PartitionIdsByShardId(ids);

      std::vector<seastar::future<std::map<uint64_t, std::map<std::string, property_type_t>>>> futures;
      for (const auto& [their_shard, grouped_ids] : sharded_ids ) {
        auto future = container().invoke_on(their_shard, [grouped_ids = grouped_ids] (Shard &local_shard) {
          return local_shard.RelationshipsGetProperties(grouped_ids);
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

    seastar::future<std::map<Link, std::map<std::string, property_type_t>>> Shard::RelationshipsGetPropertiesPeered(const std::vector<Link>& links) {
      std::map<uint16_t, std::vector<Link>> sharded_links = PartitionLinksByRelationshipShardId(links);

      std::vector<seastar::future<std::map<Link, std::map<std::string, property_type_t>>>> futures;
      for (const auto& [their_shard, grouped_ids] : sharded_links ) {
        auto future = container().invoke_on(their_shard, [grouped_ids = grouped_ids] (Shard &local_shard) {
          return local_shard.RelationshipsGetProperties(grouped_ids);
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

 }
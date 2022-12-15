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

#include <rapidcsv.h>
#include "../Shard.h"

namespace ragedb {



    std::map<uint16_t, std::vector<std::string>> Shard::PartitionNodesByNodeKeys(const std::string& type, const std::vector<std::string> &keys) const {
        std::map<uint16_t, std::vector<std::string>> sharded_nodes;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes.try_emplace(i);
        }

        for (int i = 0; i < keys.size(); i++) {
            sharded_nodes[CalculateShardId(type, keys[i])].emplace_back(keys[i]);
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_nodes.at(i).empty()) {
                sharded_nodes.erase(i);
            }
        }
        return sharded_nodes;
    }

    std::map<uint16_t, std::vector<std::tuple<std::string, std::string>>> Shard::PartitionNodesByNodeTypeKeys(const std::string& type, const std::vector<std::string> &keys, const std::vector<std::string> &properties) const {
      std::map<uint16_t, std::vector<std::tuple<std::string, std::string>>> sharded_nodes;
      for (uint16_t i = 0; i < cpus; i++) {
        sharded_nodes.try_emplace(i);
      }

      for (int i = 0; i < keys.size(); i++) {
          sharded_nodes[CalculateShardId(type, keys[i])].emplace_back(keys[i], properties[i]);
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if (sharded_nodes.at(i).empty()) {
          sharded_nodes.erase(i);
        }
      }
      return sharded_nodes;
    }

    seastar::future<std::map<uint16_t, std::vector<uint64_t>>> Shard::PartitionIdsByShardId(const roaring::Roaring64Map &ids) const {
      std::map<uint16_t, std::vector<uint64_t>> sharded_ids;
      for (uint16_t i = 0; i < cpus; i++) {
        sharded_ids.try_emplace(i);
      }
      for (const auto& id : ids) {
        uint16_t id_shard_id = CalculateShardId(id);

        sharded_ids.at(id_shard_id).emplace_back(id);
        co_await seastar::coroutine::maybe_yield();
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if (sharded_ids.at(i).empty()) {
          sharded_ids.erase(i);
        }
      }
      co_return sharded_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::PartitionIdsByShardId(const std::vector<uint64_t> &ids) const {
      std::map<uint16_t, std::vector<uint64_t>> sharded_ids;
      for (uint16_t i = 0; i < cpus; i++) {
        sharded_ids.try_emplace(i);
      }
      for (const auto& id : ids) {
        uint16_t id_shard_id = CalculateShardId(id);
        // Insert Sorted
        auto it = std::upper_bound(sharded_ids.at(id_shard_id).begin(), sharded_ids.at(id_shard_id).end(), id);
        sharded_ids.at(id_shard_id).insert(it, id);
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if (sharded_ids.at(i).empty()) {
          sharded_ids.erase(i);
        }
      }
      return sharded_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::PartitionNodeIdsByShardId(const std::vector<Link>& links) const {
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (uint16_t i = 0; i < cpus; i++) {
        sharded_nodes_ids.try_emplace(i);
      }
      for (const auto& link : links) {
        uint16_t node_shard_id = CalculateShardId(link.node_id);
        // Insert Sorted
        auto it = std::upper_bound(sharded_nodes_ids.at(node_shard_id).begin(), sharded_nodes_ids.at(node_shard_id).end(), link.node_id);
        sharded_nodes_ids.at(node_shard_id).insert(it, link.node_id);
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if (sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }
      return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<Link>> Shard::PartitionLinksByNodeShardId(const std::vector<Link> &links) const {
      std::map<uint16_t, std::vector<Link>> sharded_links;
      for (uint16_t i = 0; i < cpus; i++) {
        sharded_links.insert({i, std::vector<Link>() });
      }

      for (Link link : links) {
        uint16_t node_shard_id = CalculateShardId(link.node_id);
        // Insert Sorted
        auto it = std::upper_bound(sharded_links.at(node_shard_id).begin(), sharded_links.at(node_shard_id).end(), link);
        sharded_links.at(node_shard_id).insert(it, link);
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if (sharded_links.at(i).empty()) {
          sharded_links.erase(i);
        }
      }
      return sharded_links;
    }


    std::map<uint16_t, std::vector<uint64_t>> Shard::PartitionRelationshipIdsByShardId(const std::vector<Link>& links) const {
      std::map<uint16_t, std::vector<uint64_t>> sharded_ids;
      for (uint16_t i = 0; i < cpus; i++) {
        sharded_ids.insert({i, std::vector<uint64_t>() });
      }
      for (const auto& link : links) {
        uint16_t relationship_shard_id = CalculateShardId(link.rel_id);
        // Insert Sorted
        auto it = std::upper_bound(sharded_ids.at(relationship_shard_id).begin(), sharded_ids.at(relationship_shard_id).end(), link.rel_id);
        sharded_ids.at(relationship_shard_id).insert(it, link.rel_id);
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if (sharded_ids.at(i).empty()) {
          sharded_ids.erase(i);
        }
      }
      return sharded_ids;
    }

    std::map<uint16_t, std::vector<Link>> Shard::PartitionLinksByRelationshipShardId(const std::vector<Link> &links) const {
      std::map<uint16_t, std::vector<Link>> sharded_links;
      for (uint16_t i = 0; i < cpus; i++) {
        sharded_links.insert({i, std::vector<Link>() });
      }

      for (Link link : links) {
        uint16_t rel_shard_id = CalculateShardId(link.rel_id);
        // Insert Sorted
        auto it = std::upper_bound(sharded_links.at(rel_shard_id).begin(), sharded_links.at(rel_shard_id).end(), link);
        sharded_links.at(rel_shard_id).insert(it, link);
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if (sharded_links.at(i).empty()) {
          sharded_links.erase(i);
        }
      }
      return sharded_links;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::PartitionNodeIdsByTypeId(const std::vector<uint64_t> &ids) const {
        std::map<uint16_t, std::vector<uint64_t>> partitioned_ids;
        auto max_size = node_types.getSize();
        for (uint16_t i = 0; i < max_size; i++) {
            partitioned_ids.insert({i, std::vector<uint64_t>() });
        }
        for (const auto& id : ids) {
            uint16_t type_id = externalToTypeId(id);
            // Insert Sorted
            auto it = std::upper_bound(partitioned_ids.at(type_id).begin(), partitioned_ids.at(type_id).end(), id);
            partitioned_ids.at(type_id).insert(it, id);
        }

        for (uint16_t i = 0; i < max_size; i++) {
            if (partitioned_ids.at(i).empty()) {
                partitioned_ids.erase(i);
            } else {
                sort(partitioned_ids.at(i).begin(), partitioned_ids.at(i).end());
            }
        }
        return partitioned_ids;
    }

    std::map<uint16_t, std::vector<Link>> Shard::PartitionLinkNodeIdsByTypeId(const std::vector<Link> &links) const {
        std::map<uint16_t, std::vector<Link>> partitioned_ids;
        auto max_size = node_types.getSize();
        for (uint16_t i = 0; i < max_size; i++) {
            partitioned_ids.insert({i, std::vector<Link>() });
        }
        for (const auto& link : links) {
            uint16_t type_id = externalToTypeId(link.node_id);
            // Insert Sorted
            auto it = std::upper_bound(partitioned_ids.at(type_id).begin(), partitioned_ids.at(type_id).end(), link);
            partitioned_ids.at(type_id).insert(it, link);
        }

        for (uint16_t i = 0; i < max_size; i++) {
            if (partitioned_ids.at(i).empty()) {
                partitioned_ids.erase(i);
            }
        }
        return partitioned_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::PartitionRelationshipIdsByTypeId(const std::vector<uint64_t> &ids) const {
        std::map<uint16_t, std::vector<uint64_t>> partitioned_ids;
        auto max_size = relationship_types.getSize();
        for (uint16_t i = 0; i < max_size; i++) {
            partitioned_ids.insert({i, std::vector<uint64_t>() });
        }
        for (const auto& id : ids) {
            partitioned_ids[externalToTypeId(id)].emplace_back(id);
        }

        for (uint16_t i = 0; i < max_size; i++) {
            if (partitioned_ids.at(i).empty()) {
                partitioned_ids.erase(i);
            } else {
                sort(partitioned_ids.at(i).begin(), partitioned_ids.at(i).end());
            }
        }
        return partitioned_ids;
    }

    std::map<uint16_t, std::vector<Link>> Shard::PartitionLinkRelationshipIdsByTypeId(const std::vector<Link> &links) const {
        std::map<uint16_t, std::vector<Link>> partitioned_ids;
        auto max_size = relationship_types.getSize();
        for (uint16_t i = 0; i < max_size; i++) {
            partitioned_ids.insert({i, std::vector<Link>() });
        }
        for (const auto& link : links) {
            partitioned_ids[externalToTypeId(link.rel_id)].emplace_back(link);
        }

        for (uint16_t i = 0; i < max_size; i++) {
            if (partitioned_ids.at(i).empty()) {
                partitioned_ids.erase(i);
            }
        }
        return partitioned_ids;
    }
}

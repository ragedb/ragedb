/*
 * Copyright Max De Marzi. All Rights Reserved.
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

  seastar::future<std::pair<roaring::Roaring64Map, roaring::Roaring64Map>> Shard::KHopIdsPeered(
  roaring::Roaring64Map seen, roaring::Roaring64Map next, roaring::Roaring64Map current, uint64_t hops) {
      if (hops < 1) {
          return seastar::make_ready_future<std::pair<roaring::Roaring64Map, roaring::Roaring64Map>>();
      }
      if (hops == 1) {
          current -= seen; // we remove any node ids we have seen from the current iterator
          seen |= current; // we add the current iterator to the ids we have seen.

          // Convert iterator to list of ids
          uint64_t *arr1 = new uint64_t[current.cardinality()];
          current.toUint64Array(arr1);
          std::vector<uint64_t> ids(arr1, arr1 + current.cardinality());

          return NodeIdsGetNeighborIdsPeered(ids).then([seen, next](std::map<uint64_t, std::vector<uint64_t>> id_neighbor_ids) mutable {
            for (const auto& [node_id, neighbor_ids] : id_neighbor_ids) {
                next.addMany(neighbor_ids.size(), neighbor_ids.data());
            }

            //seen |= next;
            return std::make_pair(seen, next);
          });
      } else {
          uint64_t next_hop = hops - 1;
          return KHopIdsPeered(seen, next, current, 1).then([next_hop, this] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
              return KHopIdsPeered(result.first, roaring::Roaring64Map(), result.second, next_hop);
          });
      }
  }


  seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(uint64_t id, uint64_t hops) {
      roaring::Roaring64Map seen;
      roaring::Roaring64Map next;
      roaring::Roaring64Map current;
      current.add(id);
      return KHopIdsPeered(seen, next, current, hops).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
          result.first |= result.second;  // adds "next" to "seen"
          result.first.remove(id);

          // Convert iterator to list of ids
          uint64_t *arr1 = new uint64_t[result.first.cardinality()];
          result.first.toUint64Array(arr1);
          std::vector<uint64_t> ids(arr1, arr1 + result.first.cardinality());
          return ids;
      });

    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(uint64_t id, uint64_t hops, Direction direction) {
        return seastar::make_ready_future<std::vector<uint64_t>>();
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        return seastar::make_ready_future<std::vector<uint64_t>>();
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return seastar::make_ready_future<std::vector<uint64_t>>();
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(const std::string& type, const std::string& key, uint64_t hops) {
        return NodeGetIDPeered(type, key).then([hops, this] (uint64_t id) {
            return KHopIdsPeered(id, hops);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction) {
        return NodeGetIDPeered(type, key).then([hops, direction, this] (uint64_t id) {
            return KHopIdsPeered(id, hops, direction);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) {
        return NodeGetIDPeered(type, key).then([hops, direction, rel_type, this] (uint64_t id) {
            return KHopIdsPeered(id, hops, direction, rel_type);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return NodeGetIDPeered(type, key).then([hops, direction, rel_types, this] (uint64_t id) {
            return KHopIdsPeered(id, hops, direction, rel_types);
        });
    }

    seastar::future<std::vector<Node>> Shard::KHopNodesPeered(uint64_t id, uint64_t hops) {
        return KHopIdsPeered(id, hops).then([this](std::vector<uint64_t> ids) {
            return NodesGetPeered(ids);
        });
    }

    seastar::future<std::vector<Node>> Shard::KHopNodesPeered(uint64_t id, uint64_t hops, Direction direction) {
        return KHopIdsPeered(id, hops, direction).then([this](std::vector<uint64_t> ids) {
            return NodesGetPeered(ids);
        });
    }

    seastar::future<std::vector<Node>> Shard::KHopNodesPeered(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        return KHopIdsPeered(id, hops, direction, rel_type).then([this](std::vector<uint64_t> ids) {
            return NodesGetPeered(ids);
        });
    }

    seastar::future<std::vector<Node>> Shard::KHopNodesPeered(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return KHopIdsPeered(id, hops, direction, rel_types).then([this](std::vector<uint64_t> ids) {
            return NodesGetPeered(ids);
        });
    }

    seastar::future<std::vector<Node>> Shard::KHopNodesPeered(const std::string& type, const std::string& key, uint64_t hops) {
        return NodeGetIDPeered(type, key).then([hops, this] (uint64_t id) {
            return KHopNodesPeered(id, hops);
        });
    }

    seastar::future<std::vector<Node>> Shard::KHopNodesPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction) {
        return NodeGetIDPeered(type, key).then([hops, direction, this] (uint64_t id) {
            return KHopNodesPeered(id, hops, direction);
        });
    }

    seastar::future<std::vector<Node>> Shard::KHopNodesPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type){
        return NodeGetIDPeered(type, key).then([hops, direction, rel_type, this] (uint64_t id) {
            return KHopNodesPeered(id, hops, direction, rel_type);
        });
    }

    seastar::future<std::vector<Node>> Shard::KHopNodesPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return NodeGetIDPeered(type, key).then([hops, direction, rel_types, this] (uint64_t id) {
            return KHopNodesPeered(id, hops, direction, rel_types);
        });
    }

}
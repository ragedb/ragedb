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

seastar::future<std::pair<roaring::Roaring64Map, roaring::Roaring64Map>> Shard::KHopIdsPeeredHelper(
  roaring::Roaring64Map seen, roaring::Roaring64Map current, uint64_t hops) {
    auto pair = std::pair<roaring::Roaring64Map, roaring::Roaring64Map>();
    if (hops < 1) {
        co_return pair;
    }
    if (hops == 1) {
        current -= seen; // we remove any node ids we have seen from the current iterator
        seen |= current; // we add the current iterator to the ids we have seen.
        roaring::Roaring64Map next = co_await RoaringNodeIdsGetNeighborIdsCombinedPeered(current);
        pair = std::make_pair(seen, next);
        co_return pair;
    } else {
        pair = co_await KHopIdsPeeredHelper(seen, current, 1);
        if (pair.second.cardinality() == 0) {
            co_return pair;
        }
        co_await seastar::coroutine::maybe_yield();
        pair = co_await KHopIdsPeeredHelper(pair.first, pair.second, hops - 1);
        co_return pair;
    }
}

  seastar::future<std::pair<roaring::Roaring64Map, roaring::Roaring64Map>> Shard::KHopIdsPeeredHelper(
    roaring::Roaring64Map seen, roaring::Roaring64Map current, uint64_t hops, Direction direction) {
    auto pair = std::pair<roaring::Roaring64Map, roaring::Roaring64Map>();
      if (hops < 1) {
        co_return pair;
      }
      if (hops == 1) {
          current -= seen; // we remove any node ids we have seen from the current iterator
          seen |= current; // we add the current iterator to the ids we have seen.
          roaring::Roaring64Map next = co_await RoaringNodeIdsGetNeighborIdsCombinedPeered(current, direction);
          pair = std::make_pair(seen, next);
          co_return pair;
      } else {
          pair = co_await KHopIdsPeeredHelper(seen, current, 1, direction);
          if (pair.second.cardinality() == 0) {
            co_return pair;
          }
          co_await seastar::coroutine::maybe_yield();
          pair = co_await KHopIdsPeeredHelper(pair.first, pair.second, hops - 1, direction);
          co_return pair;
      }
  }

  seastar::future<std::pair<roaring::Roaring64Map, roaring::Roaring64Map>> Shard::KHopIdsPeeredHelper(
    roaring::Roaring64Map seen, roaring::Roaring64Map current, uint64_t hops, Direction direction, const std::string& rel_type) {
      auto pair = std::pair<roaring::Roaring64Map, roaring::Roaring64Map>();
      if (hops < 1) {
          co_return pair;
      }
      if (hops == 1) {
          current -= seen; // we remove any node ids we have seen from the current iterator
          seen |= current; // we add the current iterator to the ids we have seen.

          roaring::Roaring64Map next = co_await RoaringNodeIdsGetNeighborIdsCombinedPeered(current, direction, rel_type);
          pair = std::make_pair(seen, next);
          co_return pair;
      } else {
          pair = co_await KHopIdsPeeredHelper(seen, current, 1, direction, rel_type);
          if (pair.second.cardinality() == 0) {
            co_return pair;
          }
          co_await seastar::coroutine::maybe_yield();
          pair = co_await KHopIdsPeeredHelper(pair.first, pair.second, hops - 1, direction, rel_type);
          co_return pair;
      }
  }

  seastar::future<std::pair<roaring::Roaring64Map, roaring::Roaring64Map>> Shard::KHopIdsPeeredHelper(
    roaring::Roaring64Map seen, roaring::Roaring64Map current, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
      auto pair = std::pair<roaring::Roaring64Map, roaring::Roaring64Map>();
      if (hops < 1) {
          co_return pair;
      }
      if (hops == 1) {
          current -= seen; // we remove any node ids we have seen from the current iterator
          seen |= current; // we add the current iterator to the ids we have seen.

          roaring::Roaring64Map next = co_await RoaringNodeIdsGetNeighborIdsCombinedPeered(current, direction, rel_types);
          pair = std::make_pair(seen, next);
          co_return pair;
      } else {
          pair = co_await KHopIdsPeeredHelper(seen, current, 1, direction, rel_types);
          if (pair.second.cardinality() == 0) {
            co_return pair;
          }
          co_await seastar::coroutine::maybe_yield();
          pair = co_await KHopIdsPeeredHelper(pair.first, pair.second, hops - 1, direction, rel_types);
          co_return pair;
      }
  }

  seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(uint64_t id, uint64_t hops) {
      roaring::Roaring64Map current;
      current.add(id);
      return KHopIdsPeeredHelper(roaring::Roaring64Map(), current, hops).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
          result.first |= result.second;  // adds "next" to "seen"
          result.first.remove(id);

          // Convert iterator to list of ids
          std::vector<uint64_t> ids;
          ids.resize(result.first.cardinality());
          uint64_t* arr = ids.data();
          result.first.toUint64Array(arr);
          return ids;
      });
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(uint64_t id, uint64_t hops, Direction direction) {
        roaring::Roaring64Map current;
        current.add(id);
        return KHopIdsPeeredHelper(roaring::Roaring64Map(), current, hops, direction).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
            result.first |= result.second;  // adds "next" to "seen"
            result.first.remove(id);

            // Convert iterator to list of ids
            std::vector<uint64_t> ids;
            ids.resize(result.first.cardinality());
            uint64_t* arr = ids.data();
            result.first.toUint64Array(arr);
            return ids;
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        roaring::Roaring64Map current;
        current.add(id);
        return KHopIdsPeeredHelper(roaring::Roaring64Map(), current, hops, direction, rel_type).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
            result.first |= result.second;  // adds "next" to "seen"
            result.first.remove(id);

            // Convert iterator to list of ids
            std::vector<uint64_t> ids;
            ids.resize(result.first.cardinality());
            uint64_t* arr = ids.data();
            result.first.toUint64Array(arr);
            return ids;
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::KHopIdsPeered(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        roaring::Roaring64Map current;
        current.add(id);
        return KHopIdsPeeredHelper(roaring::Roaring64Map(), current, hops, direction, rel_types).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
            result.first |= result.second;  // adds "next" to "seen"
            result.first.remove(id);

            // Convert iterator to list of ids
            std::vector<uint64_t> ids;
            ids.resize(result.first.cardinality());
            uint64_t* arr = ids.data();
            result.first.toUint64Array(arr);
            return ids;
        });
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

    seastar::future<uint64_t> Shard::KHopCountPeered(uint64_t id, uint64_t hops) {
        roaring::Roaring64Map current;
        current.add(id);
        return KHopIdsPeeredHelper(roaring::Roaring64Map(), current, hops).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
            result.first |= result.second;  // adds "next" to "seen"
            result.first.remove(id);
            return result.first.cardinality();
        });
    }

    seastar::future<uint64_t> Shard::KHopCountPeered(uint64_t id, uint64_t hops, Direction direction) {
        roaring::Roaring64Map current;
        current.add(id);
        return KHopIdsPeeredHelper(roaring::Roaring64Map(), current, hops, direction).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
            result.first |= result.second;  // adds "next" to "seen"
            result.first.remove(id);
            return result.first.cardinality();
        });
    }

    seastar::future<uint64_t> Shard::KHopCountPeered(uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) {
        roaring::Roaring64Map current;
        current.add(id);
        return KHopIdsPeeredHelper(roaring::Roaring64Map(), current, hops, direction, rel_type).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
            result.first |= result.second;  // adds "next" to "seen"
            result.first.remove(id);
            return result.first.cardinality();
        });
    }

    seastar::future<uint64_t> Shard::KHopCountPeered(uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        roaring::Roaring64Map current;
        current.add(id);
        return KHopIdsPeeredHelper(roaring::Roaring64Map(), current, hops, direction, rel_types).then([id] (std::pair<roaring::Roaring64Map, roaring::Roaring64Map> result) {
            result.first |= result.second;  // adds "next" to "seen"
            result.first.remove(id);
            return result.first.cardinality();
        });
    }

    seastar::future<uint64_t> Shard::KHopCountPeered(const std::string& type, const std::string& key, uint64_t hops) {
        return NodeGetIDPeered(type, key).then([hops, this] (uint64_t id) {
            return KHopCountPeered(id, hops);
        });
    }

    seastar::future<uint64_t> Shard::KHopCountPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction) {
        return NodeGetIDPeered(type, key).then([hops, direction, this] (uint64_t id) {
            return KHopCountPeered(id, hops, direction);
        });
    }

    seastar::future<uint64_t> Shard::KHopCountPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string &rel_type) {
        return NodeGetIDPeered(type, key).then([hops, direction, rel_type, this] (uint64_t id) {
            return KHopCountPeered(id, hops, direction, rel_type);
        });
    }

    seastar::future<uint64_t> Shard::KHopCountPeered(const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> &rel_types) {
        return NodeGetIDPeered(type, key).then([hops, direction, rel_types, this] (uint64_t id) {
            return KHopCountPeered(id, hops, direction, rel_types);
        });
    }

}
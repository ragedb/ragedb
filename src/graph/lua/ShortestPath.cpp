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

    std::optional<Path> Shard::ShortestPathViaLua(uint64_t id, uint64_t id2, uint64_t max_hops) {
        return ShortestPathPeered(id, id2, Direction::BOTH, {}, max_hops).get0();
    }

    std::optional<Path> Shard::ShortestPathViaLua(uint64_t id, uint64_t id2, Direction direction, uint64_t max_hops) {
        return ShortestPathPeered(id, id2, direction, {}, max_hops).get0();
    }

    std::optional<Path> Shard::ShortestPathViaLua(uint64_t id, uint64_t id2, Direction direction, const std::string& rel_type, uint64_t max_hops) {
        return ShortestPathPeered(id, id2, direction, {rel_type}, max_hops).get0();
    }

    std::optional<Path> Shard::ShortestPathViaLua(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types, uint64_t max_hops) {
        return ShortestPathPeered(id, id2, direction, rel_types, max_hops).get0();
    }

    std::optional<Path> Shard::ShortestPathViaLua(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, uint64_t max_hops) {
        uint64_t id = NodeGetIDPeered(type, key).get0();
        uint64_t id2 = NodeGetIDPeered(type2, key2).get0();
        if (id == 0 || id2 == 0) {
            return std::nullopt;
        }
        return ShortestPathPeered(id, id2, Direction::BOTH, {}, max_hops).get0();
    }

    std::optional<Path> Shard::ShortestPathViaLua(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, uint64_t max_hops) {
        uint64_t id = NodeGetIDPeered(type, key).get0();
        uint64_t id2 = NodeGetIDPeered(type2, key2).get0();
        if (id == 0 || id2 == 0) {
            return std::nullopt;
        }
        return ShortestPathPeered(id, id2, direction, {}, max_hops).get0();
    }

    std::optional<Path> Shard::ShortestPathViaLua(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::string& rel_type, uint64_t max_hops) {
        uint64_t id = NodeGetIDPeered(type, key).get0();
        uint64_t id2 = NodeGetIDPeered(type2, key2).get0();
        if (id == 0 || id2 == 0) {
            return std::nullopt;
        }
        return ShortestPathPeered(id, id2, direction, {rel_type}, max_hops).get0();
    }

    std::optional<Path> Shard::ShortestPathViaLua(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::vector<std::string> &rel_types, uint64_t max_hops) {
        uint64_t id = NodeGetIDPeered(type, key).get0();
        uint64_t id2 = NodeGetIDPeered(type2, key2).get0();
        if (id == 0 || id2 == 0) {
            return std::nullopt;
        }
        return ShortestPathPeered(id, id2, direction, rel_types, max_hops).get0();
    }

    std::optional<WeightedPath> Shard::ShortestWeightedPathViaLua(uint64_t id, uint64_t id2) {
        return ShortestWeightedPathPeered(id, id2, Direction::BOTH, {}).get0();
    }

    std::optional<WeightedPath> Shard::ShortestWeightedPathViaLua(uint64_t id, uint64_t id2, Direction direction) {
        return ShortestWeightedPathPeered(id, id2, direction, {}).get0();
    }

    std::optional<WeightedPath> Shard::ShortestWeightedPathViaLua(uint64_t id, uint64_t id2, Direction direction, const std::string& rel_type) {
        return ShortestWeightedPathPeered(id, id2, direction, {rel_type}).get0();
    }

    std::optional<WeightedPath> Shard::ShortestWeightedPathViaLua(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types) {
        return ShortestWeightedPathPeered(id, id2, direction, rel_types).get0();
    }

    std::optional<WeightedPath> Shard::ShortestWeightedPathViaLua(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2) {
        uint64_t id = NodeGetIDPeered(type, key).get0();
        uint64_t id2 = NodeGetIDPeered(type2, key2).get0();
        if (id == 0 || id2 == 0) {
            return std::nullopt;
        }
        return ShortestWeightedPathPeered(id, id2, Direction::BOTH, {}).get0();
    }

    std::optional<WeightedPath> Shard::ShortestWeightedPathViaLua(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction) {
        uint64_t id = NodeGetIDPeered(type, key).get0();
        uint64_t id2 = NodeGetIDPeered(type2, key2).get0();
        if (id == 0 || id2 == 0) {
            return std::nullopt;
        }
        return ShortestWeightedPathPeered(id, id2, direction, {}).get0();
    }

    std::optional<WeightedPath> Shard::ShortestWeightedPathViaLua(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::string& rel_type) {
        uint64_t id = NodeGetIDPeered(type, key).get0();
        uint64_t id2 = NodeGetIDPeered(type2, key2).get0();
        if (id == 0 || id2 == 0) {
            return std::nullopt;
        }
        return ShortestWeightedPathPeered(id, id2, direction, {rel_type}).get0();
    }

    std::optional<WeightedPath> Shard::ShortestWeightedPathViaLua(const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetIDPeered(type, key).get0();
        uint64_t id2 = NodeGetIDPeered(type2, key2).get0();
        if (id == 0 || id2 == 0) {
            return std::nullopt;
        }
        return ShortestWeightedPathPeered(id, id2, direction, rel_types).get0();
    }

}

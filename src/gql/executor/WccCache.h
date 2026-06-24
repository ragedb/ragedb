/*
 * Copyright RageDB Contributors. All Rights Reserved.
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

#ifndef RAGEDB_WCCCACHE_H
#define RAGEDB_WCCCACHE_H

#include <string>
#include <unordered_map>
#include <map>
#include <cstdint>

namespace ragedb::gql {

/**
 * Thread-local cache for Weakly Connected Components (WCC) partition maps.
 * Avoids repeated calculations of Union-Find trees during equivalence class coalescing.
 */
class WccCache {
private:
    // Maps relationship type -> partition map (node_id -> component_id)
    std::unordered_map<std::string, std::map<uint64_t, uint64_t>> cache;

public:
    static WccCache& local() {
        thread_local WccCache instance;
        return instance;
    }

    void clear() {
        cache.clear();
    }

    void invalidate(const std::string& rel_type) {
        cache.erase(rel_type);
    }

    bool has(const std::string& rel_type) const {
        return cache.find(rel_type) != cache.end();
    }

    const std::map<uint64_t, uint64_t>& get(const std::string& rel_type) const {
        return cache.at(rel_type);
    }

    void set(const std::string& rel_type, std::map<uint64_t, uint64_t>&& partitions) {
        cache[rel_type] = std::move(partitions);
    }
};

} // namespace ragedb::gql

#endif // RAGEDB_WCCCACHE_H

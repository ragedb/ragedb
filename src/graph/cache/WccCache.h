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
#include <memory>
#include <cstdint>

namespace ragedb::gql {

/**
 * Thread-local cache for Weakly Connected Components (WCC) partition maps.
 * Avoids repeated calculations of Union-Find trees during equivalence class coalescing.
 * Entries are immutable shared snapshots: readers hold a shared_ptr instead of copying the
 * (potentially O(V)) map per query row, and invalidation just drops the cache's reference
 * while in-flight readers keep theirs.
 *
 * Footprint: one O(V) entry per distinct relationship type queried, per shard. Real schemas
 * have few relationship types; kMaxEntries is only a runaway guard against a pathological
 * stream of distinct type strings, and entries are re-derived on a miss.
 */
class WccCache {
public:
    using PartitionMap = std::map<uint64_t, uint64_t>;

private:
    // Maps relationship type -> partition map (node_id -> component_id)
    std::unordered_map<std::string, std::shared_ptr<const PartitionMap>> cache;
    static constexpr size_t kMaxEntries = 64;

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

    // Returns the cached snapshot, or nullptr if absent.
    std::shared_ptr<const PartitionMap> get(const std::string& rel_type) const {
        auto it = cache.find(rel_type);
        return it != cache.end() ? it->second : nullptr;
    }

    std::shared_ptr<const PartitionMap> set(const std::string& rel_type, PartitionMap&& partitions) {
        if (cache.size() >= kMaxEntries && cache.find(rel_type) == cache.end()) {
            cache.clear();
        }
        auto snapshot = std::make_shared<const PartitionMap>(std::move(partitions));
        cache[rel_type] = snapshot;
        return snapshot;
    }
};

} // namespace ragedb::gql

#endif // RAGEDB_WCCCACHE_H

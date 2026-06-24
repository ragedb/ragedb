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

#ifndef RAGEDB_TRANSITIVEREACHABILITYCACHE_H
#define RAGEDB_TRANSITIVEREACHABILITYCACHE_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <cstdint>

namespace ragedb::gql {

/**
 * Thread-local cache for Transitive Reachability descendant sets.
 * Keyed by relationship type. Avoids repeated calculations of transitive closures.
 */
class TransitiveReachabilityCache {
private:
    // Maps relationship type -> descendants map (node_id -> set of reachable descendant node_ids)
    std::unordered_map<std::string, std::map<uint64_t, std::unordered_set<uint64_t>>> cache;

public:
    static TransitiveReachabilityCache& local() {
        thread_local TransitiveReachabilityCache instance;
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

    const std::map<uint64_t, std::unordered_set<uint64_t>>& get(const std::string& rel_type) const {
        return cache.at(rel_type);
    }

    void set(const std::string& rel_type, std::map<uint64_t, std::unordered_set<uint64_t>>&& descendants) {
        cache[rel_type] = std::move(descendants);
    }
};

} // namespace ragedb::gql

#endif // RAGEDB_TRANSITIVEREACHABILITYCACHE_H

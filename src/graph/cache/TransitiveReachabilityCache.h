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
#include <memory>
#include <vector>
#include <cstdint>

namespace ragedb::gql {

/**
 * Thread-local cache for Transitive Reachability descendant sets.
 * Keyed by relationship type. Avoids repeated calculations of transitive closures.
 *
 * Footprint: one entry per distinct relationship type queried, per shard. Each entry is a
 * shared snapshot of the closure -- up to O(V^2) for a densely reachable graph. Real schemas
 * have a handful of relationship types, so the natural bound is small; kMaxEntries is only a
 * runaway guard against a pathological stream of distinct type strings. Entries are re-derived
 * on a miss, so dropping them on overflow costs a recompute, never correctness.
 */
class TransitiveReachabilityCache {
public:
    using DescendantsMap = std::map<uint64_t, std::unordered_set<uint64_t>>;

    // Precomputed candidate id lists for an unconstrained (unbound, unlabelled, unfiltered) endpoint:
    // the start side is the closure's keys, the end side the union of every descendant set. Both are
    // derived purely from the descendants map, so they are computed once per relationship type and
    // reused across query rows instead of being re-unioned per row.
    struct Candidates {
        std::vector<uint64_t> key_ids;
        std::vector<uint64_t> end_ids;
    };

private:
    // Maps relationship type -> descendants map (node_id -> set of reachable descendant node_ids).
    // Entries are immutable shared snapshots: readers hold a shared_ptr instead of copying the
    // (potentially O(V^2)) map per query row, and invalidation just drops the cache's reference.
    std::unordered_map<std::string, std::shared_ptr<const DescendantsMap>> cache;
    // Derived candidate id lists, lazily built from the descendants map and cleared alongside it so
    // they can never outlive or contradict the closure they were derived from.
    std::unordered_map<std::string, std::shared_ptr<const Candidates>> candidates;
    static constexpr size_t kMaxEntries = 64;

public:
    static TransitiveReachabilityCache& local() {
        thread_local TransitiveReachabilityCache instance;
        return instance;
    }

    void clear() {
        cache.clear();
        candidates.clear();
    }

    void invalidate(const std::string& rel_type) {
        cache.erase(rel_type);
        candidates.erase(rel_type);
    }

    bool has(const std::string& rel_type) const {
        return cache.find(rel_type) != cache.end();
    }

    // Returns the cached snapshot, or nullptr if absent.
    std::shared_ptr<const DescendantsMap> get(const std::string& rel_type) const {
        auto it = cache.find(rel_type);
        return it != cache.end() ? it->second : nullptr;
    }

    std::shared_ptr<const DescendantsMap> set(const std::string& rel_type, DescendantsMap&& descendants) {
        if (cache.size() >= kMaxEntries && cache.find(rel_type) == cache.end()) {
            cache.clear();
            candidates.clear();
        }
        candidates.erase(rel_type);  // stale relative to the new closure
        auto snapshot = std::make_shared<const DescendantsMap>(std::move(descendants));
        cache[rel_type] = snapshot;
        return snapshot;
    }

    // Returns the unconstrained-endpoint candidate id lists, building and caching them from the
    // closure on first request. Returns nullptr if the relationship type has no cached closure.
    std::shared_ptr<const Candidates> get_candidates(const std::string& rel_type) {
        auto it = candidates.find(rel_type);
        if (it != candidates.end()) return it->second;
        auto d = get(rel_type);
        if (!d) return nullptr;
        auto c = std::make_shared<Candidates>();
        c->key_ids.reserve(d->size());
        std::unordered_set<uint64_t> end_set;
        for (const auto& kv : *d) {
            c->key_ids.push_back(kv.first);
            for (uint64_t e : kv.second) end_set.insert(e);
        }
        c->end_ids.assign(end_set.begin(), end_set.end());
        std::shared_ptr<const Candidates> snapshot = std::move(c);
        candidates[rel_type] = snapshot;
        return snapshot;
    }
};

} // namespace ragedb::gql

#endif // RAGEDB_TRANSITIVEREACHABILITYCACHE_H

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

#include "GqlQueryCache.h"

namespace ragedb::gql {

GqlQueryCache::GqlQueryCache(size_t cap) : capacity(cap) {}

GqlQueryCache& GqlQueryCache::local() {
    static thread_local GqlQueryCache cache;
    return cache;
}

std::optional<GqlQuery> GqlQueryCache::get(const std::string& query_str) {
    auto it = cache_map.find(query_str);
    if (it == cache_map.end()) {
        return std::nullopt;
    }
    // Move referenced key to the front of lru_list
    lru_list.erase(it->second.second);
    lru_list.push_front(query_str);
    it->second.second = lru_list.begin();

    // Return a clone of the cached AST
    return it->second.first.clone();
}

void GqlQueryCache::put(const std::string& query_str, const GqlQuery& query) {
    auto it = cache_map.find(query_str);
    if (it != cache_map.end()) {
        // Update the value, move to front
        lru_list.erase(it->second.second);
        lru_list.push_front(query_str);
        it->second.first = query.clone();
        it->second.second = lru_list.begin();
    } else {
        // If capacity reached, evict LRU item (from the back)
        if (cache_map.size() >= capacity) {
            std::string lru_key = lru_list.back();
            lru_list.pop_back();
            cache_map.erase(lru_key);
        }
        lru_list.push_front(query_str);
        cache_map.emplace(query_str, std::make_pair(query.clone(), lru_list.begin()));
    }
}

void GqlQueryCache::clear() {
    lru_list.clear();
    cache_map.clear();
}

size_t GqlQueryCache::size() const {
    return cache_map.size();
}

} // namespace ragedb::gql

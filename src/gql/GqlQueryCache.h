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

#ifndef RAGEDB_GQLQUERYCACHE_H
#define RAGEDB_GQLQUERYCACHE_H

#include <string>
#include <unordered_map>
#include <list>
#include <optional>
#include "GqlAst.h"

namespace ragedb::gql {

class GqlQueryCache {
private:
    size_t capacity;
    std::list<std::string> lru_list;
    // Map stores the GqlQuery AST and its corresponding iterator in lru_list
    std::unordered_map<std::string, std::pair<GqlQuery, std::list<std::string>::iterator>> cache_map;

public:
    explicit GqlQueryCache(size_t cap = 10000);

    // Get a thread-local static instance of the cache (per Seastar shard CPU)
    static GqlQueryCache& local();

    // Look up a query from the cache, returning a clone if hit
    std::optional<GqlQuery> get(const std::string& query_str);

    // Put a query into the cache
    void put(const std::string& query_str, const GqlQuery& query);

    // Clear the cache
    void clear();

    // Check size of the cache
    size_t size() const;
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLQUERYCACHE_H

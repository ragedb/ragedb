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

#include <catch2/catch.hpp>
#include <Graph.h>
#include "../../src/gql/GqlVirtualCatalog.h"
#include "../../src/graph/cache/WccCache.h"
#include "../../src/graph/cache/TransitiveReachabilityCache.h"

using namespace ragedb;
using namespace ragedb::gql;

/*
 * Tasks 006/007: every topology mutation must drop the WCC/reachability cache entries for the
 * touched relationship type BEFORE the mutation's future resolves (read-your-writes), and types
 * without algebraic traits must skip the broadcast entirely. The caches are seeded directly so
 * these tests exercise the invalidation plumbing without needing the fast paths to run.
 */

static void seed_caches(const std::string& rel_type) {
    WccCache::local().set(rel_type, std::map<uint64_t, uint64_t>{{1, 1}});
    TransitiveReachabilityCache::local().set(rel_type, std::map<uint64_t, std::unordered_set<uint64_t>>{{1, {2}}});
    REQUIRE(WccCache::local().has(rel_type));
    REQUIRE(TransitiveReachabilityCache::local().has(rel_type));
}

TEST_CASE("topology mutations invalidate the semantic caches", "[gql_cache_invalidation]") {
    auto graph = Graph("gql_cache_invalidation_test");
    graph.Start().get();
    graph.Clear();
    GqlVirtualCatalog::local().clear();
    WccCache::local().clear();
    TransitiveReachabilityCache::local().clear();

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().RelationshipTypeInsertPeered("same_group").get();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("same_group", {"reflexive", "symmetric", "transitive"});

    uint64_t a = graph.shard.local().NodeAddPeered("Person", "alice", "{\"name\": \"Alice\"}").get();
    uint64_t b = graph.shard.local().NodeAddPeered("Person", "bob", "{\"name\": \"Bob\"}").get();

    SECTION("relationship add invalidates both caches before the write resolves") {
        seed_caches("same_group");
        uint64_t rel = graph.shard.local().RelationshipAddPeered("same_group", a, b, "{}").get();
        REQUIRE(rel > 0);
        REQUIRE_FALSE(WccCache::local().has("same_group"));
        REQUIRE_FALSE(TransitiveReachabilityCache::local().has("same_group"));
    }

    SECTION("relationship delete invalidates both caches") {
        uint64_t rel = graph.shard.local().RelationshipAddPeered("same_group", a, b, "{}").get();
        REQUIRE(rel > 0);
        seed_caches("same_group");
        REQUIRE(graph.shard.local().RelationshipRemovePeered(rel).get());
        REQUIRE_FALSE(WccCache::local().has("same_group"));
        REQUIRE_FALSE(TransitiveReachabilityCache::local().has("same_group"));
    }

    SECTION("node delete cascades relationship deletions and clears both caches") {
        uint64_t rel = graph.shard.local().RelationshipAddPeered("same_group", a, b, "{}").get();
        REQUIRE(rel > 0);
        seed_caches("same_group");
        REQUIRE(graph.shard.local().NodeRemovePeered(b).get());
        REQUIRE_FALSE(WccCache::local().has("same_group"));
        REQUIRE_FALSE(TransitiveReachabilityCache::local().has("same_group"));
    }

    SECTION("types without algebraic traits skip the invalidation broadcast") {
        graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();
        // Seed entries under the untraited type: nothing legitimate ever caches such a type, so
        // the write path may (and should) skip the broadcast for it.
        seed_caches("KNOWS");
        uint64_t rel = graph.shard.local().RelationshipAddPeered("KNOWS", a, b, "{}").get();
        REQUIRE(rel > 0);
        REQUIRE(WccCache::local().has("KNOWS"));
        REQUIRE(TransitiveReachabilityCache::local().has("KNOWS"));
        WccCache::local().clear();
        TransitiveReachabilityCache::local().clear();
    }

    GqlVirtualCatalog::local().clear();
    graph.Stop().get();
}

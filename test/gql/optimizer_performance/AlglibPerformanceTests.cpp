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

#include "PerformanceHelper.h"
#include "../../src/gql/GqlVirtualCatalog.h"

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("Alglib Performance Benchmarks", "[gql_optimizer_perf]") {
    auto graph = Graph("gql_test_alglib_perf");
    graph.Start().get();
    graph.Clear();
    PerformanceGraphStopGuard guard(graph);

    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "id", "integer").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "age", "integer").get();

    graph.shard.local().RelationshipTypeInsertPeered("knows").get();
    graph.shard.local().RelationshipTypeInsertPeered("ancestor_of").get();
    graph.shard.local().RelationshipTypeInsertPeered("parent_of").get();
    graph.shard.local().RelationshipTypeInsertPeered("part_of").get();
    graph.shard.local().RelationshipTypeInsertPeered("same_group").get();

    // Set catalog algebraic properties
    GqlVirtualCatalog::local().clear();
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("knows", {"symmetric"});
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("ancestor_of", {"transitive"});
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("parent_of", {"irreflexive"});
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("part_of", {"antisymmetric"});
    GqlVirtualCatalog::local().set_relationship_algebraic_properties("same_group", {"reflexive", "symmetric", "transitive"});

    // Setup graph nodes: 50 Person nodes
    std::vector<uint64_t> person_ids;
    for (int i = 0; i < 50; ++i) {
        std::string name = "Person" + std::to_string(i);
        uint64_t id = graph.shard.local().NodeAddPeered("Person", name, "{\"name\": \"" + name + "\", \"id\": " + std::to_string(i) + ", \"age\": " + std::to_string(20 + (i % 40)) + "}").get();
        person_ids.push_back(id);
    }

    // Add relationships
    for (int i = 0; i < 50; ++i) {
        uint64_t u = person_ids[i];
        uint64_t v = person_ids[(i + 1) % 50];
        graph.shard.local().RelationshipAddPeered("knows", u, v, "{}").get();
        graph.shard.local().RelationshipAddPeered("ancestor_of", u, v, "{}").get();
        graph.shard.local().RelationshipAddPeered("parent_of", u, v, "{}").get();
        graph.shard.local().RelationshipAddPeered("part_of", u, v, "{}").get();
        graph.shard.local().RelationshipAddPeered("same_group", u, v, "{}").get();
    }

    std::cout << "\n=========================================\n";
    std::cout << "   ALGLIB SEMANTIC OPTIMIZER BENCHMARKS\n";
    std::cout << "=========================================\n";

    std::string unique_id_str = std::to_string(person_ids[0]);

    // 1. Phase 22: Symmetric Traversal Swap
    run_bench(
        graph,
        "Phase 22: Symmetric Traversal Swap",
        "MATCH (a:Person)-[:knows]->(b:Person) WHERE b.id = " + unique_id_str + " RETURN a.name",
        "NO_SEMANTIC MATCH (a:Person)-[:knows]->(b:Person) WHERE b.id = " + unique_id_str + " RETURN a.name",
        2
    );

    // 2. Phase 23: Transitive Path Pruning
    run_bench(
        graph,
        "Phase 23: Transitive Path Pruning",
        "MATCH (x:Person)-[:ancestor_of]->(y:Person) MATCH (y:Person)-[:ancestor_of]->(z:Person) MATCH (x:Person)-[:ancestor_of]->(z:Person) WHERE x.id = 0 RETURN x.name, y.name, z.name",
        "NO_SEMANTIC MATCH (x:Person)-[:ancestor_of]->(y:Person) MATCH (y:Person)-[:ancestor_of]->(z:Person) MATCH (x:Person)-[:ancestor_of]->(z:Person) WHERE x.id = 0 RETURN x.name, y.name, z.name",
        2
    );

    // 3. Phase 24: Irreflexive Contradiction Pruning
    run_bench(
        graph,
        "Phase 24: Irreflexive Contradiction Pruning",
        "MATCH (a:Person)-[:parent_of]->(a) RETURN a.name",
        "NO_SEMANTIC MATCH (a:Person)-[:parent_of]->(a) RETURN a.name",
        2
    );

    // 4. Phase 25: Antisymmetric Loop Collapse
    run_bench(
        graph,
        "Phase 25: Antisymmetric Loop Collapse",
        "MATCH (a:Person)-[:part_of]->(b:Person)-[:part_of]->(a) WHERE a.id = 0 RETURN a.name, b.name",
        "NO_SEMANTIC MATCH (a:Person)-[:part_of]->(b:Person)-[:part_of]->(a) WHERE a.id = 0 RETURN a.name, b.name",
        2
    );

    // 5. Phase 26: Equivalence Class Coalescing
    // TRAIL is required on the unoptimised side: same_group is reflexive+symmetric, so an unbounded
    // quantifier under GQL's default WALK mode admits infinitely many walks (a walk may re-cross an edge).
    // TRAIL is what the query means -- reachability without reusing an edge -- and keeps both sides
    // comparing the same thing.
    run_bench(
        graph,
        "Phase 26: Equivalence Class Coalescing",
        "MATCH TRAIL (a:Person)-[:same_group*]->(b:Person) WHERE a.id = 0 RETURN a.name, b.name",
        "NO_SEMANTIC MATCH TRAIL (a:Person)-[:same_group*]->(b:Person) WHERE a.id = 0 RETURN a.name, b.name",
        2
    );

    std::cout << "=========================================\n";
}

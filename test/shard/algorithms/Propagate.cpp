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

#include <catch2/catch.hpp>
#include <Graph.h>
#include <map>

using namespace ragedb;

struct PropagateGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit PropagateGraphStopGuard(Graph& graph) : g(graph) {}
    ~PropagateGraphStopGuard() {
        if (!stopped) {
            try {
                g.Stop().get();
            } catch (...) {}
            stopped = true;
        }
    }
};

// Collapse the result vector into node -> {value, depth} for order-independent assertions.
static std::map<uint64_t, std::pair<double, uint32_t>> asMap(const std::vector<Shard::PropagateResult>& res) {
    std::map<uint64_t, std::pair<double, uint32_t>> m;
    for (const auto& r : res) m[r.node] = {r.value, r.depth};
    return m;
}

SCENARIO( "Shard can execute a value-propagating first-claim BFS", "[graph_propagate]" ) {
    GIVEN("A small transfer graph: A->B(5), A->C(3), B->D(10), C->D(2)") {
        auto graph = Graph("graph_propagate_test");
        graph.Start().get();
        graph.Clear();
        PropagateGraphStopGuard guard(graph);

        graph.shard.local().NodeTypeInsertPeered("Account").get();
        graph.shard.local().NodePropertyTypeAddPeered("Account", "name", "string").get();
        graph.shard.local().RelationshipTypeInsertPeered("transfer").get();
        graph.shard.local().RelationshipPropertyTypeAddPeered("transfer", "amount", "double").get();

        uint64_t idA = graph.shard.local().NodeAddPeered("Account", "A", R"({"name": "A"})").get();
        uint64_t idB = graph.shard.local().NodeAddPeered("Account", "B", R"({"name": "B"})").get();
        uint64_t idC = graph.shard.local().NodeAddPeered("Account", "C", R"({"name": "C"})").get();
        uint64_t idD = graph.shard.local().NodeAddPeered("Account", "D", R"({"name": "D"})").get();

        graph.shard.local().RelationshipAddPeered("transfer", idA, idB, R"({"amount": 5.0})").get();
        graph.shard.local().RelationshipAddPeered("transfer", idA, idC, R"({"amount": 3.0})").get();
        graph.shard.local().RelationshipAddPeered("transfer", idB, idD, R"({"amount": 10.0})").get();
        graph.shard.local().RelationshipAddPeered("transfer", idC, idD, R"({"amount": 2.0})").get();

        std::vector<std::string> rels{"transfer"};

        WHEN("Ascending fan-out order claims D through the cheaper C edge") {
            auto res = graph.shard.local().PropagateBFSPeered(
                {idA}, {100.0}, rels, Direction::OUT, 3, "amount",
                /*order_desc*/ false, /*trunc*/ 0, /*min_value*/ 0.0, "", 0, 0);
            auto m = asMap(res);
            THEN("The seed carries its own value at depth 1 and D is claimed by C's amount") {
                REQUIRE(m[idA].first == Approx(100.0));
                REQUIRE(m[idA].second == 1);
                REQUIRE(m[idC].first == Approx(3.0));
                REQUIRE(m[idC].second == 2);
                REQUIRE(m[idB].first == Approx(5.0));
                REQUIRE(m[idB].second == 2);
                REQUIRE(m[idD].first == Approx(2.0)); // C processed before B under asc, so C claims D
                REQUIRE(m[idD].second == 3);
            }
        }

        WHEN("Descending fan-out order claims D through the larger B edge") {
            auto res = graph.shard.local().PropagateBFSPeered(
                {idA}, {100.0}, rels, Direction::OUT, 3, "amount",
                /*order_desc*/ true, 0, 0.0, "", 0, 0);
            auto m = asMap(res);
            THEN("D is claimed by B's amount instead") {
                REQUIRE(m[idD].first == Approx(10.0));
                REQUIRE(m[idD].second == 3);
            }
        }

        WHEN("The depth cap stops expansion before D") {
            auto res = graph.shard.local().PropagateBFSPeered(
                {idA}, {100.0}, rels, Direction::OUT, /*max_depth*/ 2, "amount",
                false, 0, 0.0, "", 0, 0);
            auto m = asMap(res);
            THEN("Only depths 1 and 2 are reached") {
                REQUIRE(m.count(idD) == 0);
                REQUIRE(m.count(idB) == 1);
                REQUIRE(m.count(idC) == 1);
            }
        }

        WHEN("Truncation to a single edge drops the more expensive B branch under asc") {
            auto res = graph.shard.local().PropagateBFSPeered(
                {idA}, {100.0}, rels, Direction::OUT, 3, "amount",
                false, /*trunc*/ 1, 0.0, "", 0, 0);
            auto m = asMap(res);
            THEN("Only the cheapest edge from A survives, so B is never claimed") {
                REQUIRE(m.count(idC) == 1);
                REQUIRE(m.count(idB) == 0);
                REQUIRE(m[idD].first == Approx(2.0)); // D still reached via C
            }
        }

        WHEN("The exclusive min_value gate drops edges at or below the threshold") {
            auto res = graph.shard.local().PropagateBFSPeered(
                {idA}, {100.0}, rels, Direction::OUT, 3, "amount",
                false, 0, /*min_value*/ 3.0, "", 0, 0);
            auto m = asMap(res);
            THEN("C (amount 3, gated out) is skipped and D is claimed via B") {
                REQUIRE(m.count(idC) == 0);
                REQUIRE(m[idB].first == Approx(5.0));
                REQUIRE(m[idD].first == Approx(10.0)); // B->D survives, C branch gated
            }
        }

        WHEN("Two seeds accumulate inflow by sum and distance by min") {
            auto res = graph.shard.local().PropagateBFSPeered(
                {idA, idC}, {100.0, 50.0}, rels, Direction::OUT, 3, "amount",
                false, 0, 0.0, "", 0, 0);
            auto m = asMap(res);
            THEN("C sums its seed value with A's claim and D sums both seed claims") {
                REQUIRE(m[idC].first == Approx(53.0)); // 50 (seed) + 3 (claimed from A)
                REQUIRE(m[idC].second == 1);           // min(1 as seed, 2 from A)
                REQUIRE(m[idD].first == Approx(4.0));  // 2 (via A) + 2 (via C seed)
                REQUIRE(m[idD].second == 2);           // min(3 via A, 2 via C)
            }
        }
    }
}

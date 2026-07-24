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
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"
#include "../../src/gql/GqlExecutor.h"

using namespace ragedb;
using namespace ragedb::gql;

/// Stops the graph even when a test throws, so an escaping exception reports its message instead of
/// aborting in the sharded destructor.
struct FnGraphStopGuard {
    Graph& g;
    bool stopped = false;
    explicit FnGraphStopGuard(Graph& graph) : g(graph) {}
    ~FnGraphStopGuard() {
        if (!stopped) {
            try { g.Stop().get(); } catch (...) {}
            stopped = true;
        }
    }
    void stop() {
        if (!stopped) { g.Stop().get(); stopped = true; }
    }
};

/*
 * The scalar function library. Before this, only five functions existed and every other name
 * evaluated to NULL, so a query calling one returned plausible wrong answers. These are the ones the LDBC
 * suites actually call, plus the standard numeric/string/null functions.
 */
static void populate_fn_graph(Graph& graph) {
    graph.Clear();
    graph.shard.local().NodeTypeInsertPeered("Person").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "name", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "created", "string").get();
    graph.shard.local().NodePropertyTypeAddPeered("Person", "score", "integer").get();
    graph.shard.local().RelationshipTypeInsertPeered("KNOWS").get();

    uint64_t a = graph.shard.local().NodeAddPeered("Person", "a",
        "{\"name\": \"  Alice  \", \"created\": \"2011-07-01T12:34:56\", \"score\": -7}").get();
    uint64_t b = graph.shard.local().NodeAddPeered("Person", "b",
        "{\"name\": \"Bob\", \"created\": \"2012-01-02T00:00:00\", \"score\": 4}").get();
    graph.shard.local().RelationshipAddPeered("KNOWS", a, b, "{}").get();
}

TEST_CASE("scalar function library", "[gql_executor_functions]") {
    auto graph = Graph("gql_scalar_functions");
    graph.Start().get();
    FnGraphStopGuard guard(graph);
    populate_fn_graph(graph);

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("substring is 0-based, and a length past the end clamps") {
        // The shape the benchmark uses: substring(x.dateCreated, 0, 10) to take the date part.
        std::string res = run("MATCH (p:Person) FILTER p.name = 'Bob' RETURN substring(p.created, 0, 10) AS d");
        INFO("result: " << res);
        REQUIRE(res.find("\"d\": \"2012-01-02\"") != std::string::npos);

        std::string tail = run("MATCH (p:Person) FILTER p.name = 'Bob' RETURN substring(p.created, 11) AS t");
        INFO("tail: " << tail);
        REQUIRE(tail.find("\"t\": \"00:00:00\"") != std::string::npos);
    }

    SECTION("graph element functions: element_id, property_exists, all_different, same") {
        // property_exists takes a bare property-name identifier and tests the element's actual props.
        std::string pe = run("MATCH (p:Person) FILTER p.name = 'Bob' "
                             "RETURN property_exists(p, score) AS has_score, property_exists(p, missing) AS has_missing");
        INFO("property_exists: " << pe);
        REQUIRE(pe.find("\"has_score\": true") != std::string::npos);
        REQUIRE(pe.find("\"has_missing\": false") != std::string::npos);

        // Across a KNOWS edge the endpoints are distinct elements; an element is the same as itself.
        std::string el = run("MATCH (p:Person)-[:KNOWS]->(q:Person) "
                             "RETURN same(p, q) AS s_pq, same(p, p) AS s_pp, all_different(p, q) AS ad");
        INFO("element predicates: " << el);
        REQUIRE(el.find("\"s_pq\": false") != std::string::npos);
        REQUIRE(el.find("\"s_pp\": true") != std::string::npos);
        REQUIRE(el.find("\"ad\": true") != std::string::npos);

        // element_id yields a non-null integer for each matched element.
        std::string eid = run("MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN element_id(p) AS ep, element_id(q) AS eq");
        INFO("element_id: " << eid);
        REQUIRE(eid.find("\"ep\":") != std::string::npos);
        REQUIRE(eid.find("\"eq\":") != std::string::npos);
        REQUIRE(eid.find("null") == std::string::npos);
    }

    SECTION("edge orientation predicates: IS DIRECTED, IS SOURCE/DESTINATION OF") {
        // The graph has one KNOWS edge from Alice (a, the source) to Bob (b, the destination).
        std::string res = run(
            "MATCH (p:Person)-[r:KNOWS]->(q:Person) "
            "RETURN r IS DIRECTED AS d, p IS SOURCE OF r AS psrc, q IS DESTINATION OF r AS qdst, "
            "q IS SOURCE OF r AS qsrc, p IS NOT SOURCE OF r AS pnsrc");
        INFO("orientation: " << res);
        REQUIRE(res.find("\"d\": true") != std::string::npos);       // ragedb edges are directed
        REQUIRE(res.find("\"psrc\": true") != std::string::npos);    // Alice is the source
        REQUIRE(res.find("\"qdst\": true") != std::string::npos);    // Bob is the destination
        REQUIRE(res.find("\"qsrc\": false") != std::string::npos);   // Bob is not the source
        REQUIRE(res.find("\"pnsrc\": false") != std::string::npos);  // Alice IS the source, so NOT is false

        // Usable as a filter: only the row where the endpoint matches survives.
        std::string filt = run("MATCH (p:Person)-[r:KNOWS]->(q:Person) WHERE q IS DESTINATION OF r RETURN q.name AS n");
        INFO("filter: " << filt);
        REQUIRE(filt.find("Bob") != std::string::npos);
    }

    SECTION("char_length and cardinality") {
        std::string res = run("MATCH (p:Person) FILTER p.name = 'Bob' RETURN char_length(p.name) AS n");
        REQUIRE(res.find("\"n\": 3") != std::string::npos);

        // cardinality over a list literal -- the GQL spelling of what the suites call size().
        std::string card = run("MATCH (p:Person) FILTER p.name = 'Bob' RETURN cardinality([1, 2, 3]) AS c");
        INFO("cardinality: " << card);
        REQUIRE(card.find("\"c\": 3") != std::string::npos);

        // cardinality wrapping an AGGREGATE: the argument must resolve to the collected list, not a
        // per-row NULL (the group evaluator has to thread the aggregate results into the function's
        // argument). Alice knows one person, so the collected list has one element.
        std::string over_agg = run(
            "MATCH (a:Person)-[:KNOWS]->(f:Person) RETURN cardinality(collect_list(f.name)) AS c");
        INFO("over aggregate: " << over_agg);
        REQUIRE(over_agg.find("\"c\": 1") != std::string::npos);
    }

    SECTION("upper, lower and the trims") {
        std::string res = run("MATCH (p:Person) FILTER p.name = 'Bob' RETURN upper(p.name) AS u, lower(p.name) AS l");
        REQUIRE(res.find("\"u\": \"BOB\"") != std::string::npos);
        REQUIRE(res.find("\"l\": \"bob\"") != std::string::npos);

        // Alice's name is stored with surrounding spaces.
        std::string t = run("MATCH (p:Person) FILTER p.score = -7 RETURN trim(p.name) AS t, ltrim(p.name) AS lt");
        INFO("trims: " << t);
        REQUIRE(t.find("\"t\": \"Alice\"") != std::string::npos);
        REQUIRE(t.find("\"lt\": \"Alice  \"") != std::string::npos);
    }

    SECTION("numeric functions, with integers staying integral") {
        std::string res = run("MATCH (p:Person) FILTER p.score = -7 RETURN abs(p.score) AS a, mod(p.score, 4) AS m");
        INFO("result: " << res);
        REQUIRE(res.find("\"a\": 7") != std::string::npos);      // integer in, integer out
        REQUIRE(res.find("\"m\": -3") != std::string::npos);

        std::string f = run("MATCH (p:Person) FILTER p.score = 4 RETURN sqrt(p.score) AS s, power(p.score, 2) AS pw");
        INFO("floats: " << f);
        REQUIRE(f.find("\"s\": 2") != std::string::npos);
        REQUIRE(f.find("\"pw\": 16") != std::string::npos);
    }

    SECTION("a zero divisor and a negative root are NULL, not a trap") {
        std::string res = run("MATCH (p:Person) FILTER p.score = 4 RETURN mod(p.score, 0) AS m, sqrt(-1) AS s");
        INFO("result: " << res);
        REQUIRE(res.find("null") != std::string::npos);
    }

    SECTION("coalesce takes the first non-null, nullif nulls an equal pair") {
        // nullif(p.name, p.name) is null (the args are equal); coalesce then falls through to the literal.
        std::string res = run("MATCH (p:Person) FILTER p.name = 'Bob' "
                              "RETURN coalesce(nullif(p.name, p.name), 'fallback') AS c, nullif(p.name, 'Bob') AS n");
        INFO("result: " << res);
        REQUIRE(res.find("\"c\": \"fallback\"") != std::string::npos);
        REQUIRE(res.find("\"n\": null") != std::string::npos);
    }

    SECTION("nodes() and relationships() expose a path's elements") {
        std::string res = run(
            "MATCH p = ANY SHORTEST (a:Person)-[:KNOWS]-{1,2}(b:Person) "
            "FILTER a.name = 'Bob' AND b.score = -7 "
            "RETURN cardinality(nodes(p)) AS n, cardinality(relationships(p)) AS r");
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 2") != std::string::npos);   // Bob, Alice
        REQUIRE(res.find("\"r\": 1") != std::string::npos);   // one KNOWS
    }

    SECTION("rels() is an alias of relationships()") {
        std::string res = run(
            "MATCH p = ANY SHORTEST (a:Person)-[:KNOWS]-{1,2}(b:Person) "
            "FILTER a.name = 'Bob' AND b.score = -7 "
            "RETURN cardinality(rels(p)) AS r");
        INFO("result: " << res);
        REQUIRE(res.find("\"r\": 1") != std::string::npos);   // same one KNOWS as relationships(p)
    }

    SECTION("range() builds an inclusive integer list; size() over a list is its cardinality") {
        std::string res = run(
            "MATCH (a:Person) FILTER a.name = 'Bob' "
            "RETURN size(range(1, 5)) AS n, size([10, 20, 30]) AS s, range(0, 4)[2] AS x");
        INFO("result: " << res);
        REQUIRE(res.find("\"n\": 5") != std::string::npos);   // range(1,5) = [1,2,3,4,5]
        REQUIRE(res.find("\"s\": 3") != std::string::npos);   // size of a 3-element list literal
        REQUIRE(res.find("\"x\": 2") != std::string::npos);   // range(0,4)[2] == 2
    }

    SECTION("temporal field accessors .year/.month/.day/.hour on an epoch-ms datetime") {
        std::string res = run(
            "MATCH (a:Person) FILTER a.name = 'Bob' "
            "RETURN zoned_datetime('2011-03-15T10:20:30').year AS y, "
            "       zoned_datetime('2011-03-15T10:20:30').month AS mo, "
            "       zoned_datetime('2011-03-15T10:20:30').day AS d, "
            "       zoned_datetime('2011-03-15T10:20:30').hour AS h");
        INFO("res: " << res);
        REQUIRE(res.find("\"y\": 2011") != std::string::npos);
        REQUIRE(res.find("\"mo\": 3") != std::string::npos);
        REQUIRE(res.find("\"d\": 15") != std::string::npos);
        REQUIRE(res.find("\"h\": 10") != std::string::npos);
    }

    SECTION("duration() ISO-8601 string as milliseconds; datetime +/- duration arithmetic") {
        std::string res = run(
            "MATCH (a:Person) FILTER a.name = 'Bob' "
            "RETURN duration('P100D') AS d, "
            "       (zoned_datetime('2011-01-01T00:00:00') + duration('P1D') = zoned_datetime('2011-01-02T00:00:00')) AS nextDay, "
            "       (zoned_datetime('2011-01-01T05:00:00') - duration('PT2H') = zoned_datetime('2011-01-01T03:00:00')) AS minus2h");
        INFO("res: " << res);
        REQUIRE(res.find("\"d\": 8640000000") != std::string::npos);   // 100 days in ms
        REQUIRE(res.find("\"nextDay\": true") != std::string::npos);
        REQUIRE(res.find("\"minus2h\": true") != std::string::npos);
    }

    SECTION("duration({...}) map form folds to milliseconds at parse time") {
        std::string res = run(
            "MATCH (a:Person) FILTER a.name = 'Bob' "
            "RETURN duration({hours: 4}) AS h, duration({days: 1, hours: 2}) AS d, "
            "       (zoned_datetime('2011-01-01T00:00:00') + duration({days: 1}) = zoned_datetime('2011-01-02T00:00:00')) AS nextDay");
        INFO("res: " << res);
        REQUIRE(res.find("\"h\": 14400000") != std::string::npos);   // 4 hours
        REQUIRE(res.find("\"d\": 93600000") != std::string::npos);   // 1 day + 2 hours
        REQUIRE(res.find("\"nextDay\": true") != std::string::npos);
    }

    SECTION("date() truncates to the day (accepts an epoch-ms value, not only a string)") {
        std::string res = run(
            "MATCH (a:Person) FILTER a.name = 'Bob' "
            "RETURN date(zoned_datetime('2011-01-15T12:30:00')) = date(zoned_datetime('2011-01-15T23:59:00')) AS sameDay, "
            "       date(zoned_datetime('2011-01-15T12:30:00')) = date(zoned_datetime('2011-01-16T00:00:00')) AS diffDay");
        INFO("result: " << res);
        REQUIRE(res.find("\"sameDay\": true") != std::string::npos);   // both truncate to 2011-01-15
        REQUIRE(res.find("\"diffDay\": false") != std::string::npos);  // different days
    }

    SECTION("list comprehension and quantified predicates all/any/none/single") {
        std::string res = run(
            "MATCH (a:Person) FILTER a.name = 'Bob' "
            "RETURN [x IN range(1, 3) | x * 10][2] AS mappedLast, "
            "       size([x IN range(1, 5) WHERE x > 3]) AS filteredCount, "
            "       all(i IN range(0, 2) WHERE i < 5) AS allLt5, "
            "       any(i IN range(0, 2) WHERE i = 2) AS anyEq2, "
            "       none(i IN range(0, 2) WHERE i > 9) AS noneGt9, "
            "       single(i IN range(0, 3) WHERE i = 2) AS singleEq2");
        INFO("result: " << res);
        REQUIRE(res.find("\"mappedLast\": 30") != std::string::npos);    // [10,20,30][2]
        REQUIRE(res.find("\"filteredCount\": 2") != std::string::npos);  // [4,5]
        REQUIRE(res.find("\"allLt5\": true") != std::string::npos);
        REQUIRE(res.find("\"anyEq2\": true") != std::string::npos);
        REQUIRE(res.find("\"noneGt9\": true") != std::string::npos);
        REQUIRE(res.find("\"singleEq2\": true") != std::string::npos);
    }

    SECTION("list subscript list[i]: 0-based, negative from end, out-of-range -> null") {
        std::string res = run(
            "MATCH (a:Person) FILTER a.name = 'Bob' "
            "RETURN [10, 20, 30][1] AS a1, [10, 20, 30][-1] AS a2, [10, 20, 30][5] AS a3");
        INFO("result: " << res);
        REQUIRE(res.find("\"a1\": 20") != std::string::npos);     // 0-based index
        REQUIRE(res.find("\"a2\": 30") != std::string::npos);     // -1 -> last
        REQUIRE(res.find("\"a3\": null") != std::string::npos);   // out of range
    }

    guard.stop();
}

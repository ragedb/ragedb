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
#include "../../src/graph/types/NTriplesParser.h"
#include "../../src/graph/types/NTriplesLoader.h"
#include "../../src/gql/GqlParser.h"
#include "../../src/gql/GqlOptimizer.h"
#include "../../src/gql/GqlExecutor.h"
#include <cstdio>
#include <fstream>

using namespace ragedb;
using namespace ragedb::gql;

TEST_CASE("N-Triples loader maps RDF into the property graph", "[ntriples_loader]") {
    auto graph = Graph("nt_loader_test");
    graph.Start().get();
    graph.Clear();

    std::vector<NTriple> triples;
    for (const char* line : {
        "<http://ex/cw/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/CreativeWork> .",
        "<http://ex/cw/1> <http://ex/title> \"Climate Policy\" .",
        "<http://ex/cw/1> <http://ex/wordCount> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
        "<http://ex/t/9> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Thing> .",
        "<http://ex/cw/1> <http://ex/about> <http://ex/t/9> ."
    }) {
        auto t = parse_ntriple_line(line);
        if (t) triples.push_back(*t);
    }

    load_ntriples(graph.shard.local(), triples).get();

    auto run = [&graph](const std::string& q) {
        auto query = GqlParser::parse(q);
        GqlOptimizer::optimize(query);
        return GqlExecutor::execute(graph, std::move(query)).get();
    };

    SECTION("rdf:type became a label; literal objects became typed properties") {
        std::string r = run("MATCH (c:CreativeWork) RETURN c.title AS title, c.wordCount AS wc");
        INFO("result: " << r);
        REQUIRE(r.find("Climate Policy") != std::string::npos);
        REQUIRE(r.find("\"wc\": 42") != std::string::npos);   // typed integer, not a string
    }

    SECTION("an IRI-object predicate became a relationship") {
        std::string r = run("MATCH (:CreativeWork)-[:about]->(t:Thing) RETURN count(t) AS c");
        INFO("result: " << r);
        REQUIRE(r.find("\"c\": 1") != std::string::npos);
    }

    graph.Stop().get();
}

TEST_CASE("N-Triples file loader reads a file and loads it", "[ntriples_loader]") {
    const std::string path = "/tmp/ragedb_nt_loader_test.nt";
    {
        std::ofstream out(path);
        out << "<http://ex/p/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Person> .\n";
        out << "<http://ex/p/1> <http://ex/name> \"Alice\" .\n";
        out << "# a comment, then a blank line\n";
        out << "\n";
    }
    auto graph = Graph("nt_file_loader_test");
    graph.Start().get();
    graph.Clear();

    load_ntriples_file(graph.shard.local(), path).get();

    auto query = GqlParser::parse("MATCH (p:Person) RETURN p.name AS name");
    GqlOptimizer::optimize(query);
    std::string r = GqlExecutor::execute(graph, std::move(query)).get();
    INFO("result: " << r);
    REQUIRE(r.find("Alice") != std::string::npos);

    graph.Stop().get();
    std::remove(path.c_str());
}

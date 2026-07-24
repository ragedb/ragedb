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
#include "../../src/graph/types/NTriplesParser.h"

using namespace ragedb;

TEST_CASE("N-Triples line parser", "[ntriples]") {
    SECTION("IRI subject/predicate with an IRI object") {
        auto t = parse_ntriple_line(
            "<http://ex/CreativeWork/1> <http://ex/about> <http://ex/Thing/2> .");
        REQUIRE(t.has_value());
        REQUIRE(t->subject.kind == NTerm::IRI);
        REQUIRE(t->subject.value == "http://ex/CreativeWork/1");
        REQUIRE(t->predicate.value == "http://ex/about");
        REQUIRE(t->object.kind == NTerm::IRI);
        REQUIRE(t->object.value == "http://ex/Thing/2");
        REQUIRE(t->graph.empty());
    }

    SECTION("plain string literal object") {
        auto t = parse_ntriple_line("<http://ex/CW/1> <http://ex/title> \"climate policy\" .");
        REQUIRE(t.has_value());
        REQUIRE(t->object.kind == NTerm::LITERAL);
        REQUIRE(t->object.value == "climate policy");
        REQUIRE(t->object.lang.empty());
        REQUIRE(t->object.datatype.empty());
    }

    SECTION("literal with a language tag (lowercased)") {
        auto t = parse_ntriple_line("<http://ex/CW/1> <http://ex/title> \"Bonjour\"@FR .");
        REQUIRE(t->object.value == "Bonjour");
        REQUIRE(t->object.lang == "fr");
    }

    SECTION("typed literal with ^^<datatype>") {
        auto t = parse_ntriple_line(
            "<http://ex/CW/1> <http://ex/length> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .");
        REQUIRE(t->object.value == "42");
        REQUIRE(t->object.datatype == "http://www.w3.org/2001/XMLSchema#integer");
    }

    SECTION("string escapes are decoded") {
        auto t = parse_ntriple_line("<http://ex/s> <http://ex/p> \"a\\tb\\n\\\"c\\\"\" .");
        REQUIRE(t->object.value == std::string("a\tb\n\"c\""));
    }

    SECTION("\\u unicode escape decodes to UTF-8") {
        auto t = parse_ntriple_line("<http://ex/s> <http://ex/p> \"\\u00e9\" .");  // e-acute
        REQUIRE(t->object.value == std::string("\xC3\xA9"));
    }

    SECTION("blank node subject and object") {
        auto t = parse_ntriple_line("_:b0 <http://ex/knows> _:b1 .");
        REQUIRE(t->subject.kind == NTerm::BLANK);
        REQUIRE(t->subject.value == "b0");
        REQUIRE(t->object.kind == NTerm::BLANK);
        REQUIRE(t->object.value == "b1");
    }

    SECTION("N-Quads graph label is captured") {
        auto t = parse_ntriple_line(
            "<http://ex/s> <http://ex/p> <http://ex/o> <http://ex/graph/1> .");
        REQUIRE(t->graph == "http://ex/graph/1");
    }

    SECTION("blank lines and comments are skipped") {
        REQUIRE_FALSE(parse_ntriple_line("").has_value());
        REQUIRE_FALSE(parse_ntriple_line("   ").has_value());
        REQUIRE_FALSE(parse_ntriple_line("# a comment").has_value());
    }

    SECTION("a malformed statement throws") {
        REQUIRE_THROWS_AS(parse_ntriple_line("<http://ex/s> <http://ex/p> \"unterminated"),
                          std::runtime_error);
        REQUIRE_THROWS_AS(parse_ntriple_line("<http://ex/s> <http://ex/p> <http://ex/o>"),  // no '.'
                          std::runtime_error);
    }
}

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

// The value-comparison primitives underneath ORDER BY, MIN/MAX and every WHERE comparison. compare_properties
// orders the scalar property variant; compare_gql_values wraps it and adds list/type ordering. Both are pure,
// so they are pinned directly rather than through a query result.

#include <catch2/catch.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "../../src/gql/GqlValue.h"
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
// A LIST GqlValue built from integer elements.
GqlValue int_list(std::vector<int64_t> xs) {
    GqlValue v;
    v.type = GqlValue::LIST;
    v.list = std::make_shared<std::vector<GqlValue>>();
    for (auto x : xs) v.list->push_back(GqlValue(property_type_t{x}));
    return v;
}
}  // namespace

TEST_CASE("compare_properties orders null first and compares within a type", "[gql_value]") {
    // The null (monostate) alternative sorts before any actual value.
    REQUIRE(compare_properties(property_type_t{}, property_type_t{(int64_t)5}) < 0);
    REQUIRE(compare_properties(property_type_t{(int64_t)5}, property_type_t{}) > 0);
    REQUIRE(compare_properties(property_type_t{}, property_type_t{}) == 0);
    // Within a single type: integers, floats, strings (lexicographic) and booleans (false < true).
    REQUIRE(compare_properties(property_type_t{(int64_t)3}, property_type_t{(int64_t)5}) < 0);
    REQUIRE(compare_properties(property_type_t{2.5}, property_type_t{1.5}) > 0);
    REQUIRE(compare_properties(property_type_t{std::string("apple")}, property_type_t{std::string("banana")}) < 0);
    REQUIRE(compare_properties(property_type_t{false}, property_type_t{true}) < 0);
}

TEST_CASE("compare_properties compares an integer and a float by numeric value", "[gql_value]") {
    REQUIRE(compare_properties(property_type_t{(int64_t)5}, property_type_t{5.0}) == 0);
    REQUIRE(compare_properties(property_type_t{(int64_t)5}, property_type_t{5.5}) < 0);
    REQUIRE(compare_properties(property_type_t{5.5}, property_type_t{(int64_t)5}) > 0);
    // A non-numeric cross-type pair has no defined order and compares equal.
    REQUIRE(compare_properties(property_type_t{std::string("x")}, property_type_t{(int64_t)5}) == 0);
}

TEST_CASE("compare_gql_values compares lists by length then element by element", "[gql_value]") {
    REQUIRE(compare_gql_values(int_list({1, 2}), int_list({1, 2, 3})) < 0);   // the shorter list sorts first
    REQUIRE(compare_gql_values(int_list({1, 2}), int_list({1, 3})) < 0);      // equal length, 2 < 3
    REQUIRE(compare_gql_values(int_list({1, 2}), int_list({1, 2})) == 0);
}

TEST_CASE("compare_gql_values orders values of different types consistently", "[gql_value]") {
    GqlValue prop(property_type_t{(int64_t)1});
    GqlValue list = int_list({1});
    int forward = compare_gql_values(prop, list);
    REQUIRE(forward != 0);                                 // values of different types are never equal
    REQUIRE(compare_gql_values(list, prop) == -forward);   // and the ordering is antisymmetric
}

namespace {
bool is_null_value(const GqlValue& v) {
    return v.type != GqlValue::PROPERTY || std::holds_alternative<std::monostate>(v.property);
}
GqlValue prop_int(int64_t x) { return GqlValue(property_type_t{x}); }
GqlValue prop_double(double x) { return GqlValue(property_type_t{x}); }
GqlValue prop_string(std::string x) { return GqlValue(property_type_t{x}); }
GqlValue prop_bool(bool x) { return GqlValue(property_type_t{x}); }
}  // namespace

TEST_CASE("apply_cast to STRING renders each primitive", "[gql_value]") {
    REQUIRE(std::get<std::string>(apply_cast(prop_int(42), CastType::STRING).property) == "42");
    REQUIRE(std::get<std::string>(apply_cast(prop_double(2.5), CastType::STRING).property) == "2.5");
    REQUIRE(std::get<std::string>(apply_cast(prop_bool(true), CastType::STRING).property) == "true");
}

TEST_CASE("apply_cast to INTEGER parses whole strings, rounds floats, and NULLs the rest", "[gql_value]") {
    REQUIRE(std::get<int64_t>(apply_cast(prop_string("42"), CastType::INTEGER).property) == 42);
    REQUIRE(is_null_value(apply_cast(prop_string("42x"), CastType::INTEGER)));   // a partial parse is NULL
    REQUIRE(is_null_value(apply_cast(prop_string("abc"), CastType::INTEGER)));
    REQUIRE(std::get<int64_t>(apply_cast(prop_double(2.7), CastType::INTEGER).property) == 3);   // rounds
    REQUIRE(std::get<int64_t>(apply_cast(prop_double(2.4), CastType::INTEGER).property) == 2);
    REQUIRE(std::get<int64_t>(apply_cast(prop_bool(true), CastType::INTEGER).property) == 1);
}

TEST_CASE("apply_cast to FLOAT widens integers and parses whole strings", "[gql_value]") {
    REQUIRE(std::get<double>(apply_cast(prop_int(5), CastType::FLOAT).property) == 5.0);
    REQUIRE(std::get<double>(apply_cast(prop_string("2.5"), CastType::FLOAT).property) == 2.5);
    REQUIRE(is_null_value(apply_cast(prop_string("2.5x"), CastType::FLOAT)));
}

TEST_CASE("apply_cast to BOOLEAN uses truthiness and case-insensitive keywords", "[gql_value]") {
    REQUIRE(std::get<bool>(apply_cast(prop_int(0), CastType::BOOLEAN).property) == false);
    REQUIRE(std::get<bool>(apply_cast(prop_int(5), CastType::BOOLEAN).property) == true);
    REQUIRE(std::get<bool>(apply_cast(prop_string("TRUE"), CastType::BOOLEAN).property) == true);
    REQUIRE(is_null_value(apply_cast(prop_string("yes"), CastType::BOOLEAN)));   // not a boolean keyword
}

TEST_CASE("apply_cast returns NULL for a null or non-primitive value", "[gql_value]") {
    REQUIRE(is_null_value(apply_cast(GqlValue(), CastType::INTEGER)));
    GqlValue list;
    list.type = GqlValue::LIST;
    list.list = std::make_shared<std::vector<GqlValue>>();
    REQUIRE(is_null_value(apply_cast(list, CastType::STRING)));
}

namespace {
// Match a node's actual type against the label expression parsed from `(n<label>)`.
bool label_matches(const std::string& label, const std::string& type) {
    auto q = GqlParser::parse("MATCH (n" + label + ") RETURN n");
    return matches_label_expr(type, q.matches[0].pattern.nodes[0].label_expr);
}
}  // namespace

TEST_CASE("matches_label_expr evaluates a label expression against a node's type", "[gql_value]") {
    SECTION("a literal matches only its own type") {
        REQUIRE(label_matches(":Person", "Person"));
        REQUIRE_FALSE(label_matches(":Person", "Company"));
    }
    SECTION("NOT inverts the match") {
        REQUIRE_FALSE(label_matches(":!Person", "Person"));
        REQUIRE(label_matches(":!Person", "Company"));
    }
    SECTION("OR matches either alternative") {
        REQUIRE(label_matches(":Person|Company", "Person"));
        REQUIRE(label_matches(":Person|Company", "Company"));
        REQUIRE_FALSE(label_matches(":Person|Company", "Other"));
    }
    SECTION("AND of two distinct labels cannot be satisfied by a single type") {
        REQUIRE_FALSE(label_matches(":Person&Employee", "Person"));
        REQUIRE_FALSE(label_matches(":Person&Employee", "Employee"));
    }
    SECTION("a wildcard matches any non-empty type") {
        REQUIRE(label_matches(":%", "Person"));
        REQUIRE_FALSE(label_matches(":%", ""));
    }
    SECTION("an absent label expression matches any type") {
        REQUIRE(label_matches("", "Anything"));
    }
}

TEST_CASE("as_list_elements coerces lists and stored list properties, else nullopt", "[gql_value]") {
    SECTION("a LIST value yields its elements") {
        GqlValue v;
        v.type = GqlValue::LIST;
        v.list = std::make_shared<std::vector<GqlValue>>();
        v.list->push_back(GqlValue(property_type_t{(int64_t)1}));
        v.list->push_back(GqlValue(property_type_t{(int64_t)2}));
        auto r = as_list_elements(v);
        REQUIRE(r.has_value());
        REQUIRE(r->size() == 2);
    }
    SECTION("a LIST with no backing vector is an empty list") {
        GqlValue v;
        v.type = GqlValue::LIST;   // the list pointer is left null
        auto r = as_list_elements(v);
        REQUIRE(r.has_value());
        REQUIRE(r->empty());
    }
    SECTION("a stored integer-list property is a list") {
        auto r = as_list_elements(GqlValue(property_type_t{std::vector<int64_t>{5, 6, 7}}));
        REQUIRE(r.has_value());
        REQUIRE(r->size() == 3);
    }
    SECTION("a stored string-list property is a list") {
        auto r = as_list_elements(GqlValue(property_type_t{std::vector<std::string>{"a", "b"}}));
        REQUIRE(r.has_value());
        REQUIRE(r->size() == 2);
    }
    SECTION("a scalar property is not a list") {
        REQUIRE_FALSE(as_list_elements(GqlValue(property_type_t{(int64_t)7})).has_value());
    }
    SECTION("a null value is not a list") {
        REQUIRE_FALSE(as_list_elements(GqlValue()).has_value());
    }
}

TEST_CASE("gql_temporal_field extracts calendar and clock components from an epoch-ms datetime", "[gql_value]") {
    auto field = [](int64_t ms, const std::string& f) {
        return std::get<int64_t>(gql_temporal_field(ms, f).property);
    };
    SECTION("the epoch itself is 1970-01-01T00:00:00") {
        REQUIRE(field(0, "year") == 1970);
        REQUIRE(field(0, "month") == 1);
        REQUIRE(field(0, "day") == 1);
        REQUIRE(field(0, "hour") == 0);
        REQUIRE(field(0, "minute") == 0);
        REQUIRE(field(0, "second") == 0);
    }
    SECTION("time-of-day components come from the millisecond offset") {
        REQUIRE(field(3661000, "hour") == 1);     // 1h 1m 1s past the epoch
        REQUIRE(field(3661000, "minute") == 1);
        REQUIRE(field(3661000, "second") == 1);
    }
    SECTION("a full day advances the day component") {
        REQUIRE(field(86400000, "day") == 2);
    }
    SECTION("an unknown field is NULL") {
        GqlValue r = gql_temporal_field(0, "bogus");
        REQUIRE((r.type != GqlValue::PROPERTY || std::holds_alternative<std::monostate>(r.property)));
    }
}

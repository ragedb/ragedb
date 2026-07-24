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

// The scalar-function library (GqlValueFunctions.cpp), exercised over literal arguments through
// evaluate_expression with an empty row -- the numeric, string, list, null-handling and temporal builtins the
// benchmark queries call. Element/graph functions (element_id, nodes, length, ...) need a live graph and are
// covered by the executor suite instead.

#include <catch2/catch.hpp>
#include <string>
#include "../../src/gql/GqlValue.h"
#include "../../src/gql/GqlParser.h"

using namespace ragedb;
using namespace ragedb::gql;

namespace {
GqlValue eval_scalar(const std::string& call) {
    auto q = GqlParser::parse("MATCH (n) RETURN " + call + " AS r");
    GqlRow row;
    return evaluate_expression(row, q.returns[0].expr.get());
}
bool is_null(const GqlValue& v) {
    return v.type != GqlValue::PROPERTY || std::holds_alternative<std::monostate>(v.property);
}
int64_t as_int(const GqlValue& v) { return std::get<int64_t>(v.property); }
double as_dbl(const GqlValue& v) { return std::get<double>(v.property); }
std::string as_str(const GqlValue& v) { return std::get<std::string>(v.property); }
size_t list_size(const GqlValue& v) { return v.list ? v.list->size() : 0; }
}  // namespace

TEST_CASE("scalar numeric functions", "[gql_value]") {
    REQUIRE(as_int(eval_scalar("abs(-5)")) == 5);            // an integer argument stays integral
    REQUIRE(as_dbl(eval_scalar("abs(-2.5)")) == 2.5);
    REQUIRE(as_dbl(eval_scalar("ceil(2.1)")) == 3.0);
    REQUIRE(as_dbl(eval_scalar("floor(2.9)")) == 2.0);
    REQUIRE(as_dbl(eval_scalar("sqrt(9)")) == 3.0);
    REQUIRE(is_null(eval_scalar("sqrt(-1)")));               // no real root -> NULL, not NaN
    REQUIRE(as_dbl(eval_scalar("power(2, 10)")) == 1024.0);
    REQUIRE(as_int(eval_scalar("mod(7, 3)")) == 1);
    REQUIRE(as_int(eval_scalar("mod(-7, 3)")) == -1);
    REQUIRE(is_null(eval_scalar("mod(5, 0)")));              // a zero divisor is NULL, not a trap
}

TEST_CASE("scalar string functions", "[gql_value]") {
    REQUIRE(as_str(eval_scalar("substring('hello', 1, 3)")) == "ell");   // start is 0-based
    REQUIRE(as_str(eval_scalar("substring('hello', 3)")) == "lo");
    REQUIRE(as_str(eval_scalar("substring('hi', 5, 2)")) == "");         // a start past the end is empty
    REQUIRE(as_int(eval_scalar("char_length('hello')")) == 5);
    REQUIRE(as_str(eval_scalar("upper('abc')")) == "ABC");
    REQUIRE(as_str(eval_scalar("lower('ABC')")) == "abc");
    REQUIRE(as_str(eval_scalar("trim('  x  ')")) == "x");
    REQUIRE(as_str(eval_scalar("ltrim('  x  ')")) == "x  ");             // trailing spaces remain
    REQUIRE(as_str(eval_scalar("rtrim('  x  ')")) == "  x");             // leading spaces remain
}

TEST_CASE("scalar list functions", "[gql_value]") {
    REQUIRE(as_int(eval_scalar("cardinality([1, 2, 3])")) == 3);
    REQUIRE(list_size(eval_scalar("range(1, 5)")) == 5);        // [1, 2, 3, 4, 5]
    REQUIRE(list_size(eval_scalar("range(1, 5, 2)")) == 3);     // [1, 3, 5]
}

TEST_CASE("scalar null-handling functions", "[gql_value]") {
    REQUIRE(as_int(eval_scalar("coalesce(null, 3)")) == 3);
    REQUIRE(is_null(eval_scalar("coalesce(null, null)")));
    REQUIRE(is_null(eval_scalar("nullif(5, 5)")));
    REQUIRE(as_int(eval_scalar("nullif(5, 3)")) == 5);
}

TEST_CASE("scalar temporal functions return epoch milliseconds", "[gql_value]") {
    REQUIRE(as_int(eval_scalar("date('1970-01-01')")) == 0);
    REQUIRE(as_int(eval_scalar("datetime('1970-01-01T00:00:00')")) == 0);
    REQUIRE(as_int(eval_scalar("duration('P1D')")) == 86400000);
    REQUIRE(as_int(eval_scalar("duration('PT1H')")) == 3600000);
}

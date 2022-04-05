/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
#include <sstream>
#include "../../src/graph/types/Date.h"

SCENARIO( "Date can be created", "[date]" ) {
  GIVEN("Nothing") {
    ragedb::Date date(1649137648);

    WHEN("a date is requested") {
      THEN("we get it back") {
        REQUIRE(date.value == 1649137648);
      }
    }

    WHEN("we print a date") {
      std::stringstream out;
      out << date;
      THEN("we get the correct output") {
        REQUIRE(out.str() == "1649137648.000");
      }
    }
  }
}
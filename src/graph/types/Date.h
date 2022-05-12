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

#ifndef RAGEDB_DATE_H
#define RAGEDB_DATE_H


#include <cstdint>
#include <string>
namespace ragedb {
    class Date {

      Date() = default;

      static Date null()
      {
        return Date(0);
      }

      inline uint64_t hash() const;

      auto operator<=>(const Date&) const = default;

      static Date fromString(const char* str, uint32_t length);

      static Date fromString(std::string_view s);

    public:
      explicit Date(double _value) : value(_value) {}
      explicit Date(std::string_view _value)  {
        value = fromString(_value).value;
      }
      double value;
      static double convert(std::string s);

      friend std::ostream& operator<<(std::ostream& os, const Date& date);
    };
}



#endif// RAGEDB_DATE_H

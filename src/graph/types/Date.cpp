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

#include <cstring>
#include <ios>
#include <iomanip>
#include "Date.h"

namespace ragedb {
    static const uint64_t msPerDay = 24 * 60 * 60 * 1000;

    // Merge into ms since midnight
    static unsigned mergeTime(unsigned hour, unsigned minute, unsigned second, unsigned ms) {
      return ms + (1000 * second) + (60 * 1000 * minute) + (60 * 60 * 1000 * hour);
    }

    // Algorithm from the Calendar FAQ
    static unsigned mergeJulianDay(unsigned year, unsigned month, unsigned day) {
      unsigned a = (14 - month) / 12;
      unsigned y = year + 4800 - a;
      unsigned m = month + (12 * a) - 3;

      return day + ((153 * m + 2) / 5) + (365 * y) + (y / 4) - (y / 100) + (y / 400) - 32045;
    }

    uint64_t Date::hash() const {
      return std::hash<double>()(value);
    }

    bool Date::operator==(const Date &n) const {
      return value == n.value;
    }

    bool Date::operator!=(const Date &n) const {
      return value != n.value;
    }

    bool Date::operator<(const Date &n) const {
      return value < n.value;
    }

    bool Date::operator<=(const Date &n) const {
      return value <= n.value;
    }

    bool Date::operator>(const Date &n) const {
      return value > n.value;
    }

    bool Date::operator>=(const Date &n) const {
      return value >= n.value;
    }

    Date Date::fromString(const char *str, uint32_t length) {
      if ((length == 4) && (strncmp(str, "NULL", 4) == 0))
        return null();

      auto iter = str;
      auto limit = str + length;

      // Trim WS
      while ((iter != limit) && ((*iter) == ' ')) ++iter;
      while ((iter != limit) && ((*(limit - 1)) == ' ')) --limit;

      // Year
      unsigned year = 0;
      while (true) {
        if (iter == limit) return null();
        char c = *(iter++);
        if (c == '-') break;
        if ((c >= '0') && (c <= '9')) {
          year = 10 * year + (c - '0');
        } else
          return null();
      }
      // Month
      unsigned month = 0;
      while (true) {
        if (iter == limit) return null();
        char c = *(iter++);
        if (c == '-') break;
        if ((c >= '0') && (c <= '9')) {
          month = 10 * month + (c - '0');
        } else
          return null();
      }
      // Day
      unsigned day = 0;
      while (true) {
        if (iter == limit) break;
        char c = *(iter++);
        if (c == ' ') break;
        if ((c >= '0') && (c <= '9')) {
          day = 10 * day + (c - '0');
        } else
          return null();
      }

      // Range check
      if ((year > 9999) || (month < 1) || (month > 12) || (day < 1) || (day > 31))
        return null();
      uint64_t date = mergeJulianDay(year, month, day);

      // Hour
      unsigned hour = 0;
      while (true) {
        if (iter == limit) return null();
        char c = *(iter++);
        if (c == ':') break;
        if ((c >= '0') && (c <= '9')) {
          hour = 10 * hour + (c - '0');
        } else
          return null();
      }
      // Minute
      unsigned minute = 0;
      while (true) {
        if (iter == limit) return null();
        char c = *(iter++);
        if (c == ':') break;
        if ((c >= '0') && (c <= '9')) {
          minute = 10 * minute + (c - '0');
        } else
          return null();
      }
      // Second
      unsigned second = 0;
      while (true) {
        if (iter == limit) break;
        char c = *(iter++);
        if (c == '.') break;
        if ((c >= '0') && (c <= '9')) {
          second = 10 * second + (c - '0');
        } else
          return null();
      }
      // Millisecond
      unsigned ms = 0;
      while (iter != limit) {
        char c = *(iter++);
        if ((c >= '0') && (c <= '9')) {
          ms = 10 * ms + (c - '0');
        } else
          return null();
      }

      // Range check
      if ((hour >= 24) || (minute >= 60) || (second >= 60) || (ms >= 1000))
        return null();
      uint64_t time = mergeTime(hour, minute, second, ms);

      // Merge
      Date t;
      t.value = (date * msPerDay) + time;
      return t;
    }

    Date Date::fromString(std::string s) {
      return fromString(s.data(), s.size());
    }

    std::ostream &operator<<(std::ostream &os, const Date &date) {
      os << std::fixed << std::setw(11) << std::setprecision(3) << std::setfill('0') << date.value;
      return os;
    }

}
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
#include <chrono>
#include "Date.h"

namespace ragedb {
    static const uint64_t secondsPerDay = 24 * 60 * 60;

    // Merge into seconds since midnight
    static double mergeTime(unsigned hour, unsigned minute, unsigned second, unsigned ms) {
      return (ms/1000.0) + second + (60 * minute) + (60 * 60 * hour);
    }

    static unsigned mergeTimeZone(unsigned hour, unsigned minute) {
      return (60 * minute) + (60 * 60 * hour);
    }

    // From http://howardhinnant.github.io/date_algorithms.html#days_from_civil
    static double daysFromCivil(int y, unsigned m, unsigned d) noexcept {
      y -= m <= 2;
      const int era = (y >= 0 ? y : y - 399) / 400;
      const auto yoe = static_cast<unsigned>(y - era * 400);            // [0, 399]
      const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;  // [0, 365]
      const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;           // [0, 146096]
      return era * 146097 + static_cast<int>(doe) - 719468;
    }

    uint64_t Date::hash() const {
      return std::hash<double>()(value);
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
        } else {
          return null();
        }
      }

      // Month
      unsigned month = 0;
      while (true) {
        if (iter == limit) return null();
        char c = *(iter++);
        if (c == '-') break;
        if ((c >= '0') && (c <= '9')) {
          month = 10 * month + (c - '0');
        } else {
          return null();
        }
      }

      // Day
      unsigned day = 0;
      while (true) {
        if (iter == limit) break;
        char c = *(iter++);
        if (c == 'T') break;
        if ((c >= '0') && (c <= '9')) {
          day = 10 * day + (c - '0');
        } else {
          return null();
        }
      }

      // Range check
      if ((year > 9999) || (month < 1) || (month > 12) || (day < 1) || (day > 31)) {
        return null();
      }

      int64_t date = daysFromCivil(year, month, day);

      // Hour
      unsigned hour = 0;
      while (true) {
        if (iter == limit) return null();
        char c = *(iter++);
        if (c == ':') break;
        if ((c >= '0') && (c <= '9')) {
          hour = 10 * hour + (c - '0');
        } else {
          return null();
        }
      }

      // Minute
      unsigned minute = 0;
      while (true) {
        if (iter == limit) return null();
        char c = *(iter++);
        if (c == ':') break;
        if ((c >= '0') && (c <= '9')) {
          minute = 10 * minute + (c - '0');
        } else {
          return null();
        }
      }

      // Second
      unsigned second = 0;
      while (true) {
        if (iter == limit) break;
        char c = *(iter++);
        if (c == '.') break;
        if ((c >= '0') && (c <= '9')) {
          second = 10 * second + (c - '0');
        } else {
          return null();
        }
      }

      char tzc = 'Z';
      // Millisecond
      unsigned ms = 0;
      while (iter != limit) {
        char c = *(iter++);
        if ((c >= '0') && (c <= '9')) {
          ms = 10 * ms + (c - '0');
        } else {
          tzc = c;
          break;
        }
      }

      // Range check
      if ((hour >= 24) || (minute >= 60) || (second >= 60) || (ms >= 1000)) {
        return null();
      }

      double time = mergeTime(hour, minute, second, ms);

      // Time Zone
      unsigned timezone = 0;
      if (tzc != 'Z') {
        // Time Zone Hour
        unsigned tz_hour = 0;
        if (iter == limit) return null();
        char c = *(iter++);
        if ((c >= '0') && (c <= '9')) {
          tz_hour = 10 * tz_hour + (c - '0');
        } else {
          return null();
        }
        if (iter == limit) return null();
        c = *(iter++);
        if ((c >= '0') && (c <= '9')) {
          tz_hour = 10 * tz_hour + (c - '0');
        } else {
          return null();
        }
        if (c == ':') {
          c = *(iter++);
        }

        // Time Zone Minute
        unsigned tz_minute = 0;
        while (true) {
          if (iter == limit) break;
          char c2 = *(iter++);
          if ((c2 >= '0') && (c2 <= '9')) {
            tz_minute = 10 * tz_minute + (c2 - '0');
          } else
            return null();
        }
        // Range check
        if ((tz_hour >= 24) || (tz_minute >= 60)) {
          return null();
        }

        timezone = mergeTimeZone(tz_hour, tz_minute);
      }

      // Merge
      Date t;
      t.value = (date * secondsPerDay) + time;

      if (tzc == '-') {
        t.value -= timezone;
      } else {
        t.value += timezone;
      }

      return t;
    }

    Date Date::fromString(std::string_view s) {
      return fromString(s.data(), s.size());
    }

    double Date::convert(std::string s) {
      return Date(s).value;
    }

    template <class Int>
    constexpr
      std::tuple<Int, unsigned, unsigned>
      civil_from_days(Int z) noexcept
    {
        static_assert(std::numeric_limits<unsigned>::digits >= 18,
          "This algorithm has not been ported to a 16 bit unsigned integer");
        static_assert(std::numeric_limits<Int>::digits >= 20,
          "This algorithm has not been ported to a 16 bit signed integer");
        z += 719468;
        const Int era = (z >= 0 ? z : z - 146096) / 146097;
        const unsigned doe = static_cast<unsigned>(z - era * 146097);          // [0, 146096]
        const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;  // [0, 399]
        const Int y = static_cast<Int>(yoe) + era * 400;
        const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);                // [0, 365]
        const unsigned mp = (5*doy + 2)/153;                                   // [0, 11]
        const unsigned d = doy - (153*mp+2)/5 + 1;                             // [1, 31]
        const unsigned m = mp < 10 ? mp+3 : mp-9;                            // [1, 12]
        return std::tuple<Int, unsigned, unsigned>(y + (m <= 2), m, d);
    }

    std::string Date::toISO(double value) {
        std::stringstream stream;
        std::chrono::duration<double> now = std::chrono::duration<double>(value);
        auto today = duration_cast<std::chrono::days>(now);
        int year;
        unsigned month;
        unsigned day;
        std::tie(year, month, day) = civil_from_days(today.count());
        now -= today;
        auto h = duration_cast<std::chrono::hours>(now);
        now -= h;
        auto m = duration_cast<std::chrono::minutes>(now);
        now -= m;
        auto s = duration_cast<std::chrono::seconds>(now);
        now -= s;
        auto ml = duration_cast<std::chrono::milliseconds>(now);
        stream.fill('0');
//"2010-03-13T07:37:21.718+0000Z"
        stream
           << year << '-'
           << std::setw(2) << month << '-'
           << std::setw(2) << day
           << "T" << std::setw(2) << h.count() << ':'
           << std::setw(2) << m.count() << ':'
           << std::setw(2) << s.count() << '.'
           << std::setw(3) << ml.count() <<
          "+0000Z";
        return stream.str();
    }

    std::ostream &operator<<(std::ostream &os, const Date &date) {
      os << std::fixed << std::setw(11) << std::setprecision(3) << std::setfill('0') << date.value;
      return os;
    }

    }
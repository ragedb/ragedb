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

#ifndef RAGEDB_EXPRESSION_H
#define RAGEDB_EXPRESSION_H

#include <algorithm>
#include <string>
#include <vector>
#include "Operation.h"

namespace ragedb {
    class Expression {
    public:

        template <typename T>
        static bool Evaluate(Operation operation, T a, T b) {
            switch(operation) {
                case Operation::EQ:
                  return EQ(a, b);
                case Operation::NEQ:
                  return NEQ(a, b);
                case Operation::GT:
                  return GT(a, b);
                case Operation::GTE:
                  return GTE(a, b);
                case Operation::LT:
                  return LT(a, b);
                case Operation::LTE:
                  return LTE(a, b);
                default:
                  return false;
            }
        }

        //TODO: Look into case insensitivity https://www.boost.org/doc/libs/1_77_0/doc/html/string_algo/reference.html#header.boost.algorithm.string.predicate_hpp
        static bool EvaluateString(Operation operation, std::string a, std::string b) {
            switch(operation) {
                case Operation::EQ:
                  return EQ(a, b);
                case Operation::NEQ:
                  return NEQ(a, b);
                case Operation::GT:
                  return GT(a, b);
                case Operation::GTE:
                  return GTE(a, b);
                case Operation::LT:
                  return LT(a, b);
                case Operation::LTE:
                  return LTE(a, b);
                case Operation::STARTS_WITH:
                  return STARTS_WITH(a, b);
                case Operation::CONTAINS:
                  return CONTAINS(a, b);
                case Operation::ENDS_WITH:
                  return ENDS_WITH(a, b);
                case Operation::NOT_STARTS_WITH:
                  return NOT_STARTS_WITH(a, b);
                case Operation::NOT_CONTAINS:
                  return NOT_CONTAINS(a, b);
                case Operation::NOT_ENDS_WITH:
                  return NOT_ENDS_WITH(a, b);
                default:
                  return false;
            }
        }

        template <typename T>
        static bool EvaluateVector(Operation operation, std::vector<T> a, std::vector<T> b) {
            switch(operation) {
                case Operation::EQ:
                  return EQ(a, b);
                case Operation::NEQ:
                  return NEQ(a, b);
                case Operation::GT:
                  return GT(a.size(), b.size());
                case Operation::GTE:
                  return GT(a.size(), b.size()) || EQ(a, b);
                case Operation::LT:
                  return LT(a.size(), b.size());
                case Operation::LTE:
                  return LT(a.size(), b.size()) || EQ(a, b);
                case Operation::STARTS_WITH:
                  return STARTS_WITH(a, b);
                case Operation::CONTAINS:
                  return std::ranges::includes(a, b);
                case Operation::ENDS_WITH:
                  return ENDS_WITH(a, b);
                case Operation::NOT_STARTS_WITH:
                  return NOT_STARTS_WITH(a, b);
                case Operation::NOT_CONTAINS:
                  return !std::ranges::includes(a, b);
                case Operation::NOT_ENDS_WITH:
                  return NOT_ENDS_WITH(a, b);
                default:
                  return false;
            }
        }

        template <typename T>
        static bool EQ(T a, T b) {
            return a == b;
        }

        template <typename T>
        static bool NEQ(T a, T b) {
            return a != b;
        }

        template <typename T>
        static bool GT(T a, T b) {
            return a > b;
        }

        static bool GT(bool a, bool b) {
            return !b && a;
        }

        template <typename T>
        static bool GTE(T a, T b) {
            return (a >= b);
        }

        template <typename T>
        static bool LT(T a, T b)  {
            return a < b;
        }

        static bool LT(bool a, bool b) {
            return !a && b;
        }

        template <typename T>
        static bool LTE(T a, T b) {
            return (a <= b);
        }

        template <typename T>
        static bool STARTS_WITH(T a, T b) {
          if (a.size() >= b.size()) {
            for (int i = 0; i < b.size(); i++) {
              if (a[i] != b[i]) {
                return false;
              }
            }
            return true;
          }
          return false;
        }

        template <typename T>
        static bool ENDS_WITH(T a, T b) {
          if (a.size() >= b.size()) {
            auto index = a.size() - b.size();
            for (int i = 0; i < b.size(); i++) {
              if (a[i + index] != b[i]) {
                return false;
              }
            }
            return true;
          }
          return false;
        }

        template <typename T>
        static bool NOT_STARTS_WITH(T a, T b) {
          if (a.size() >= b.size()) {
            for (int i = 0; i < b.size(); i++) {
              if (a[i] != b[i]) {
                return true;
              }
            }
            return false;
          }
          return true;
        }

        template <typename T>
        static bool NOT_ENDS_WITH(T a, T b) {
          if (a.size() >= b.size()) {
            auto index = a.size() - b.size();
            for (int i = 0; i < b.size(); i++) {
              if (a[i + index] != b[i]) {
                return true;
              }
            }
            return false;
          }
          return true;
        }

        static bool STARTS_WITH(const std::string &a, const std::string &b) {
            return a.starts_with(b);
        }

        static bool CONTAINS(const std::string &a, const std::string &b) {
            return a.find(b) != std::string::npos;
            //return a.contains(b); is available in C++ 23
        }

        static bool ENDS_WITH(const std::string &a, const std::string &b) {
            return a.ends_with(b);
        }

        static bool NOT_STARTS_WITH(const std::string &a, const std::string &b) {
            return !STARTS_WITH(a, b);
        }

        static bool NOT_CONTAINS(const std::string &a, const std::string &b) {
            return !CONTAINS(a, b);
        }

        static bool NOT_ENDS_WITH(const std::string &a, const std::string &b) {
            return !ENDS_WITH(a, b);
        }
    };
}

#endif //RAGEDB_EXPRESSION_H

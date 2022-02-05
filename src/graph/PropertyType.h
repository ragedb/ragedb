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

#ifndef RAGEDB_PROPERTYTYPE_H
#define RAGEDB_PROPERTYTYPE_H

#include <variant>
#include <cstdint>
#include <string>
#include <vector>

namespace ragedb {

  using property_type_t = std::variant<std::monostate, bool, int64_t, double, std::string, std::vector<bool>, std::vector<int64_t>, std::vector<double>, std::vector<std::string>>;

  }

#endif// RAGEDB_PROPERTYTYPE_H

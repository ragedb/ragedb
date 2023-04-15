/*
 * Copyright Max De Marzi. All Rights Reserved.
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


#include "../Shard.h"

namespace ragedb {
    InnerType Shard::disambiguate(sol::object arg)
    {
        if (arg.get_type() == sol::type::table) {
            // Many things
            auto table = arg.as<sol::table>();
            sol::object inner_type_value = table.get<sol::object>(1);
            // If we get an unsigned number, it must be ids
            if (inner_type_value.is<unsigned long>()) {
                return ids;
            }
            // Maybe its links?
            if (inner_type_value.is<Link>()) {
                return links;
            }

            // Could be nodes...
            if (inner_type_value.is<Node>()) {
                return nodes;
            }
            if (inner_type_value.is<std::string>()) {
                return strings;
            }
        } else {
            // Single thing
            sol::object value = arg;
            if (value.is<unsigned long>()) {
                return id;
            }
            if (value.is<std::string>()) {
                return string;
            }
            if (value.is<Direction>()) {
                return direction;
            }
            if (value.is<Link>()) {
                return link;
            }
            if (value.is<Node>()) {
                return node;
            }
        }
        // Otherwise it is unknown
        return unknown;
    }
}
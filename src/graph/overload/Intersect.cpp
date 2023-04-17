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
#include <sol/types.hpp>
#include "../Shard.h"

namespace ragedb {

    sol::object Shard::IntersectOverload(sol::this_state ts, sol::variadic_args args) {
        // If we have zero arguments, return nothing
        if (args.size() == 0) {
            return sol::make_object(ts, sol::nil);
        }
        auto type = disambiguate(args[0]);

        if (type == ids)
            return sol::make_object(ts, IntersectIdsViaLua(args.get<std::vector<uint64_t>>(), args.get<std::vector<uint64_t>>(1)));
        if (type == nodes)
            return sol::make_object(ts, IntersectNodesViaLua(args.get<std::vector<Node>>(), args.get<std::vector<Node>>(1)));
        if (type == relationships)
            return sol::make_object(ts, IntersectRelationshipsViaLua(args.get<std::vector<Relationship>>(), args.get<std::vector<Relationship>>(1)));

        return sol::make_object(ts, sol::nil);
    }

    sol::object Shard::IntersectCountOverload(sol::this_state ts, sol::variadic_args args) {
        // If we have zero arguments, return nothing
        if (args.size() == 0) {
            return sol::make_object(ts, sol::nil);
        }
        auto type = disambiguate(args[0]);

        if (type == ids)
            return sol::make_object(ts, IntersectIdsCountViaLua(args.get<std::vector<uint64_t>>(), args.get<std::vector<uint64_t>>(1)));
        if (type == nodes)
            return sol::make_object(ts, IntersectNodesCountViaLua(args.get<std::vector<Node>>(), args.get<std::vector<Node>>(1)));
        if (type == relationships)
            return sol::make_object(ts, IntersectRelationshipsCountViaLua(args.get<std::vector<Relationship>>(), args.get<std::vector<Relationship>>(1)));

        return sol::make_object(ts, sol::nil);
    }

}
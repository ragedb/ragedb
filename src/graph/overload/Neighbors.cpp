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

    sol::object Shard::NeighborsOverload(sol::this_state ts, sol::variadic_args args) {
        // If we have zero arguments, return nothing
        if (args.size() == 0) {
            return sol::make_object(ts, sol::nil);
        }
        auto type = disambiguate(args[0]);
        if (type == id) {
            switch (args.size()) {
            case 1: return sol::make_object(ts, NodeGetNeighborsViaLua(args.get<uint64_t>()));
            case 2: {
                auto type2 = disambiguate(args[1]);
                if (type2 == direction)
                    return sol::make_object(ts, NodeGetNeighborsViaLua(args.get<uint64_t>(), args.get<Direction>(1)));
                if (type2 == string)
                    return sol::make_object(ts, NodeGetNeighborsViaLua(args.get<uint64_t>(), Direction::BOTH, args.get<std::string>(1)));
                else
                    return sol::make_object(ts, NodeGetNeighborsViaLua(args.get<uint64_t>(), Direction::BOTH, args.get<std::vector<std::string>>(1)));
            }
            case 3: {
                if (disambiguate(args[2]) == string)
                    return sol::make_object(ts, NodeGetNeighborsViaLua(args.get<uint64_t>(), args.get<Direction>(1), args.get<std::string>(2)));
                else
                    return sol::make_object(ts, NodeGetNeighborsViaLua(args.get<uint64_t>(), args.get<Direction>(1), args.get<std::vector<std::string>>(2)));
            }
            default: return sol::make_object(ts, sol::nil);
            }
        }
        if (type == ids) {
            switch (args.size()) {
            case 1: return sol::make_object(ts, NodeIdsGetNeighborsViaLua(args.get<std::vector<uint64_t>>()));
            case 2: {
                auto type2 = disambiguate(args[1]);
                if (type2 == direction)
                    return sol::make_object(ts, NodeIdsGetNeighborsViaLua(args.get<std::vector<uint64_t>>(), args.get<Direction>(1)));
                if (type2 == string)
                    return sol::make_object(ts, NodeIdsGetNeighborsViaLua(args.get<std::vector<uint64_t>>(), Direction::BOTH, args.get<std::string>(1)));
                else
                    return sol::make_object(ts, NodeIdsGetNeighborsViaLua(args.get<std::vector<uint64_t>>(), Direction::BOTH, args.get<std::vector<std::string>>(1)));
            }
            case 3: {
                if (disambiguate(args[2]) == string)
                    return sol::make_object(ts, NodeIdsGetNeighborsViaLua(args.get<std::vector<uint64_t>>(), args.get<Direction>(1), args.get<std::string>(2)));
                else
                    return sol::make_object(ts, NodeIdsGetNeighborsViaLua(args.get<std::vector<uint64_t>>(), args.get<Direction>(1), args.get<std::vector<std::string>>(2)));
            }
            default: return sol::make_object(ts, sol::nil);
            }
        }
        if (type == links) {
            switch (args.size()) {
            case 1: return sol::make_object(ts, LinksGetNeighborsViaLua(args.get<std::vector<Link>>()));
            case 2: {
                auto type2 = disambiguate(args[1]);
                if (type2 == direction)
                    return sol::make_object(ts, LinksGetNeighborsViaLua(args.get<std::vector<Link>>(), args.get<Direction>(1)));
                if (type2 == string)
                    return sol::make_object(ts, LinksGetNeighborsViaLua(args.get<std::vector<Link>>(), Direction::BOTH, args.get<std::string>(1)));
                else
                    return sol::make_object(ts, LinksGetNeighborsViaLua(args.get<std::vector<Link>>(), Direction::BOTH, args.get<std::vector<std::string>>(1)));
            }
            case 3: {
                if (disambiguate(args[2]) == string)
                    return sol::make_object(ts, LinksGetNeighborsViaLua(args.get<std::vector<Link>>(), args.get<Direction>(1), args.get<std::string>(2)));
                else
                    return sol::make_object(ts, LinksGetNeighborsViaLua(args.get<std::vector<Link>>(), args.get<Direction>(1), args.get<std::vector<std::string>>(2)));
            }
            default: return sol::make_object(ts, sol::nil);
            }
        }
        return sol::make_object(ts, sol::nil);
    }
}
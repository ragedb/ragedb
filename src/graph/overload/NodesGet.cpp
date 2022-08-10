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

    sol::as_table_t<std::vector<Node>> Shard::NodesGetOverload(sol::this_state ts, sol::variadic_args args) {
        auto vargs = std::vector<sol::object>(args.begin(), args.end());
        std::vector<std::string> types;
        std::for_each(vargs.begin(), vargs.end(), [&](sol::object obj) {
              const auto type = obj.get_type();
              if (type == sol::type::table) {
                  auto table = obj.as<sol::table>();
                  if (table.size() > 0) {
                      sol::object inner_type_value = table.get<sol::object>(1);
                      const auto inner_type = inner_type_value.get_type();
                      if(inner_type == sol::type::number) {
                          // We have ids
                          std::vector<uint64_t> ids = obj.as<std::vector<uint64_t>>();
                          return NodesGetViaLua(ids);
                      } else  {
                          // we have links
                          std::vector<Link> links = obj.as<std::vector<Link>>();
                          //return NodesGetViaLua(links);
                          return NodesGetByLinksViaLua(links);
                      }
                  }
              }
          });
        return sol::as_table(std::vector<Node>());
    }
}
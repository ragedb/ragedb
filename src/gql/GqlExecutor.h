/*
 * Copyright RageDB Contributors. All Rights Reserved.
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

#ifndef RAGEDB_GQLEXECUTOR_H
#define RAGEDB_GQLEXECUTOR_H

#include <seastar/core/future.hh>
#include "../graph/Graph.h"
#include "GqlAst.h"

namespace ragedb::gql {

class GqlExecutor {
public:
    inline static bool force_disable_honeycomb = false;
    inline static bool force_enable_honeycomb = false;
    inline static bool force_enable_lftj = false;
    inline static bool force_disable_lftj = false;
    static seastar::future<std::string> execute(ragedb::Graph& graph, GqlQuery query);
    static seastar::future<std::string> execute(ragedb::Graph& graph, const std::string& query_str);
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLEXECUTOR_H

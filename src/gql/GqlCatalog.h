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

#ifndef RAGEDB_GQLCATALOG_H
#define RAGEDB_GQLCATALOG_H

#include <seastar/core/future.hh>
#include "../graph/Graph.h"
#include "GqlAst.h"

namespace ragedb::gql {

struct GqlCatalog {
    static seastar::future<std::string> execute_schema_op(ragedb::Graph& graph, const SchemaOperation& schema_op);
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLCATALOG_H

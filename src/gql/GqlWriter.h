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

#ifndef RAGEDB_GQLWRITER_H
#define RAGEDB_GQLWRITER_H

#include <seastar/core/future.hh>
#include <memory>
#include "../graph/Graph.h"
#include "GqlAst.h"
#include "GqlValue.h"

namespace ragedb::gql {

/**
 * @brief Recursively executes all write operations (INSERT, SET, REMOVE, DELETE) for a single query row.
 * 
 * Operations are chained sequentially within each row context.
 * 
 * @param graph The RageDB graph instance.
 * @param query_ptr Shared GQL query configuration.
 * @param write_idx Index of the write operation in the writes list.
 * @param row The row context to apply modifications to.
 * @return seastar::future<GqlRow> Future wrapping the updated row bindings.
 */
seastar::future<GqlRow> execute_writes_for_row(ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, size_t write_idx, GqlRow row);

} // namespace ragedb::gql

#endif // RAGEDB_GQLWRITER_H

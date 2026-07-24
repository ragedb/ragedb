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

#pragma once

// Parser helpers shared across the GqlParser translation units. resolve_order_by_aliases is defined in
// GqlParserStatements.cpp (next to parse_query, which uses it) and also called from parse_single_query in
// GqlParser.cpp, so it cannot be a file-static in either.

namespace ragedb::gql {

struct GqlQuery;

/// Bind the RETURN/WITH projection's output aliases into the ORDER BY sort-key scope by substituting them
/// with their defining expressions.
void resolve_order_by_aliases(GqlQuery& query);

}  // namespace ragedb::gql

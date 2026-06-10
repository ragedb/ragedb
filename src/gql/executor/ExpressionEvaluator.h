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

#ifndef RAGEDB_EXPRESSION_EVALUATOR_H
#define RAGEDB_EXPRESSION_EVALUATOR_H

#include <vector>
#include <map>
#include "GqlAst.h"
#include "GqlValue.h"

/**
 * @brief Utilities for checking, extracting, and evaluating GQL expressions and aggregations.
 * 
 * Example Query utilizing ExpressionEvaluator:
 *   MATCH (p:Person)
 *   RETURN p.city, count(*), avg(p.age)
 * 
 * 1. Detection: has_aggregates is used to determine if return items or sort specs
 *    contain aggregate expressions (like count(*) or avg(p.age)).
 * 2. Collection: find_aggregates extracts the AggregateExpr nodes.
 * 3. Group Evaluation: evaluate_group_expression computes final values for the group's
 *    projected items by looking up the precomputed aggregate values (e.g. from the map)
 *    and evaluating remaining non-aggregate sub-expressions.
 */
namespace ragedb::gql {

bool has_aggregates(const Expression* expr);

void find_aggregates(const Expression* expr, std::vector<const AggregateExpr*>& aggregates);

GqlValue evaluate_group_expression(
    const GqlRow& representative,
    const std::map<const AggregateExpr*, GqlValue>& aggregate_results,
    const Expression* expr
);

} // namespace ragedb::gql

#endif // RAGEDB_EXPRESSION_EVALUATOR_H

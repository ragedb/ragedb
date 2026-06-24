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

#ifndef RAGEDB_GQLOPTIMIZER_H
#define RAGEDB_GQLOPTIMIZER_H

#include "GqlAst.h"

namespace ragedb {
    class Graph;
}

namespace ragedb::gql {

class GqlOptimizer {
private:
    static void extract_filters(Expression* expr, std::map<std::string, std::vector<PropertyFilter>>& pushdowns);
    static std::unique_ptr<Expression> rebuild_expression_without_pushed_predicates(std::unique_ptr<Expression> expr, const std::map<std::string, std::vector<PropertyFilter>>& pushdowns);
    static void unnest_subqueries_in_expr(std::unique_ptr<Expression>& expr, std::vector<MatchStatement>& query_matches, const std::set<std::string>& outer_vars, bool& has_unnested, int& var_counter);
    static void reverse_path_pattern(PathPattern& pattern);
    static bool has_node_index_seek(ragedb::Graph& graph, const PatternNode& node);
    static bool has_relationship_index_seek(ragedb::Graph& graph, const PatternEdge& edge);

public:
    static void optimize(GqlQuery& query);
    static void optimize(ragedb::Graph& graph, GqlQuery& query);
    static void semantic_pruning_pass(GqlQuery& query);
    static void semantic_join_elimination_pass(GqlQuery& query);
    static void relational_pruning_pass(GqlQuery& query);
    static void semantic_cardinality_limit_pass(GqlQuery& query);
    static void algebraic_rewriter_pass(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLOPTIMIZER_H

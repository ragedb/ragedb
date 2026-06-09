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

#ifndef RAGEDB_GQLTYPECHECKER_H
#define RAGEDB_GQLTYPECHECKER_H

#include "../graph/Graph.h"
#include "GqlAst.h"
#include <string>
#include <vector>
#include <set>
#include <map>
#include <memory>

namespace ragedb::gql {

enum class GqlType {
    ANY,
    VOID,
    BOOLEAN,
    INTEGER,
    DOUBLE,
    STRING,
    BOOLEAN_LIST,
    INTEGER_LIST,
    DOUBLE_LIST,
    STRING_LIST,
    NODE,
    RELATIONSHIP,
    PATH,
    RELATIONSHIP_LIST
};

std::string to_string(GqlType type);

struct VariableSchema {
    GqlType type = GqlType::ANY;
    std::set<std::string> labels;
};

class GqlTypechecker {
private:
    ragedb::Graph& graph;
    std::map<std::string, VariableSchema> env;

    std::set<std::string> all_node_labels;
    std::set<std::string> all_relationship_labels;

    std::set<std::string> evaluate_label_expr(const std::shared_ptr<LabelExpression>& label_expr, bool is_node);
    void meet_variable(const std::string& name, GqlType type, const std::set<std::string>& labels);
    
    void check_path_pattern(const PathPattern& path_pattern);
    void check_write_op(const WriteOp& write_op);
    void check_return_item(const ReturnItem& return_item);
    
    GqlType check_expression(const Expression& expr);
    GqlType get_property_type(const std::string& var_name, const std::string& prop_name);

public:
    explicit GqlTypechecker(ragedb::Graph& g);
    static void typecheck(ragedb::Graph& graph, const GqlQuery& query);
    void check_query(const GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLTYPECHECKER_H

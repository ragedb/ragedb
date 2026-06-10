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

namespace ragedb::gql {

class GqlOptimizer {
private:
    static void extract_filters(Expression* expr, std::map<std::string, std::vector<PropertyFilter>>& pushdowns);
    static std::unique_ptr<Expression> rebuild_expression_without_pushed_predicates(std::unique_ptr<Expression> expr, const std::map<std::string, std::vector<PropertyFilter>>& pushdowns);

public:
    static void optimize(GqlQuery& query);
};

} // namespace ragedb::gql

#endif // RAGEDB_GQLOPTIMIZER_H

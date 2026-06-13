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

#include "ProjectionPruner.h"

namespace ragedb::gql {

void collect_accessed_properties(const Expression* expr,
                                 std::map<std::string, std::set<std::string>>& accessed_props,
                                 std::set<std::string>& whole_objects) {
    if (!expr) return;
    switch (expr->kind) {
        case ExpressionKind::VARIABLE: {
            auto* var = static_cast<const VariableExpr*>(expr);
            whole_objects.insert(var->name);
            break;
        }
        case ExpressionKind::PROPERTY_LOOKUP: {
            auto* prop_lookup = static_cast<const PropertyLookupExpr*>(expr);
            accessed_props[prop_lookup->variable].insert(prop_lookup->property);
            break;
        }
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<const UnaryOpExpr*>(expr);
            collect_accessed_properties(un->expr.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<const BinaryOpExpr*>(expr);
            collect_accessed_properties(bin->left.get(), accessed_props, whole_objects);
            collect_accessed_properties(bin->right.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<const AggregateExpr*>(expr);
            collect_accessed_properties(agg->expr.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::IS_NULL_CHECK: {
            auto* is_null = static_cast<const IsNullExpr*>(expr);
            collect_accessed_properties(is_null->expr.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::LITERAL:
        default:
            break;
    }
}

} // namespace ragedb::gql

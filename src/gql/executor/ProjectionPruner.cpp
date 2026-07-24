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

/**
 * @brief Registers the properties a subquery's own pattern reads through its inline WHERE filters, so
 *        they survive pruning alongside the ones its residual WHERE reads.
 */
static void collect_accessed_properties_in_match(const MatchStatement& match,
                                                 std::map<std::string, std::set<std::string>>& accessed_props,
                                                 std::set<std::string>& whole_objects) {
    for (const auto& node : match.pattern.nodes) {
        collect_accessed_properties(node.where_expr.get(), accessed_props, whole_objects);
    }
    for (const auto& edge : match.pattern.edges) {
        collect_accessed_properties(edge.where_expr.get(), accessed_props, whole_objects);
    }
}

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
        case ExpressionKind::FUNCTION_CALL: {
            auto* fc = static_cast<const FunctionCallExpr*>(expr);
            for (const auto& a : fc->args) {
                collect_accessed_properties(a.get(), accessed_props, whole_objects);
            }
            break;
        }
        case ExpressionKind::CASE_WHEN: {
            auto* ce = static_cast<const CaseExpr*>(expr);
            for (const auto& b : ce->branches) {
                collect_accessed_properties(b.first.get(), accessed_props, whole_objects);
                collect_accessed_properties(b.second.get(), accessed_props, whole_objects);
            }
            collect_accessed_properties(ce->else_expr.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::IN_LIST: {
            auto* in = static_cast<const InExpr*>(expr);
            collect_accessed_properties(in->value.get(), accessed_props, whole_objects);
            collect_accessed_properties(in->list.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::CAST: {
            collect_accessed_properties(static_cast<const CastExpr*>(expr)->value.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::LIST_LITERAL: {
            for (const auto& element : static_cast<const ListExpr*>(expr)->elements) {
                collect_accessed_properties(element.get(), accessed_props, whole_objects);
            }
            break;
        }
        case ExpressionKind::LIST_INDEX: {
            auto* ie = static_cast<const IndexExpr*>(expr);
            collect_accessed_properties(ie->list.get(), accessed_props, whole_objects);
            collect_accessed_properties(ie->index.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::LIST_COMPREHENSION: {
            auto* lc = static_cast<const ListComprehensionExpr*>(expr);
            collect_accessed_properties(lc->list.get(), accessed_props, whole_objects);
            collect_accessed_properties(lc->filter.get(), accessed_props, whole_objects);
            collect_accessed_properties(lc->projection.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::QUANTIFIED_PREDICATE: {
            auto* qp = static_cast<const QuantifiedPredicateExpr*>(expr);
            collect_accessed_properties(qp->list.get(), accessed_props, whole_objects);
            collect_accessed_properties(qp->predicate.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::TEMPORAL_FIELD:
            collect_accessed_properties(static_cast<const TemporalFieldExpr*>(expr)->value.get(), accessed_props, whole_objects);
            break;
        case ExpressionKind::IS_LABELED: {
            // The label is read from the entity itself, not from a property, so the operand must survive
            // pruning as a whole object rather than as a property set.
            auto* l = static_cast<const IsLabeledExpr*>(expr);
            if (l->value && l->value->kind == ExpressionKind::VARIABLE) {
                whole_objects.insert(static_cast<const VariableExpr*>(l->value.get())->name);
            }
            collect_accessed_properties(l->value.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::IS_DIRECTED: {
            // Reads the edge's orientation from the entity, so keep it as a whole object.
            auto* d = static_cast<const IsDirectedExpr*>(expr);
            if (d->value && d->value->kind == ExpressionKind::VARIABLE) {
                whole_objects.insert(static_cast<const VariableExpr*>(d->value.get())->name);
            }
            collect_accessed_properties(d->value.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::IS_SOURCE_DEST: {
            // Compares node/edge identities and endpoints, so both operands must survive as whole objects.
            auto* s = static_cast<const IsSourceDestExpr*>(expr);
            if (s->value && s->value->kind == ExpressionKind::VARIABLE) {
                whole_objects.insert(static_cast<const VariableExpr*>(s->value.get())->name);
            }
            if (s->edge && s->edge->kind == ExpressionKind::VARIABLE) {
                whole_objects.insert(static_cast<const VariableExpr*>(s->edge.get())->name);
            }
            collect_accessed_properties(s->value.get(), accessed_props, whole_objects);
            collect_accessed_properties(s->edge.get(), accessed_props, whole_objects);
            break;
        }
        case ExpressionKind::EXISTS: {
            // A subquery predicate reads properties too. When the semi-join rewrite lifts the subquery's
            // pattern into the outer match list, its residual WHERE stays here and is evaluated against
            // the joined row, so anything it reads has to survive pruning -- otherwise the predicate sees
            // a null property and silently drops rows. Only literal comparisons become scan filters, so
            // without this an unpushable predicate (an OR, a NOT, a property-to-property comparison) is
            // the difference between a correct answer and an empty one.
            auto* ee = static_cast<const ExistsExpr*>(expr);
            collect_accessed_properties(ee->where_expr.get(), accessed_props, whole_objects);
            for (const auto& match : ee->matches) {
                collect_accessed_properties_in_match(match, accessed_props, whole_objects);
            }
            break;
        }
        case ExpressionKind::SIZE_OP: {
            // COUNT { ... } carries the same shape as EXISTS.
            auto* se = static_cast<const SizeExpr*>(expr);
            collect_accessed_properties(se->where_expr.get(), accessed_props, whole_objects);
            for (const auto& match : se->matches) {
                collect_accessed_properties_in_match(match, accessed_props, whole_objects);
            }
            break;
        }
        case ExpressionKind::LITERAL:
        default:
            break;
    }
}

} // namespace ragedb::gql

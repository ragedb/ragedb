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

#include "GqlOptimizer.h"
#include "GqlValue.h"
#include "GqlVirtualCatalog.h"
#include "GqlParser.h"
#include "optimizer/OptimizerUtils.h"
#include "optimizer/ContradictionPruner.h"
#include "optimizer/DomainConstraintReasoner.h"
#include "optimizer/JoinEliminator.h"
#include "optimizer/SubsumptionPruner.h"
#include "optimizer/CardinalityShortCircuiter.h"
#include "optimizer/AlgebraicRewriter.h"
#include "optimizer/TransitiveReachabilityPruner.h"
#include "optimizer/FunctionalDependencyPruner.h"
#include "optimizer/AutomorphicSymmetryOptimizer.h"
#include "optimizer/SchemaReachabilityPruner.h"
#include "optimizer/OptionalMatchPromoter.h"
#include "optimizer/DegreeConstraintPruner.h"
#include "optimizer/UniqueJoinEliminator.h"
#include "optimizer/LimitPushdownOptimizer.h"
#include "optimizer/TransitiveFilterPropagator.h"
#include "optimizer/EdgeContradictionPruner.h"
#include "optimizer/AntiSemiJoinPromoter.h"
#include "optimizer/EqualityJoinEliminator.h"
#include "optimizer/DisjointConceptPruner.h"
#include "optimizer/DirectionSwapOptimizer.h"
#include "optimizer/TransitivePathOptimizer.h"
#include "optimizer/IrreflexiveContradictionPruner.h"
#include "optimizer/AntisymmetricLoopCollapser.h"
#include "optimizer/EquivalenceClassOptimizer.h"
#include "../graph/Graph.h"
#include <limits>
#include <algorithm>

namespace ragedb::gql {

namespace {

/**
 * @brief Expands virtual views in a query pattern by looking up referenced view definitions in the catalog.
 * @param query The GQL query to process.
 * @return True if any virtual views were expanded, false otherwise.
 */
bool expand_virtual_views(GqlQuery& query) {
    bool expanded_any = false;
    for (auto& match : query.matches) {
        if (match.is_search || match.is_propagate) continue;
        
        std::vector<PatternNode> new_nodes;
        std::vector<PatternEdge> new_edges;
        
        for (size_t i = 0; i < match.pattern.nodes.size(); ++i) {
            auto& node = match.pattern.nodes[i];
            std::string view_name;
            if (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) {
                view_name = node.label_expr->name;
            }
            
            auto view_opt = GqlVirtualCatalog::local().get_view(view_name);
            if (view_opt.has_value()) {
                expanded_any = true;
                auto view_q = GqlParser::parse(view_opt.value());
                
                std::string view_ret_var;
                if (!view_q.returns.empty() && view_q.returns[0].expr && view_q.returns[0].expr->kind == ExpressionKind::VARIABLE) {
                    view_ret_var = static_cast<VariableExpr*>(view_q.returns[0].expr.get())->name;
                }
                
                std::string target_var = node.variable;
                if (target_var.empty()) {
                    target_var = "_v_node_" + view_name;
                }
                
                std::map<std::string, std::string> var_map;
                if (!view_ret_var.empty()) {
                    var_map[view_ret_var] = target_var;
                }
                
                std::set<std::string> view_vars;
                for (const auto& vm : view_q.matches) {
                    if (vm.is_search || vm.is_propagate) {
                        if (!vm.yield_var.empty()) view_vars.insert(vm.yield_var);
                        if (!vm.yield_score_var.empty()) view_vars.insert(vm.yield_score_var);
                        if (!vm.yield_depth_var.empty()) view_vars.insert(vm.yield_depth_var);
                    } else {
                        for (const auto& n : vm.pattern.nodes) {
                            if (!n.variable.empty()) view_vars.insert(n.variable);
                        }
                        for (const auto& e : vm.pattern.edges) {
                            if (!e.variable.empty()) view_vars.insert(e.variable);
                        }
                    }
                }
                
                int name_id = 0;
                for (const auto& var : view_vars) {
                    if (var != view_ret_var) {
                        var_map[var] = "_v_" + view_name + "_" + var + "_" + std::to_string(name_id++);
                    }
                }
                
                if (!view_q.matches.empty()) {
                    const auto& first_view_match = view_q.matches[0];
                    const auto& path = first_view_match.pattern;
                    
                    for (size_t n_idx = 0; n_idx < path.nodes.size(); ++n_idx) {
                        PatternNode v_node = path.nodes[n_idx];
                        if (!v_node.variable.empty()) {
                            v_node.variable = var_map[v_node.variable];
                        }
                        rewrite_expr_vars(v_node.where_expr, var_map);
                        
                        if (n_idx == 0) {
                            for (const auto& [pk, pv] : node.properties) {
                                v_node.properties[pk] = pv;
                            }
                            for (const auto& filter : node.property_filters) {
                                v_node.property_filters.push_back(filter);
                            }
                            if (node.where_expr) {
                                if (v_node.where_expr) {
                                    v_node.where_expr = std::make_shared<BinaryOpExpr>(BinaryOpKind::AND, v_node.where_expr->clone(), node.where_expr->clone());
                                } else {
                                    v_node.where_expr = node.where_expr->clone();
                                }
                            }
                        }
                        new_nodes.push_back(std::move(v_node));
                    }
                    
                    for (size_t e_idx = 0; e_idx < path.edges.size(); ++e_idx) {
                        PatternEdge v_edge = path.edges[e_idx];
                        if (!v_edge.variable.empty()) {
                            v_edge.variable = var_map[v_edge.variable];
                        }
                        rewrite_expr_vars(v_edge.where_expr, var_map);
                        rewrite_expr_vars(v_edge.cost_expr, var_map);
                        new_edges.push_back(std::move(v_edge));
                    }
                }
                
                if (view_q.where_expr) {
                    auto mapped_where = view_q.where_expr->clone();
                    rewrite_expr_vars(mapped_where, var_map);
                    if (query.where_expr) {
                        query.where_expr = std::make_unique<BinaryOpExpr>(BinaryOpKind::AND, std::move(query.where_expr), std::move(mapped_where));
                    } else {
                        query.where_expr = std::move(mapped_where);
                    }
                }
            } else {
                new_nodes.push_back(node);
            }
            
            if (i < match.pattern.edges.size()) {
                new_edges.push_back(match.pattern.edges[i]);
            }
        }
        
        match.pattern.nodes = std::move(new_nodes);
        match.pattern.edges = std::move(new_edges);
    }
    return expanded_any;
}

/**
 * @brief Recursively expands virtual views up to a maximum depth of 20 to handle nested views.
 * @param query The GQL query to process.
 * @param depth The current recursion depth.
 */
void expand_views_recursive(GqlQuery& query, int depth = 0) {
    if (depth > 20) return;
    bool expanded = expand_virtual_views(query);
    if (expanded) {
        expand_views_recursive(query, depth + 1);
    }
}



/**
 * @brief Unnests EXISTS subqueries in an expression and moves them to optional match patterns.
 * @param expr The expression to search for EXISTS subqueries.
 * @param query_matches The match statements list to append unnested patterns to.
 * @param outer_vars The set of variables defined in outer matches.
 * @param has_unnested Flag updated to true if any unnesting occurs.
 * @param var_counter Counter used to generate unique temporary variables.
 */
void unnest_subqueries_in_expr(
    std::unique_ptr<Expression>& expr,
    std::vector<MatchStatement>& query_matches,
    const std::set<std::string>& outer_vars,
    bool& has_unnested,
    int& var_counter
) {
    if (!expr) return;

    if (expr->kind == ExpressionKind::EXISTS) {
        auto* exists = static_cast<ExistsExpr*>(expr.get());
        if (exists->target_variable.empty()) {
            std::set<std::string> sub_vars;
            collect_variables_from_matches(exists->matches, sub_vars);
            
            std::vector<std::string> new_vars;
            for (const auto& var : sub_vars) {
                if (!outer_vars.count(var)) {
                    new_vars.push_back(var);
                }
            }

            if (new_vars.empty()) {
                std::string new_temp_var = "_exists_var_" + std::to_string(var_counter++);
                bool assigned = false;
                for (auto& match : exists->matches) {
                    for (auto& edge : match.pattern.edges) {
                        if (edge.variable.empty()) {
                            edge.variable = new_temp_var;
                            assigned = true;
                            break;
                        }
                    }
                    if (assigned) break;
                }
                if (!assigned) {
                    for (auto& match : exists->matches) {
                        for (auto& node : match.pattern.nodes) {
                            if (node.variable.empty()) {
                                node.variable = new_temp_var;
                                assigned = true;
                                break;
                            }
                        }
                        if (assigned) break;
                    }
                }
                if (assigned) {
                    new_vars.push_back(new_temp_var);
                }
            }

            if (!new_vars.empty()) {
                exists->target_variable = new_vars[0];
                has_unnested = true;
                
                if (exists->where_expr) {
                    std::map<std::string, std::vector<PropertyFilter>> sub_pushdowns;
                    extract_filters(exists->where_expr.get(), sub_pushdowns);
                    if (!sub_pushdowns.empty()) {
                        for (auto& match : exists->matches) {
                            for (auto& node : match.pattern.nodes) {
                                if (!node.variable.empty()) {
                                    auto it = sub_pushdowns.find(node.variable);
                                    if (it != sub_pushdowns.end()) {
                                        for (const auto& filter : it->second) {
                                            node.property_filters.push_back(filter);
                                        }
                                    }
                                }
                            }
                            for (auto& edge : match.pattern.edges) {
                                if (!edge.variable.empty()) {
                                    auto it = sub_pushdowns.find(edge.variable);
                                    if (it != sub_pushdowns.end()) {
                                        for (const auto& filter : it->second) {
                                            edge.property_filters.push_back(filter);
                                        }
                                    }
                                }
                            }
                        }
                        exists->where_expr = rebuild_expression_without_pushed_predicates(std::move(exists->where_expr), sub_pushdowns);
                    }
                    
                    if (exists->where_expr) {
                        unnest_subqueries_in_expr(exists->where_expr, query_matches, outer_vars, has_unnested, var_counter);
                    }
                }

                for (const auto& match : exists->matches) {
                    MatchStatement opt_match = match;
                    opt_match.is_optional = true;
                    query_matches.push_back(std::move(opt_match));
                }
            }
        }
    }

    switch (expr->kind) {
        case ExpressionKind::UNARY_OP: {
            auto* un = static_cast<UnaryOpExpr*>(expr.get());
            unnest_subqueries_in_expr(un->expr, query_matches, outer_vars, has_unnested, var_counter);
            break;
        }
        case ExpressionKind::BINARY_OP: {
            auto* bin = static_cast<BinaryOpExpr*>(expr.get());
            unnest_subqueries_in_expr(bin->left, query_matches, outer_vars, has_unnested, var_counter);
            unnest_subqueries_in_expr(bin->right, query_matches, outer_vars, has_unnested, var_counter);
            break;
        }
        case ExpressionKind::AGGREGATION: {
            auto* agg = static_cast<AggregateExpr*>(expr.get());
            if (agg->expr) {
                unnest_subqueries_in_expr(agg->expr, query_matches, outer_vars, has_unnested, var_counter);
            }
            break;
        }
        default:
            break;
    }
}

/**
 * @brief Reverses a path pattern (nodes and edges) along with edge directions.
 * @param pattern The path pattern to reverse.
 */
void reverse_path_pattern(PathPattern& pattern) {
    std::reverse(pattern.nodes.begin(), pattern.nodes.end());
    std::reverse(pattern.edges.begin(), pattern.edges.end());
    for (auto& edge : pattern.edges) {
        if (edge.direction == EdgeDirection::RIGHT) {
            edge.direction = EdgeDirection::LEFT;
        } else if (edge.direction == EdgeDirection::LEFT) {
            edge.direction = EdgeDirection::RIGHT;
        }
    }
}

/**
 * @brief Checks if a node pattern has a property filter that can utilize a schema index seek.
 * @param graph The database graph instance.
 * @param node The pattern node to check.
 * @return True if a matching node index exists, false otherwise.
 */
bool has_node_index_seek(ragedb::Graph& graph, const PatternNode& node) {
    if (!node.label_expr || node.label_expr->kind != LabelExprKind::LITERAL) {
        return false;
    }
    std::string label = node.label_expr->name;
    for (const auto& [prop, val] : node.properties) {
        if (graph.shard.local().NodeIndexExists(label, prop)) {
            return true;
        }
    }
    for (const auto& filter : node.property_filters) {
        if (filter.op == Operation::EQ && graph.shard.local().NodeIndexExists(label, filter.property)) {
            return true;
        }
    }
    return false;
}

/**
 * @brief Checks if an edge pattern has a property filter that can utilize a schema index seek.
 * @param graph The database graph instance.
 * @param edge The pattern edge to check.
 * @return True if a matching relationship index exists, false otherwise.
 */
bool has_relationship_index_seek(ragedb::Graph& graph, const PatternEdge& edge) {
    if (!edge.label_expr || edge.label_expr->kind != LabelExprKind::LITERAL) {
        return false;
    }
    std::string label = edge.label_expr->name;
    for (const auto& [prop, val] : edge.properties) {
        if (graph.shard.local().RelationshipIndexExists(label, prop)) {
            return true;
        }
    }
    for (const auto& filter : edge.property_filters) {
        if (filter.op == Operation::EQ && graph.shard.local().RelationshipIndexExists(label, filter.property)) {
            return true;
        }
    }
    return false;
}

} // namespace

/**
 * @brief Performs GQL query optimization passes including views expansion, contradiction pruning,
 * domain constraint reasoning, join elimination, subsumption pruning, cardinality short-circuiting,
 * algebraic re-writing, functional dependency pruning, and automorphic symmetry deduplication.
 * @param query The GQL query to optimize.
 */
void GqlOptimizer::optimize(GqlQuery& query) {
    if (query.schema_op.has_value()) {
        return;
    }

    if (query.skip_semantic) {
        return;
    }

    if (query.kind != QueryKind::SINGLE) {
        if (query.left) optimize(*query.left);
        if (query.right) optimize(*query.right);
        return;
    }

    // Optimize each WITH-pipeline segment (each is a sub-query with its own patterns). Every part
    // after the first segment starts from rows piped in by its predecessor, so rewrites that
    // assume they scan the pattern from scratch must know to stand down.
    for (size_t i = 0; i < query.with_segments.size(); ++i) {
        auto& seg = query.with_segments[i];
        if (seg) {
            seg->consumes_piped_rows = (i > 0);
            optimize(*seg);
        }
    }
    if (!query.with_segments.empty()) {
        query.consumes_piped_rows = true;
    }
    // A segment with no MATCH (e.g. WITH p RETURN p.name) has no pattern to optimize, and the
    // match-oriented passes below assume at least one MATCH--skip them.
    if (query.matches.empty()) {
        return;
    }

    // Phase 1: Expand any virtual views recursively up to a limit of 20.
    expand_views_recursive(query);

    // Phase 2: Apply semantic contradiction pruning to detect impossible class or property bounds early.
    ContradictionPruner::semantic_pruning_pass(query);
    if (query.no_op) return;

    // Phase 4: Run domain constraint reasoning to infer additional type/value bounds.
    DomainConstraintReasoner::domain_constraint_reasoning_pass(query);
    if (query.no_op) return;

    // Phase 7: Prune redundant traversals using transitive reachability constraints.
    TransitiveReachabilityPruner::transitive_reachability_pruning_pass(query);
    if (query.no_op) return;

    // Phase 3: Eliminate redundant joins from semantic metadata.
    JoinEliminator::semantic_join_elimination_pass(query);
    // Phase 5: Prune subsumed query conditions.
    SubsumptionPruner::semantic_subsumption_pass(query);
    // Phase 2 (Part B): Run relational pruning for contradictions in the where clause.
    ContradictionPruner::relational_pruning_pass(query);
    if (query.no_op) return;

    // Phase 6: Short-circuit query if cardinality limits (e.g. LIMIT 0) dictate it.
    CardinalityShortCircuiter::semantic_cardinality_limit_pass(query);

    // Phase 8: Rewrite patterns algebraically (e.g., path counts, algebraic simplifications).
    AlgebraicRewriter::algebraic_rewriter_pass(query);
    // Phase 9: Prune/optimize queries using functional dependency information.
    FunctionalDependencyPruner::functional_dependency_pass(query);
    // Phase 10: Deduplicate automorphic graph symmetries for counting queries.
    AutomorphicSymmetryOptimizer::automorphic_symmetry_pass(query);

    // Phase 11: Prune query matching steps that violate schema path reachability rules.
    SchemaReachabilityPruner::schema_reachability_pass(query);
    if (query.no_op) return;

    // Phase 12: Promote optional matches to regular matches if variables are null-rejected.
    OptionalMatchPromoter::optional_match_promotion_pass(query);

    // Phase 13: Prune nodes based on degree constraints before traversing.
    DegreeConstraintPruner::degree_constraint_pruning_pass(query);

    // Phase 14: Eliminate joins to unique destination nodes if only their IDs/variables are needed.
    UniqueJoinEliminator::unique_join_elimination_pass(query);

    // Phase 15: Push global query limits down to early traversal steps when safe.
    LimitPushdownOptimizer::limit_pushdown_pass(query);

    // Phase 16: Propagate constant filters transitively across equated variables.
    TransitiveFilterPropagator::transitive_filter_propagation_pass(query);

    // Phase 17: Prune query matching steps that violate relationship property constraints.
    EdgeContradictionPruner::edge_contradiction_pruning_pass(query);
    if (query.no_op) return;

    // Phase 18: Promote optional matches to anti-semi-joins if targets are null-checked.
    AntiSemiJoinPromoter::optional_match_to_antisemijoin_pass(query);

    // Phase 19: Merge redundant self-joins equated in query filters.
    EqualityJoinEliminator::equality_join_elimination_pass(query);

    // Phase 20: Short-circuit query if variable-length path category values are disjoint.
    DisjointConceptPruner::disjoint_concept_pruning_pass(query);
    if (query.no_op) return;

    // Phase 21: Reverse inner match traversal directions to start at selective index lookups.
    DirectionSwapOptimizer::direction_swap_pass(query);

    // Phase 22 (Symmetric Traversal Simplification) was removed: phase 21 already reverses any
    // match by the same selectivity criterion, and reversing is semantics-preserving for every
    // relation, so the symmetric-only gate bought nothing.

    // Phase 26: Equivalence Class Coalescing pass.
    EquivalenceClassOptimizer::equivalence_class_pass(query);

    // Phase 23: Transitive Path Pruning pass.
    TransitivePathOptimizer::transitive_path_pass(query);

    // Phase 24: Irreflexive Contradiction Pruning pass.
    IrreflexiveContradictionPruner::irreflexive_contradiction_pass(query);
    if (query.no_op) return;

    // Phase 25: Antisymmetric Loop Collapse pass.
    AntisymmetricLoopCollapser::antisymmetric_loop_pass(query);

    std::set<std::string> outer_vars;
    collect_variables_from_matches(query.matches, outer_vars);
    int var_counter = 0;
    unnest_subqueries_in_expr(query.where_expr, query.matches, outer_vars, query.has_unnested_subquery, var_counter);
    for (auto& item : query.returns) {
        unnest_subqueries_in_expr(item.expr, query.matches, outer_vars, query.has_unnested_subquery, var_counter);
    }
    for (auto& spec : query.order_by) {
        unnest_subqueries_in_expr(spec.expr, query.matches, outer_vars, query.has_unnested_subquery, var_counter);
    }
    if (query.has_unnested_subquery) {
        query.outer_vars = std::move(outer_vars);
    }

    for (auto it = query.matches.begin(); it != query.matches.end(); ) {
        if (it->is_search || it->is_propagate) {
            ++it;
            continue;
        }
        bool duplicate_found = false;
        for (auto prev_it = query.matches.begin(); prev_it != it; ++prev_it) {
            if (prev_it->is_search || prev_it->is_propagate) continue;
            if (is_equivalent_pattern(it->pattern, prev_it->pattern)) {
                if (!it->is_optional && prev_it->is_optional) {
                    prev_it->is_optional = false;
                }
                duplicate_found = true;
                break;
            }
        }
        if (duplicate_found) {
            it = query.matches.erase(it);
        } else {
            ++it;
        }
    }

    if (query.kind == QueryKind::SINGLE && query.matches.size() == 1 && query.writes.empty()) {
        auto& match = query.matches[0];
        if (!match.is_optional && match.pattern.nodes.size() == 2 && match.pattern.edges.size() == 1) {
            const auto& start_node = match.pattern.nodes[0];
            const auto& end_node = match.pattern.nodes[1];
            const auto& edge = match.pattern.edges[0];
            
            if (!start_node.variable.empty() && !edge.is_variable_length &&
                !end_node.label_expr && end_node.properties.empty() && end_node.property_filters.empty() &&
                edge.properties.empty() && edge.property_filters.empty()) {
                
                std::string start_var = start_node.variable;
                std::string end_var = end_node.variable;
                std::string edge_var = edge.variable;
                
                bool referenced_outside = false;
                if (!end_var.empty()) {
                    if (is_variable_referenced_outside_count(query.where_expr.get(), end_var)) referenced_outside = true;
                    for (const auto& spec : query.order_by) {
                        if (is_variable_referenced_outside_count(spec.expr.get(), end_var)) referenced_outside = true;
                    }
                    for (const auto& item : query.returns) {
                        if (is_variable_referenced_outside_count(item.expr.get(), end_var)) referenced_outside = true;
                    }
                }
                if (!edge_var.empty()) {
                    if (is_variable_referenced_outside_count(query.where_expr.get(), edge_var)) referenced_outside = true;
                    for (const auto& spec : query.order_by) {
                        if (is_variable_referenced_outside_count(spec.expr.get(), edge_var)) referenced_outside = true;
                    }
                    for (const auto& item : query.returns) {
                        if (is_variable_referenced_outside_count(item.expr.get(), edge_var)) referenced_outside = true;
                    }
                }
                
                // DISTINCT aggregates observe the expansion's actual rows; the degree-sum rewrite
                // (and the pattern truncation it triggers) would corrupt them. Piped-row parts
                // bind the anchor from the pipe, where the degree property is never populated.
                // A self-loop pattern (both endpoints one variable) carries a source==target join
                // the plain degree sum would drop.
                if (!referenced_outside && !query_has_distinct_aggregate(query) && !query.consumes_piped_rows &&
                    start_var != end_var) {
                    std::string degree_prop = "_degree_" + start_var + "_opt";
                    bool rewritten = false;
                    
                    for (auto& item : query.returns) {
                        if (!item.alias.has_value() && item.expr) {
                            if (item.expr->kind == ExpressionKind::AGGREGATION) {
                                auto* agg = static_cast<const AggregateExpr*>(item.expr.get());
                                if (agg->fn_kind == AggregateKind::COUNT) {
                                    if (!agg->expr) {
                                        item.alias = "count(*)";
                                    } else if (agg->expr->kind == ExpressionKind::VARIABLE) {
                                        item.alias = "count(" + static_cast<const VariableExpr*>(agg->expr.get())->name + ")";
                                    } else if (agg->expr->kind == ExpressionKind::PROPERTY_LOOKUP) {
                                        auto* pl = static_cast<const PropertyLookupExpr*>(agg->expr.get());
                                        item.alias = "count(" + pl->variable + "." + pl->property + ")";
                                    }
                                }
                            }
                        }
                    }
                    
                    for (auto& item : query.returns) {
                        rewrite_count_to_sum_degree(item.expr, start_var, end_var, edge_var, degree_prop, rewritten);
                    }
                    for (auto& spec : query.order_by) {
                        rewrite_count_to_sum_degree(spec.expr, start_var, end_var, edge_var, degree_prop, rewritten);
                    }
                    
                    if (rewritten) {
                        std::vector<std::string> rel_types;
                        extract_rel_types(edge.label_expr.get(), rel_types);
                        
                        Direction dir = Direction::BOTH;
                        if (edge.direction == EdgeDirection::RIGHT) {
                            dir = Direction::OUT;
                        } else if (edge.direction == EdgeDirection::LEFT) {
                            dir = Direction::IN;
                        } else if (edge.direction == EdgeDirection::ANY) {
                            dir = Direction::BOTH;
                        }
                        
                        DegreePopulateInfo info;
                        info.property_name = degree_prop;
                        info.direction = dir;
                        info.rel_types = std::move(rel_types);
                        match.pattern.nodes[0].degree_opt_info.push_back(std::move(info));
                        
                        match.pattern.edges.clear();
                        match.pattern.nodes.resize(1);
                    }
                }
            }
        }
    }

    for (auto& match : query.matches) {
        if (match.is_search || match.is_propagate) continue;
        for (auto& node : match.pattern.nodes) {
            if (node.where_expr) {
                std::map<std::string, std::vector<PropertyFilter>> node_pushdowns;
                extract_filters(node.where_expr.get(), node_pushdowns);
                if (!node_pushdowns.empty() && !node.variable.empty()) {
                    auto it = node_pushdowns.find(node.variable);
                    if (it != node_pushdowns.end()) {
                        for (const auto& filter : it->second) {
                            node.property_filters.push_back(filter);
                        }
                        node.where_expr = rebuild_expression_without_pushed_predicates(node.where_expr->clone(), node_pushdowns);
                    }
                }
            }
        }
        for (auto& edge : match.pattern.edges) {
            if (edge.where_expr) {
                std::map<std::string, std::vector<PropertyFilter>> edge_pushdowns;
                extract_filters(edge.where_expr.get(), edge_pushdowns);
                if (!edge_pushdowns.empty() && !edge.variable.empty()) {
                    auto it = edge_pushdowns.find(edge.variable);
                    if (it != edge_pushdowns.end()) {
                        for (const auto& filter : it->second) {
                            edge.property_filters.push_back(filter);
                        }
                        edge.where_expr = rebuild_expression_without_pushed_predicates(edge.where_expr->clone(), edge_pushdowns);
                    }
                }
            }
        }
    }

    if (!query.where_expr) return;

    std::map<std::string, std::vector<PropertyFilter>> pushdowns;
    extract_filters(query.where_expr.get(), pushdowns);

    if (pushdowns.empty()) return;

    for (auto& match : query.matches) {
        if (match.is_search || match.is_propagate) continue;
        for (auto& node : match.pattern.nodes) {
            if (!node.variable.empty()) {
                auto it = pushdowns.find(node.variable);
                if (it != pushdowns.end()) {
                    for (const auto& filter : it->second) {
                        node.property_filters.push_back(filter);
                    }
                }
            }
        }
        for (auto& edge : match.pattern.edges) {
            if (!edge.variable.empty()) {
                auto it = pushdowns.find(edge.variable);
                if (it != pushdowns.end()) {
                    for (const auto& filter : it->second) {
                        edge.property_filters.push_back(filter);
                    }
                }
            }
        }
    }

    query.where_expr = rebuild_expression_without_pushed_predicates(std::move(query.where_expr), pushdowns);

    for (auto& match : query.matches) {
        if (match.is_khop && match.pattern.nodes.size() == 2 && !match.pattern.nodes[1].variable.empty() &&
            match.pattern.nodes[1].variable != match.pattern.nodes[0].variable) {
            std::string end_var = match.pattern.nodes[1].variable;
            bool referenced_outside = false;
            if (query.where_expr && is_variable_referenced_outside_count(query.where_expr.get(), end_var)) {
                referenced_outside = true;
            }
            for (const auto& item : query.returns) {
                if (is_variable_referenced_outside_count(item.expr.get(), end_var)) {
                    referenced_outside = true;
                    break;
                }
            }
            for (const auto& spec : query.order_by) {
                if (is_variable_referenced_outside_count(spec.expr.get(), end_var)) {
                    referenced_outside = true;
                    break;
                }
            }
            
            if (!referenced_outside && !query_has_distinct_aggregate(query) && !query.consumes_piped_rows) {
                match.khop_count_only = true;
                for (auto& item : query.returns) {
                    if (!item.alias.has_value()) {
                        if (item.expr && item.expr->kind == ExpressionKind::AGGREGATION) {
                            auto* agg = static_cast<AggregateExpr*>(item.expr.get());
                            if (agg->fn_kind == AggregateKind::COUNT) {
                                if (!agg->expr) {
                                    item.alias = "count(*)";
                                } else if (agg->expr->kind == ExpressionKind::VARIABLE) {
                                    item.alias = "count(" + static_cast<VariableExpr*>(agg->expr.get())->name + ")";
                                }
                            }
                        }
                    }
                    rewrite_khop_count_to_var(item.expr, end_var);
                }
                for (auto& spec : query.order_by) {
                    rewrite_khop_count_to_var(spec.expr, end_var);
                }
            }
        }
    }
}

/**
 * @brief Performs query optimizations including database-backed index-seek direction tuning.
 * @param graph The database graph instance.
 * @param query The GQL query to optimize.
 */
namespace {

static std::string node_literal_label(const PatternNode& node) {
    return (node.label_expr && node.label_expr->kind == LabelExprKind::LITERAL) ? node.label_expr->name : "";
}
// A variable's label is often declared only on its first occurrence (`(f:Person) ... RETURN f NEXT
// MATCH (forum)->(f)`), so fall back to the label the variable carried elsewhere in the query.
static std::string node_effective_label(const PatternNode& node,
                                        const std::map<std::string, std::string>& var_labels) {
    std::string lbl = node_literal_label(node);
    if (!lbl.empty()) return lbl;
    if (!node.variable.empty()) {
        auto it = var_labels.find(node.variable);
        if (it != var_labels.end()) return it->second;
    }
    return "";
}
static std::string edge_literal_type(const PatternEdge& edge) {
    return (edge.label_expr && edge.label_expr->kind == LabelExprKind::LITERAL) ? edge.label_expr->name : "";
}
static uint64_t label_node_count(ragedb::Graph& graph, const std::string& lbl) {
    return lbl.empty() ? graph.shard.local().AllNodesCount() : graph.shard.local().NodeCount(lbl);
}

// Approximate fan-out of stepping across `edge_type` from an average node of `from_label`:
// (edges of that type) / (nodes of that label). Direction-agnostic degree estimate, floored at 1.
static double avg_edge_degree(ragedb::Graph& graph, const std::string& edge_type, const std::string& from_label) {
    uint64_t rels = edge_type.empty() ? graph.shard.local().AllRelationshipsCount()
                                      : graph.shard.local().RelationshipCount(edge_type);
    uint64_t nodes = from_label.empty() ? graph.shard.local().AllNodesCount()
                                        : graph.shard.local().NodeCount(from_label);
    if (nodes == 0) return static_cast<double>(rels ? rels : 1);
    double d = static_cast<double>(rels) / static_cast<double>(nodes);
    return d < 1.0 ? 1.0 : d;
}

// Estimated fan-out multiplier of driving `match` given the already-bound variables. A node is bound if
// its variable is in `bound` or it has a selective index seek (id lookup). Walking from a bound
// endpoint, each hop to an unbound node multiplies by that hop's avg degree; a hop to an already-bound
// node is a join constraint (selectivity < 1). With no bound endpoint the match must scan, so its cost
// is the smaller labelled endpoint's node count.
static double estimate_match_fanout(ragedb::Graph& graph, const MatchStatement& match,
                                    const std::set<std::string>& bound,
                                    const std::map<std::string, std::string>& var_labels) {
    const auto& nodes = match.pattern.nodes;
    const auto& edges = match.pattern.edges;
    size_t n = nodes.size();
    if (n < 2 || edges.size() + 1 != n) return 1.0;
    auto is_bound = [&](const PatternNode& nd) {
        return (!nd.variable.empty() && bound.count(nd.variable)) || has_node_index_seek(graph, nd);
    };
    bool front = is_bound(nodes.front());
    bool back = is_bound(nodes.back());
    if (!front && !back) {
        return static_cast<double>(std::min(label_node_count(graph, node_effective_label(nodes.front(), var_labels)),
                                            label_node_count(graph, node_effective_label(nodes.back(), var_labels))));
    }
    std::set<std::string> local = bound;
    double fanout = 1.0;
    if (front) {
        for (size_t k = 0; k + 1 < n; ++k) {
            const auto& to = nodes[k + 1];
            if (!to.variable.empty() && local.count(to.variable)) { fanout *= 0.5; }
            else {
                fanout *= avg_edge_degree(graph, edge_literal_type(edges[k]), node_effective_label(nodes[k], var_labels));
                if (!to.variable.empty()) local.insert(to.variable);
            }
        }
    } else {
        for (size_t k = n - 1; k > 0; --k) {
            const auto& to = nodes[k - 1];
            if (!to.variable.empty() && local.count(to.variable)) { fanout *= 0.5; }
            else {
                fanout *= avg_edge_degree(graph, edge_literal_type(edges[k - 1]), node_effective_label(nodes[k], var_labels));
                if (!to.variable.empty()) local.insert(to.variable);
            }
        }
    }
    return fanout;
}

static void collect_match_vars(const MatchStatement& m, std::set<std::string>& out) {
    for (const auto& nd : m.pattern.nodes) if (!nd.variable.empty()) out.insert(nd.variable);
    for (const auto& e : m.pattern.edges) if (!e.variable.empty()) out.insert(e.variable);
}

static std::set<std::string> segment_output_vars(const GqlQuery& seg) {
    std::set<std::string> out;
    for (const auto& item : seg.returns) {
        if (item.alias) out.insert(*item.alias);
        else if (item.expr && item.expr->kind == ExpressionKind::VARIABLE)
            out.insert(static_cast<const VariableExpr*>(item.expr.get())->name);
    }
    return out;
}

// Reorder a segment's MATCH statements to minimise total intermediate size, given the variables bound
// on entry (piped in). Reordering is result-preserving (the matches are conjunctive), so it only
// changes speed; applied only when a strictly cheaper order exists by a clear margin, to avoid churn
// from estimate noise. This is what lets the FoF shape run the cheap expansion first.
static void reorder_matches_by_cost(ragedb::Graph& graph, std::vector<MatchStatement>& matches,
                                    const std::set<std::string>& initial_bound,
                                    const std::map<std::string, std::string>& var_labels) {
    size_t n = matches.size();
    if (n < 2 || n > 6) return;
    for (const auto& m : matches) {
        if (m.is_search || m.is_khop || m.shortest_path_kind != ShortestPathKind::NONE ||
            !m.path_variable.empty() || m.is_optional) {
            return;  // special shapes: keep the written order
        }
    }
    auto order_cost = [&](const std::vector<size_t>& perm) {
        std::set<std::string> bound = initial_bound;
        double intermediate = 1.0, total = 0.0;
        for (size_t idx : perm) {
            intermediate *= estimate_match_fanout(graph, matches[idx], bound, var_labels);
            total += intermediate;
            collect_match_vars(matches[idx], bound);
        }
        return total;
    };
    std::vector<size_t> identity(n);
    for (size_t i = 0; i < n; ++i) identity[i] = i;
    double base_cost = order_cost(identity);
    std::vector<size_t> best = identity, perm = identity;
    double best_cost = base_cost;
    while (std::next_permutation(perm.begin(), perm.end())) {
        double c = order_cost(perm);
        if (c < best_cost) { best_cost = c; best = perm; }
    }
    if (best != identity && best_cost < base_cost * 0.9) {
        std::vector<MatchStatement> reordered;
        reordered.reserve(n);
        for (size_t idx : best) reordered.push_back(std::move(matches[idx]));
        matches = std::move(reordered);
    }
}

} // namespace

void GqlOptimizer::optimize(ragedb::Graph& graph, GqlQuery& query) {
    optimize(query);

    if (query.kind == QueryKind::SINGLE) {
        // reorder each segment's MATCHes by estimated cardinality so the cheap expansion runs
        // first (piped-frontier FoF -> the low-fan-out side), turning the rest into verifications.
        // A variable's label is often declared only on its first occurrence and reused unlabelled in a
        // later segment, so collect labels across the whole query for the degree estimates.
        std::map<std::string, std::string> var_labels;
        auto learn_labels = [&](const std::vector<MatchStatement>& ms) {
            for (const auto& m : ms) {
                for (const auto& nd : m.pattern.nodes) {
                    std::string lbl = node_literal_label(nd);
                    if (!nd.variable.empty() && !lbl.empty()) var_labels.emplace(nd.variable, lbl);
                }
            }
        };
        for (auto& seg : query.with_segments) if (seg) learn_labels(seg->matches);
        learn_labels(query.matches);

        std::set<std::string> incoming;
        for (auto& seg : query.with_segments) {
            if (seg && !seg->matches.empty()) reorder_matches_by_cost(graph, seg->matches, incoming, var_labels);
            if (seg) incoming = segment_output_vars(*seg);
        }
        if (!query.matches.empty()) reorder_matches_by_cost(graph, query.matches, incoming, var_labels);

        for (auto& match : query.matches) {
            if (match.is_search || match.is_propagate) continue;
            auto& pattern = match.pattern;
            if (pattern.nodes.size() >= 2) {
                bool start_node_idx = has_node_index_seek(graph, pattern.nodes.front());
                bool end_node_idx = has_node_index_seek(graph, pattern.nodes.back());
                
                if (end_node_idx && !start_node_idx) {
                    reverse_path_pattern(pattern);
                } else if (!start_node_idx && !end_node_idx && !pattern.edges.empty()) {
                    bool first_edge_idx = has_relationship_index_seek(graph, pattern.edges.front());
                    bool last_edge_idx = has_relationship_index_seek(graph, pattern.edges.back());
                    if (last_edge_idx && !first_edge_idx) {
                        reverse_path_pattern(pattern);
                    }
                }
            }
        }
    } else {
        if (query.left) optimize(graph, *query.left);
        if (query.right) optimize(graph, *query.right);
    }
}

} // namespace ragedb::gql

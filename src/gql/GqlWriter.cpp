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

#include "GqlWriter.h"
#include <chrono>
#include <cstdlib>

namespace ragedb::gql {

/**
 * @brief Recursively executes all write operations (INSERT, SET, REMOVE, DELETE) for a single query row.
 * 
 * Each row represents a specific match result (or a single blank row if no match clauses exist).
 * Write operations must execute sequentially within a row because operations can depend on previous 
 * writes (e.g. INSERTing a node, then SETting a property on it in the next clause).
 * 
 * Seastar's thread-per-core asynchronous model is utilized. Each operation returns a future, 
 * and continuations (.then) link the subsequent write steps.
 * 
 * @param graph The RageDB graph instance.
 * @param query_ptr Shared pointer to GQL query config (contains AST writes list).
 * @param write_idx Index of the current write operation in the query_ptr->writes array.
 * @param row The current row bindings containing variables and graph structures.
 * @return seastar::future<GqlRow> Future wrapping the updated GqlRow after all writes are applied.
 */
seastar::future<GqlRow> execute_writes_for_row(ragedb::Graph& graph, std::shared_ptr<GqlQuery> query_ptr, size_t write_idx, GqlRow row) {
    // Base Case: All write operations for this row have been executed
    if (write_idx >= query_ptr->writes.size()) {
        return seastar::make_ready_future<GqlRow>(std::move(row));
    }
    
    const auto& op = query_ptr->writes[write_idx];

    // ==========================================
    // 1. SET Operation
    // ==========================================
    // Modifies or adds a property value on a node or relationship.
    if (op.type == WriteOp::Type::SET) {
        auto val = evaluate_expression(row, op.set_expr.get());
        auto it = row.bindings.find(op.set_var);
        
        if (it != row.bindings.end()) {
            // SET property on a node
            if (it->second.type == GqlValue::NODE) {
                uint64_t id = it->second.node->getId();
                return graph.shard.local().NodeSetPropertyPeered(id, op.set_prop, val.property)
                .then([id, set_var = op.set_var, query_ptr, write_idx, row = std::move(row), &graph](bool success) mutable {
                    if (success) {
                        // Retrieve the updated node from the local/peered core to refresh internal properties
                        return graph.shard.local().NodeGetPeered(id)
                        .then([row = std::move(row), set_var](Node new_node) mutable {
                            row.bindings[set_var] = GqlValue(new_node);
                            return row;
                        });
                    }
                    return seastar::make_ready_future<GqlRow>(std::move(row));
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    // Chain the execution of the next write operation
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            } 
            // SET property on a relationship
            else if (it->second.type == GqlValue::RELATIONSHIP) {
                uint64_t id = it->second.relationship->getId();
                return graph.shard.local().RelationshipSetPropertyPeered(id, op.set_prop, val.property)
                .then([id, set_var = op.set_var, query_ptr, write_idx, row = std::move(row), &graph](bool success) mutable {
                    if (success) {
                        // Retrieve the updated relationship from the core to refresh properties
                        return graph.shard.local().RelationshipGetPeered(id)
                        .then([row = std::move(row), set_var](Relationship new_rel) mutable {
                            row.bindings[set_var] = GqlValue(new_rel);
                            return row;
                        });
                    }
                    return seastar::make_ready_future<GqlRow>(std::move(row));
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    // Chain the execution of the next write operation
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            }
        }
        // If variable is not bound, skip this SET operation
        return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(row));
    }

    // ==========================================
    // 2. REMOVE Operation
    // ==========================================
    // Deletes a property key-value pair from a node or relationship.
    if (op.type == WriteOp::Type::REMOVE) {
        auto it = row.bindings.find(op.remove_var);
        
        if (it != row.bindings.end()) {
            // REMOVE property from a node
            if (it->second.type == GqlValue::NODE) {
                uint64_t id = it->second.node->getId();
                return graph.shard.local().NodeDeletePropertyPeered(id, op.remove_prop)
                .then([id, remove_var = op.remove_var, query_ptr, write_idx, row = std::move(row), &graph](bool success) mutable {
                    if (success) {
                        // Retrieve the updated node to sync bindings with deleted properties
                        return graph.shard.local().NodeGetPeered(id)
                        .then([row = std::move(row), remove_var](Node new_node) mutable {
                            row.bindings[remove_var] = GqlValue(new_node);
                            return row;
                        });
                    }
                    return seastar::make_ready_future<GqlRow>(std::move(row));
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    // Chain next operation
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            } 
            // REMOVE property from a relationship
            else if (it->second.type == GqlValue::RELATIONSHIP) {
                uint64_t id = it->second.relationship->getId();
                return graph.shard.local().RelationshipDeletePropertyPeered(id, op.remove_prop)
                .then([id, remove_var = op.remove_var, query_ptr, write_idx, row = std::move(row), &graph](bool success) mutable {
                    if (success) {
                        // Retrieve the updated relationship to sync bindings
                        return graph.shard.local().RelationshipGetPeered(id)
                        .then([row = std::move(row), remove_var](Relationship new_rel) mutable {
                            row.bindings[remove_var] = GqlValue(new_rel);
                            return row;
                        });
                    }
                    return seastar::make_ready_future<GqlRow>(std::move(row));
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    // Chain next operation
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            }
        }
        // If variable is not bound, skip REMOVE
        return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(row));
    }

    // ==========================================
    // 3. DELETE (or DETACH DELETE) Operation
    // ==========================================
    // Deletes an element from the database. Replaces its binding with GqlValue::NIL.
    if (op.type == WriteOp::Type::DELETE_OP) {
        auto it = row.bindings.find(op.delete_var);
        
        if (it != row.bindings.end()) {
            // DELETE node
            if (it->second.type == GqlValue::NODE) {
                uint64_t id = it->second.node->getId();
                // Detach delete is implicitly supported by RageDB's NodeRemovePeered
                return graph.shard.local().NodeRemovePeered(id)
                .then([delete_var = op.delete_var, query_ptr, write_idx, row = std::move(row)](bool success) mutable {
                    if (success) {
                        row.bindings[delete_var] = GqlValue(); // Bound to NIL
                    }
                    return row;
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    // Chain next operation
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            } 
            // DELETE relationship
            else if (it->second.type == GqlValue::RELATIONSHIP) {
                uint64_t id = it->second.relationship->getId();
                return graph.shard.local().RelationshipRemovePeered(id)
                .then([delete_var = op.delete_var, query_ptr, write_idx, row = std::move(row)](bool success) mutable {
                    if (success) {
                        row.bindings[delete_var] = GqlValue(); // Bound to NIL
                    }
                    return row;
                }).then([&graph, query_ptr, write_idx](GqlRow r) {
                    // Chain next operation
                    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(r));
                });
            }
        }
        // If variable is not bound, skip DELETE
        return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(row));
    }

    // ==========================================
    // 4. INSERT Operation
    // ==========================================
    // Dynamically inserts path patterns (alternating node-edge-node structures).
    if (op.type == WriteOp::Type::INSERT) {
        // Keeps track of the asynchronous insertion execution state
        struct InsertState {
            PathPattern pattern;
            GqlRow row;
            std::vector<uint64_t> node_ids; // Maps pattern node indexes to database IDs
        };
        
        auto state = std::make_shared<InsertState>();
        state->pattern = op.insert_pattern;
        state->row = std::move(row);
        state->node_ids.resize(op.insert_pattern.nodes.size(), 0);

        // Recursive helper to insert all nodes in the pattern sequentially
        auto insert_node_func = std::make_shared<std::function<seastar::future<>(size_t)>>();
        *insert_node_func = [state, &graph, insert_node_func](size_t i) -> seastar::future<> {
            // Base case: All nodes inserted
            if (i >= state->pattern.nodes.size()) {
                return seastar::make_ready_future<>();
            }
            
            const auto& node = state->pattern.nodes[i];
            
            // Check if variable is already bound in context.
            // In GQL, inserting a pattern with an already bound variable references the existing node.
            if (!node.variable.empty()) {
                auto it = state->row.bindings.find(node.variable);
                if (it != state->row.bindings.end() && it->second.type == GqlValue::NODE) {
                    state->node_ids[i] = it->second.node->getId();
                    return (*insert_node_func)(i + 1); // Re-use node and continue
                }
            }

            // Determine unique key for the new node.
            // If the query defines a 'key' property in the pattern, use it. Otherwise, generate a time-based unique key.
            std::string key;
            auto& node_props = state->pattern.nodes[i].properties;
            auto key_it = node_props.find("key");
            if (key_it != node_props.end() && std::holds_alternative<std::string>(key_it->second)) {
                key = std::get<std::string>(key_it->second);
            } else {
                key = std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count()) + "_" + std::to_string(rand());
            }

            // Execute NodeAddPeered asynchronously on the graph
            return graph.shard.local().NodeAddPeered(node.label, key, serialize_properties_to_json(node.properties))
            .then([state, i, &graph, node_var = node.variable](uint64_t new_id) {
                state->node_ids[i] = new_id;
                // Retrieve the inserted Node object to bind to its variable name in the GqlRow
                return graph.shard.local().NodeGetPeered(new_id)
                .then([state, node_var](Node new_node) {
                    if (!node_var.empty()) {
                        state->row.bindings[node_var] = GqlValue(new_node);
                    }
                });
            }).then([i, insert_node_func]() {
                // Progress to the next node in the pattern
                return (*insert_node_func)(i + 1);
            });
        };

        // Recursive helper to insert all edges/relationships in the pattern sequentially
        auto insert_edge_func = std::make_shared<std::function<seastar::future<>(size_t)>>();
        *insert_edge_func = [state, &graph, insert_edge_func](size_t i) -> seastar::future<> {
            // Base case: All relationships inserted
            if (i >= state->pattern.edges.size()) {
                return seastar::make_ready_future<>();
            }
            
            const auto& edge = state->pattern.edges[i];
            uint64_t start = state->node_ids[i];
            uint64_t end = state->node_ids[i + 1];

            // Execute RelationshipAddPeered asynchronously on the graph
            return graph.shard.local().RelationshipAddPeered(edge.label, start, end, serialize_properties_to_json(edge.properties))
            .then([state, &graph, i, edge_var = edge.variable](uint64_t new_rel_id) {
                // Retrieve relationship properties and object to bind in GqlRow
                return graph.shard.local().RelationshipGetPeered(new_rel_id)
                .then([state, edge_var](Relationship new_rel) {
                    if (!edge_var.empty()) {
                        state->row.bindings[edge_var] = GqlValue(new_rel);
                    }
                });
            }).then([i, insert_edge_func]() {
                // Progress to the next edge in the pattern
                return (*insert_edge_func)(i + 1);
            });
        };

        // 1. Insert all nodes in the path pattern sequentially
        return (*insert_node_func)(0)
        .then([insert_edge_func]() {
            // 2. Insert all relationships connecting the nodes sequentially
            return (*insert_edge_func)(0);
        }).then([state, query_ptr, write_idx, &graph, insert_node_func, insert_edge_func]() mutable {
            // 3. Complete this INSERT operation, then move to the next GQL write clause
            return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(state->row));
        });
    }

    // Default fallback: Skip unknown operations and advance
    return execute_writes_for_row(graph, query_ptr, write_idx + 1, std::move(row));
}

} // namespace ragedb::gql

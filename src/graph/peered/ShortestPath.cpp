/*
 * Copyright Max De Marzi. All Rights Reserved.
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

#include "../Shard.h"
#include <queue>

namespace ragedb {

    seastar::future<std::optional<Path>> Shard::ShortestPathPeered(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types, uint64_t max_hops) {
        if (id == id2) {
            auto nodes = co_await NodesGetPeered({id});
            co_return Path(nodes);
        }

        bool valid_start = co_await container().invoke_on(CalculateShardId(id), [id](Shard &local_shard) { return local_shard.ValidNodeId(id); });
        bool valid_end = co_await container().invoke_on(CalculateShardId(id2), [id2](Shard &local_shard) { return local_shard.ValidNodeId(id2); });
        if (!valid_start || !valid_end) {
            co_return std::nullopt;
        }

        std::unordered_map<uint64_t, Link> parent_forward;
        std::unordered_map<uint64_t, Link> parent_backward;

        std::vector<uint64_t> current_level_forward = { id };
        std::vector<uint64_t> current_level_backward = { id2 };

        parent_forward.emplace(id, Link(0, 0));
        parent_backward.emplace(id2, Link(0, 0));

        bool found = false;
        uint64_t meeting_node = 0;
        uint64_t hops_forward = 0;
        uint64_t hops_backward = 0;

        auto reverse_direction = [](Direction dir) {
            if (dir == Direction::OUT) return Direction::IN;
            if (dir == Direction::IN) return Direction::OUT;
            return Direction::BOTH;
        };

        while (!current_level_forward.empty() && !current_level_backward.empty() && !found) {
            if (hops_forward + hops_backward >= max_hops) {
                break;
            }

            if (current_level_forward.size() <= current_level_backward.size()) {
                hops_forward++;
                std::vector<seastar::future<std::vector<Link>>> futures;
                for (uint64_t node_id : current_level_forward) {
                    if (rel_types.empty()) {
                        futures.push_back(NodeGetLinksPeered(node_id, direction));
                    } else {
                        futures.push_back(NodeGetLinksPeered(node_id, direction, rel_types));
                    }
                }
                
                std::vector<std::vector<Link>> links_results = co_await seastar::when_all_succeed(futures.begin(), futures.end());
                
                std::vector<uint64_t> next_level;
                for (size_t i = 0; i < current_level_forward.size(); ++i) {
                    uint64_t parent_id = current_level_forward[i];
                    for (const Link& link : links_results[i]) {
                        uint64_t neighbor_id = link.node_id;
                        if (parent_forward.find(neighbor_id) == parent_forward.end()) {
                            parent_forward.emplace(neighbor_id, Link(parent_id, link.rel_id));
                            next_level.push_back(neighbor_id);
                            if (parent_backward.find(neighbor_id) != parent_backward.end()) {
                                found = true;
                                meeting_node = neighbor_id;
                                break;
                            }
                        }
                    }
                    if (found) break;
                }
                current_level_forward = std::move(next_level);
            } else {
                hops_backward++;
                Direction rev_dir = reverse_direction(direction);
                std::vector<seastar::future<std::vector<Link>>> futures;
                for (uint64_t node_id : current_level_backward) {
                    if (rel_types.empty()) {
                        futures.push_back(NodeGetLinksPeered(node_id, rev_dir));
                    } else {
                        futures.push_back(NodeGetLinksPeered(node_id, rev_dir, rel_types));
                    }
                }
                
                std::vector<std::vector<Link>> links_results = co_await seastar::when_all_succeed(futures.begin(), futures.end());
                
                std::vector<uint64_t> next_level;
                for (size_t i = 0; i < current_level_backward.size(); ++i) {
                    uint64_t parent_id = current_level_backward[i];
                    for (const Link& link : links_results[i]) {
                        uint64_t neighbor_id = link.node_id;
                        if (parent_backward.find(neighbor_id) == parent_backward.end()) {
                            parent_backward.emplace(neighbor_id, Link(parent_id, link.rel_id));
                            next_level.push_back(neighbor_id);
                            if (parent_forward.find(neighbor_id) != parent_forward.end()) {
                                found = true;
                                meeting_node = neighbor_id;
                                break;
                            }
                        }
                    }
                    if (found) break;
                }
                current_level_backward = std::move(next_level);
            }
            co_await seastar::coroutine::maybe_yield();
        }

        if (found) {
            std::vector<uint64_t> node_ids_forward;
            std::vector<uint64_t> rel_ids_forward;
            uint64_t curr = meeting_node;
            while (curr != id) {
                node_ids_forward.push_back(curr);
                Link edge = parent_forward.at(curr);
                rel_ids_forward.push_back(edge.rel_id);
                curr = edge.node_id;
            }
            node_ids_forward.push_back(id);
            std::reverse(node_ids_forward.begin(), node_ids_forward.end());
            std::reverse(rel_ids_forward.begin(), rel_ids_forward.end());

            std::vector<uint64_t> node_ids_backward;
            std::vector<uint64_t> rel_ids_backward;
            curr = meeting_node;
            while (curr != id2) {
                Link edge = parent_backward.at(curr);
                curr = edge.node_id;
                node_ids_backward.push_back(curr);
                rel_ids_backward.push_back(edge.rel_id);
            }

            std::vector<uint64_t> node_ids = std::move(node_ids_forward);
            node_ids.insert(node_ids.end(), node_ids_backward.begin(), node_ids_backward.end());

            std::vector<uint64_t> rel_ids = std::move(rel_ids_forward);
            rel_ids.insert(rel_ids.end(), rel_ids_backward.begin(), rel_ids_backward.end());

            auto nodes = co_await NodesGetPeered(node_ids);
            auto relationships = co_await RelationshipsGetPeered(rel_ids);

            std::unordered_map<uint64_t, Node> node_map;
            for (auto&& node : nodes) {
                node_map.emplace(node.getId(), std::move(node));
            }
            std::vector<Node> ordered_nodes;
            ordered_nodes.reserve(node_ids.size());
            for (uint64_t nid : node_ids) {
                auto it = node_map.find(nid);
                if (it != node_map.end()) {
                    ordered_nodes.push_back(std::move(it->second));
                }
            }

            std::unordered_map<uint64_t, Relationship> rel_map;
            for (auto&& rel : relationships) {
                rel_map.emplace(rel.getId(), std::move(rel));
            }
            std::vector<Relationship> ordered_rels;
            ordered_rels.reserve(rel_ids.size());
            for (uint64_t rid : rel_ids) {
                auto it = rel_map.find(rid);
                if (it != rel_map.end()) {
                    ordered_rels.push_back(std::move(it->second));
                }
            }

            co_return Path(ordered_nodes, ordered_rels);
        } else {
            co_return std::nullopt;
        }
    }

    seastar::future<std::optional<WeightedPath>> Shard::ShortestWeightedPathPeered(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types) {
        if (id == id2) {
            auto nodes = co_await NodesGetPeered({id});
            co_return WeightedPath(nodes, {}, 0.0);
        }

        bool valid_start = co_await container().invoke_on(CalculateShardId(id), [id](Shard &local_shard) { return local_shard.ValidNodeId(id); });
        bool valid_end = co_await container().invoke_on(CalculateShardId(id2), [id2](Shard &local_shard) { return local_shard.ValidNodeId(id2); });
        if (!valid_start || !valid_end) {
            co_return std::nullopt;
        }

        std::unordered_map<uint64_t, double> distances;
        std::unordered_map<uint64_t, Link> parent;
        
        // Priority queue of (distance, node_id)
        std::priority_queue<std::pair<double, uint64_t>, std::vector<std::pair<double, uint64_t>>, std::greater<std::pair<double, uint64_t>>> pq;
        
        distances[id] = 0.0;
        pq.push({0.0, id});
        
        bool found = false;
        
        while (!pq.empty()) {
            auto [d, u] = pq.top();
            pq.pop();
            
            if (u == id2) {
                found = true;
                break;
            }
            
            if (d > distances[u]) {
                continue;
            }
            
            std::vector<Link> links;
            if (rel_types.empty()) {
                links = co_await NodeGetLinksPeered(u, direction);
            } else {
                links = co_await NodeGetLinksPeered(u, direction, rel_types);
            }
            
            // Fetch weights of the relationships in bulk
            std::vector<uint64_t> rel_ids;
            rel_ids.reserve(links.size());
            for (const auto& link : links) {
                rel_ids.push_back(link.rel_id);
            }
            
            std::map<uint64_t, property_type_t> weights_map = co_await RelationshipsGetPropertyPeered(rel_ids, "weight");
            
            auto get_weight = [](const property_type_t& val) -> double {
                if (std::holds_alternative<double>(val)) {
                    return std::get<double>(val);
                } else if (std::holds_alternative<int64_t>(val)) {
                    return static_cast<double>(std::get<int64_t>(val));
                }
                return 1.0;
            };
            
            for (const auto& link : links) {
                double weight = 1.0;
                auto it = weights_map.find(link.rel_id);
                if (it != weights_map.end()) {
                    weight = get_weight(it->second);
                }
                
                double next_dist = d + weight;
                uint64_t v = link.node_id;
                
                if (distances.find(v) == distances.end() || next_dist < distances[v]) {
                    distances[v] = next_dist;
                    parent.insert_or_assign(v, Link(u, link.rel_id));
                    pq.push({next_dist, v});
                }
            }
            co_await seastar::coroutine::maybe_yield();
        }

        if (found) {
            std::vector<uint64_t> node_ids;
            std::vector<uint64_t> rel_ids;
            uint64_t curr = id2;
            while (curr != id) {
                node_ids.push_back(curr);
                Link edge = parent.at(curr);
                rel_ids.push_back(edge.rel_id);
                curr = edge.node_id;
            }
            node_ids.push_back(id);
            std::reverse(node_ids.begin(), node_ids.end());
            std::reverse(rel_ids.begin(), rel_ids.end());
            
            auto nodes = co_await NodesGetPeered(node_ids);
            auto relationships = co_await RelationshipsGetPeered(rel_ids);

            std::unordered_map<uint64_t, Node> node_map;
            for (auto&& node : nodes) {
                node_map.emplace(node.getId(), std::move(node));
            }
            std::vector<Node> ordered_nodes;
            ordered_nodes.reserve(node_ids.size());
            for (uint64_t nid : node_ids) {
                auto it = node_map.find(nid);
                if (it != node_map.end()) {
                    ordered_nodes.push_back(std::move(it->second));
                }
            }

            std::unordered_map<uint64_t, Relationship> rel_map;
            for (auto&& rel : relationships) {
                rel_map.emplace(rel.getId(), std::move(rel));
            }
            std::vector<Relationship> ordered_rels;
            ordered_rels.reserve(rel_ids.size());
            for (uint64_t rid : rel_ids) {
                auto it = rel_map.find(rid);
                if (it != rel_map.end()) {
                    ordered_rels.push_back(std::move(it->second));
                }
            }

            double path_weight = distances[id2];
            co_return WeightedPath(ordered_nodes, ordered_rels, path_weight);
        } else {
            co_return std::nullopt;
        }
    }

    seastar::future<std::vector<Path>> Shard::ShortestPathsPeered(uint64_t id, uint64_t id2, Direction direction, std::vector<std::string> rel_types, uint64_t min_hops, uint64_t max_hops, ShortestPathKind kind, uint64_t k) {
        // If the start and end nodes are identical, and 0 hops is allowed by min_hops,
        // return a single-node path of length 0.
        if (id == id2) {
            if (min_hops == 0) {
                auto nodes = co_await NodesGetPeered({id});
                co_return std::vector<Path>{ Path(nodes) };
            }
        }

        // Validate the starting and ending node IDs on their respective shards.
        bool valid_start = co_await container().invoke_on(CalculateShardId(id), [id](Shard &local_shard) { return local_shard.ValidNodeId(id); });
        bool valid_end = co_await container().invoke_on(CalculateShardId(id2), [id2](Shard &local_shard) { return local_shard.ValidNodeId(id2); });
        if (!valid_start || !valid_end) {
            co_return std::vector<Path>{};
        }

        // Helper struct representing the intermediate traversal state of a path.
        struct PathState {
            std::vector<uint64_t> node_ids;
            std::vector<uint64_t> rel_ids;
        };

        // Initialize the queue with the starting node path.
        std::vector<PathState> current_paths = { PathState{{id}, {}} };
        std::vector<PathState> found_paths;
        uint64_t groups_found = 0; // Tracks levels/depth groups containing valid shortest paths.

        // BFS traversal, expanding level-by-level up to the maximum permitted depth.
        for (uint64_t depth = 1; depth <= max_hops; ++depth) {
            if (current_paths.empty()) {
                break;
            }

            // Asynchronously fetch links for the last node of each active path at this level.
            std::vector<seastar::future<std::vector<Link>>> futures;
            for (const auto& path : current_paths) {
                uint64_t last_node = path.node_ids.back();
                futures.push_back(NodeGetLinksPeered(last_node, direction, rel_types));
            }

            // Wait for all peered link queries to complete.
            std::vector<std::vector<Link>> links_results = co_await seastar::when_all_succeed(futures.begin(), futures.end());

            std::vector<PathState> next_paths;
            std::vector<PathState> paths_this_level;

            // Expand paths using the retrieved adjacent links.
            for (size_t i = 0; i < current_paths.size(); ++i) {
                const auto& path = current_paths[i];
                for (const auto& link : links_results[i]) {
                    // Trail semantics enforcement: avoid traversing the same relationship twice in a single path.
                    if (std::find(path.rel_ids.begin(), path.rel_ids.end(), link.rel_id) != path.rel_ids.end()) {
                        continue;
                    }

                    PathState new_path = path;
                    new_path.node_ids.push_back(link.node_id);
                    new_path.rel_ids.push_back(link.rel_id);

                    // If the neighbor is the destination node and we satisfy min_hops, record it as a found path.
                    if (link.node_id == id2 && depth >= min_hops) {
                        paths_this_level.push_back(new_path);
                    } else {
                        // Otherwise, queue it for expansion at the next BFS level.
                        next_paths.push_back(std::move(new_path));
                    }
                }
            }

            // If we found any valid shortest paths at the current level:
            if (!paths_this_level.empty()) {
                groups_found++;
                found_paths.insert(found_paths.end(), paths_this_level.begin(), paths_this_level.end());

                // Evaluate early-termination conditions based on the requested ShortestPathKind.
                // ANY / ALL: BFS guarantees these paths of minimal length are the shortest, so stop.
                if (kind == ShortestPathKind::ANY) {
                    break;
                }
                if (kind == ShortestPathKind::ALL) {
                    break;
                }
                // K: Stop if we have accumulated at least k paths.
                if (kind == ShortestPathKind::K && found_paths.size() >= k) {
                    break;
                }
                // K_GROUP: Stop if we have found paths spanning at least k distinct depth levels.
                if (kind == ShortestPathKind::K_GROUP && groups_found >= k) {
                    break;
                }
            }

            current_paths = std::move(next_paths);
            // Yield control to allow other Seastar fibers to execute.
            co_await seastar::coroutine::maybe_yield();
        }

        // Apply any post-traversal constraints on the results size.
        if (kind == ShortestPathKind::K && found_paths.size() > k) {
            found_paths.resize(k);
        }
        if (kind == ShortestPathKind::ANY && found_paths.size() > 1) {
            found_paths.resize(1);
        }

        // Bulk-retrieve all traversed node and relationship records to assemble the final Path objects.
        std::vector<Path> results;
        for (const auto& path_state : found_paths) {
            auto nodes = co_await NodesGetPeered(path_state.node_ids);
            auto rels = co_await RelationshipsGetPeered(path_state.rel_ids);
            
            // Reconstruct the node sequence in traversal order.
            std::unordered_map<uint64_t, Node> node_map;
            for (auto&& node : nodes) {
                node_map.emplace(node.getId(), std::move(node));
            }
            std::vector<Node> ordered_nodes;
            for (uint64_t nid : path_state.node_ids) {
                ordered_nodes.push_back(std::move(node_map.at(nid)));
            }

            // Reconstruct the relationship sequence in traversal order.
            std::unordered_map<uint64_t, Relationship> rel_map;
            for (auto&& rel : rels) {
                rel_map.emplace(rel.getId(), std::move(rel));
            }
            std::vector<Relationship> ordered_rels;
            for (uint64_t rid : path_state.rel_ids) {
                ordered_rels.push_back(std::move(rel_map.at(rid)));
            }

            results.push_back(Path(ordered_nodes, ordered_rels));
        }

        co_return results;
    }

}

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

        std::unordered_map<uint64_t, Link> parent;
        std::vector<uint64_t> current_level = { id };
        parent.emplace(id, Link(0, 0));

        bool found = false;
        uint64_t hops = 0;

        while (!current_level.empty() && !found && hops < max_hops) {
            hops++;
            std::vector<seastar::future<std::vector<Link>>> futures;
            for (uint64_t node_id : current_level) {
                if (rel_types.empty()) {
                    futures.push_back(NodeGetLinksPeered(node_id, direction));
                } else {
                    futures.push_back(NodeGetLinksPeered(node_id, direction, rel_types));
                }
            }
            
            std::vector<std::vector<Link>> links_results = co_await seastar::when_all_succeed(futures.begin(), futures.end());
            
            std::vector<uint64_t> next_level;
            for (size_t i = 0; i < current_level.size(); ++i) {
                uint64_t parent_id = current_level[i];
                for (const Link& link : links_results[i]) {
                    uint64_t neighbor_id = link.node_id;
                    if (parent.find(neighbor_id) == parent.end()) {
                        parent.emplace(neighbor_id, Link(parent_id, link.rel_id));
                        next_level.push_back(neighbor_id);
                        if (neighbor_id == id2) {
                            found = true;
                            break;
                        }
                    }
                }
                if (found) break;
            }
            current_level = std::move(next_level);
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

}

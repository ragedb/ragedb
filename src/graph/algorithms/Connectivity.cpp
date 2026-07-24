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
#include <map>
#include <vector>
#include <unordered_set>

namespace ragedb {

    // GetGraphEdges: Standardizes graph loading by translating both direct relationship schemas
    // and intermediate node pattern schemas (Pattern 3) into a single vector of Edge structs.
    std::vector<Shard::Edge> Shard::GetGraphEdges(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        std::vector<Edge> edges;

        // Pattern 1 & 2: Direct relationships between nodes
        if (edge_concept.empty()) {
            uint64_t max_rels = AllRelationshipsCountPeered(rel_type).get0();
            std::vector<uint64_t> rel_ids = AllRelationshipIdsPeered(rel_type, 0, max_rels).get0();
            std::vector<Relationship> rels = RelationshipsGetPeered(rel_ids).get0();

            for (const auto& rel : rels) {
                double w = 1.0;
                if (weighted && !weight_prop.empty()) {
                    auto prop = rel.getProperty(weight_prop);
                    if (prop.index() != 0) {
                        if (std::holds_alternative<double>(prop)) {
                            w = std::get<double>(prop);
                        } else if (std::holds_alternative<int64_t>(prop)) {
                            w = static_cast<double>(std::get<int64_t>(prop));
                        }
                    }
                }
                edges.push_back({rel.getStartingNodeId(), rel.getEndingNodeId(), w});
            }
        } 
        // Pattern 3: Intermediate Node representation
        else {
            uint64_t max_nodes = AllNodesCountPeered(edge_concept).get0();
            std::vector<uint64_t> edge_node_ids = AllNodeIdsPeered(edge_concept, 0, max_nodes).get0();

            for (uint64_t e : edge_node_ids) {
                // Find node references on both ends of the intermediate edge node
                auto src_neighbors = NodeGetLinksPeered(e, Direction::BOTH, src_rel).get0();
                auto dst_neighbors = NodeGetLinksPeered(e, Direction::BOTH, dst_rel).get0();

                if (!src_neighbors.empty() && !dst_neighbors.empty()) {
                    uint64_t src_id = src_neighbors[0].getNodeId();
                    uint64_t dst_id = dst_neighbors[0].getNodeId();

                    double w = 1.0;
                    if (weighted && !weight_prop.empty()) {
                        auto node_opt = NodeGetPeered(e).get0();
                        auto prop = node_opt.getProperty(weight_prop);
                        if (prop.index() != 0) {
                            if (std::holds_alternative<double>(prop)) {
                                w = std::get<double>(prop);
                            } else if (std::holds_alternative<int64_t>(prop)) {
                                w = static_cast<double>(std::get<int64_t>(prop));
                            }
                        }
                    }
                    edges.push_back({src_id, dst_id, w});
                }
            }
        }
        return edges;
    }

    // Reachable: BFS starting from each node to identify all reachable (src, dst) pairs.
    std::vector<std::pair<uint64_t, uint64_t>> Shard::ReachablePeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        // Build internal adjacency lookup map. This runs synchronously inside a seastar::thread
        // (the caller reached it through .get0()); yield periodically so the O(E) build and the
        // O(V*(V+E)) BFS sweep below do not monopolise the reactor for their whole duration.
        std::map<uint64_t, std::vector<uint64_t>> adj;
        uint64_t work = 0;
        for (const auto& e : edges) {
            adj[e.src].push_back(e.dst);
            if (!directed) {
                adj[e.dst].push_back(e.src);
            }
            if ((++work & 0xFFFF) == 0) seastar::thread::maybe_yield();
        }

        std::vector<std::pair<uint64_t, uint64_t>> reachable_pairs;

        // Run BFS for every node to establish reachability
        for (uint64_t s : nodes) {
            std::unordered_set<uint64_t> visited;
            std::queue<uint64_t> Q;
            Q.push(s);
            visited.insert(s);

            while (!Q.empty()) {
                uint64_t u = Q.front();
                Q.pop();

                for (uint64_t v : adj[u]) {
                    if (!visited.count(v)) {
                        visited.insert(v);
                        Q.push(v);
                        reachable_pairs.push_back({s, v});
                        if ((++work & 0xFFFF) == 0) seastar::thread::maybe_yield();
                    }
                }
            }
            seastar::thread::maybe_yield();
        }
        return reachable_pairs;
    }

    // WeaklyConnectedComponents: Partitions nodes using a Union-Find tree structure.
    std::map<uint64_t, uint64_t> Shard::WeaklyConnectedComponentsPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        std::map<uint64_t, uint64_t> parent;
        for (uint64_t u : nodes) parent[u] = u;

        // Find root helper with path compression
        auto find_root = [&](uint64_t i) {
            uint64_t root = i;
            while (parent[root] != root) {
                root = parent[root];
            }
            uint64_t curr = i;
            while (curr != root) {
                uint64_t nxt = parent[curr];
                parent[curr] = root;
                curr = nxt;
            }
            return root;
        };

        // Union sets helper
        auto union_sets = [&](uint64_t i, uint64_t j) {
            uint64_t root_i = find_root(i);
            uint64_t root_j = find_root(j);
            if (root_i != root_j) {
                if (root_i < root_j) {
                    parent[root_j] = root_i;
                } else {
                    parent[root_i] = root_j;
                }
            }
        };

        // Process all edges. Runs in a seastar::thread (caller used .get0()); yield periodically so
        // the O(E) union pass and the O(V) gather below stay cooperative on large graphs.
        uint64_t work = 0;
        for (const auto& e : edges) {
            union_sets(e.src, e.dst);
            if ((++work & 0xFFFF) == 0) seastar::thread::maybe_yield();
        }

        // Gather components
        std::map<uint64_t, uint64_t> wcc;
        for (uint64_t u : nodes) {
            wcc[u] = find_root(u);
            if ((++work & 0xFFFF) == 0) seastar::thread::maybe_yield();
        }
        return wcc;
    }

    // IsConnected: True if the entire graph forms a single weakly connected component.
    bool Shard::IsConnectedPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto wcc = WeaklyConnectedComponentsPeered(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        if (wcc.empty()) return true;

        std::unordered_set<uint64_t> unique_components;
        for (const auto& pair : wcc) {
            unique_components.insert(pair.second);
        }
        return unique_components.size() <= 1;
    }

}

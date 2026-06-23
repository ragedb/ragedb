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
#include <stack>
#include <cmath>
#include <map>
#include <vector>

namespace ragedb {

    // PageRank: Measures importance by modeling a user walking edges with a damping probability.
    std::map<uint64_t, double> Shard::PageRankPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted, double damping, uint64_t iterations, double tolerance) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        std::map<uint64_t, double> pr;
        if (nodes.empty()) return pr;

        // Initialize PageRank uniformly
        double initial_value = 1.0 / nodes.size();
        for (uint64_t id : nodes) pr[id] = initial_value;

        // Build adjacency lists and degree sums
        std::map<uint64_t, std::vector<std::pair<uint64_t, double>>> adj;
        std::map<uint64_t, double> out_weights;
        for (const auto& e : edges) {
            adj[e.src].push_back({e.dst, e.weight});
            out_weights[e.src] += e.weight;
            if (!directed) {
                adj[e.dst].push_back({e.src, e.weight});
                out_weights[e.dst] += e.weight;
            }
        }

        // Run PageRank power iteration loops
        for (uint64_t iter = 0; iter < iterations; ++iter) {
            std::map<uint64_t, double> next_pr;
            double sink_value = 0.0;
            double constant = (1.0 - damping) / nodes.size();

            // Setup default damping values and compile sink node credits
            for (uint64_t id : nodes) {
                next_pr[id] = constant;
                if (out_weights[id] == 0.0) {
                    sink_value += damping * pr[id] / nodes.size();
                }
            }

            // Distribute credits across connected neighbors
            for (uint64_t u : nodes) {
                for (const auto& edge : adj[u]) {
                    uint64_t v = edge.first;
                    double w = edge.second;
                    next_pr[v] += damping * pr[u] * w / out_weights[u];
                }
            }

            // Compute delta difference to check convergence
            double diff = 0.0;
            for (uint64_t id : nodes) {
                next_pr[id] += sink_value;
                diff += std::abs(next_pr[id] - pr[id]);
            }

            pr = std::move(next_pr);
            if (diff < tolerance) break;
        }

        return pr;
    }

    // Eigenvector Centrality: Measures node influence recursively (nodes get higher score if connected to high-scoring nodes).
    std::map<uint64_t, double> Shard::EigenvectorCentralityPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted, uint64_t iterations, double tolerance) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        std::map<uint64_t, double> x;
        for (uint64_t id : nodes) x[id] = 1.0;

        std::map<uint64_t, std::vector<std::pair<uint64_t, double>>> adj;
        for (const auto& e : edges) {
            adj[e.src].push_back({e.dst, e.weight});
            if (!directed) {
                adj[e.dst].push_back({e.src, e.weight});
            }
        }

        // Run iterations using power method
        for (uint64_t iter = 0; iter < iterations; ++iter) {
            std::map<uint64_t, double> next_x;
            double norm = 0.0;

            for (uint64_t u : nodes) {
                double sum = 0.0;
                for (const auto& edge : adj[u]) {
                    sum += x[edge.first] * edge.second;
                }
                next_x[u] = sum;
                norm += sum * sum;
            }

            // Normalize vector lengths
            norm = std::sqrt(norm);
            if (norm == 0.0) break;

            double diff = 0.0;
            for (uint64_t id : nodes) {
                double val = next_x[id] / norm;
                diff += std::abs(val - x[id]);
                x[id] = val;
            }

            if (diff < tolerance) break;
        }

        return x;
    }

    // Betweenness Centrality: Brandes' algorithm counting shortest paths passing through a node.
    std::map<uint64_t, double> Shard::BetweennessCentralityPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        std::map<uint64_t, double> BC;
        for (uint64_t id : nodes) BC[id] = 0.0;

        std::map<uint64_t, std::vector<uint64_t>> adj;
        for (const auto& e : edges) {
            adj[e.src].push_back(e.dst);
            if (!directed) {
                adj[e.dst].push_back(e.src);
            }
        }

        // Run Brandes shortest-path accumulation for each source node
        for (uint64_t s : nodes) {
            std::stack<uint64_t> S;
            std::map<uint64_t, std::vector<uint64_t>> P;
            std::map<uint64_t, double> sigma;
            std::map<uint64_t, int> d;

            for (uint64_t w : nodes) {
                sigma[w] = 0.0;
                d[w] = -1;
            }

            sigma[s] = 1.0;
            d[s] = 0;

            std::queue<uint64_t> Q;
            Q.push(s);

            while (!Q.empty()) {
                uint64_t v = Q.front();
                Q.pop();
                S.push(v);

                for (uint64_t w : adj[v]) {
                    if (d[w] < 0) {
                        Q.push(w);
                        d[w] = d[v] + 1;
                    }
                    if (d[w] == d[v] + 1) {
                        sigma[w] += sigma[v];
                        P[w].push_back(v);
                    }
                }
            }

            std::map<uint64_t, double> delta;
            for (uint64_t w : nodes) delta[w] = 0.0;

            // Accumulate dependencies backwards
            while (!S.empty()) {
                uint64_t w = S.top();
                S.pop();

                for (uint64_t v : P[w]) {
                    delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
                }
                if (w != s) {
                    BC[w] += delta[w];
                }
            }
        }

        // Normalize if undirected (since shortest paths are counted in both directions)
        if (!directed) {
            for (auto& pair : BC) pair.second /= 2.0;
        }

        return BC;
    }

    // Degree Centrality: Fraction of nodes connected to each node.
    std::map<uint64_t, double> Shard::DegreeCentralityPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted, Direction direction) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        std::map<uint64_t, double> deg;
        for (uint64_t id : nodes) deg[id] = 0.0;

        for (const auto& e : edges) {
            if (direction == Direction::BOTH) {
                deg[e.src] += 1.0;
                deg[e.dst] += 1.0;
            } else if (direction == Direction::OUT) {
                deg[e.src] += 1.0;
            } else if (direction == Direction::IN) {
                deg[e.dst] += 1.0;
            }
        }

        double denominator = nodes.size() > 1 ? static_cast<double>(nodes.size() - 1) : 1.0;
        for (uint64_t id : nodes) {
            deg[id] /= denominator;
        }

        return deg;
    }

}

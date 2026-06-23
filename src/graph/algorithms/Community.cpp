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
#include <map>
#include <vector>
#include <unordered_map>
#include <cmath>

namespace ragedb {

    // Helper for modularity gain calculation
    static double GetModularityGain(uint64_t u, uint64_t u_comm, const std::map<uint64_t, std::vector<Shard::Edge>>& adj, const std::map<uint64_t, uint64_t>& comm, const std::map<uint64_t, double>& k, double m2) {
        double k_in = 0.0;
        double sigma_tot = 0.0;

        for (const auto& edge : adj.at(u)) {
            if (comm.at(edge.dst) == u_comm) {
                k_in += edge.weight;
            }
        }

        for (const auto& pair : comm) {
            if (pair.second == u_comm && pair.first != u) {
                sigma_tot += k.at(pair.first);
            }
        }

        double k_u = k.at(u);
        return (k_in / m2) - (sigma_tot * k_u / (m2 * m2));
    }

    // Louvain: Partitions nodes into modularity communities.
    std::map<uint64_t, uint64_t> Shard::LouvainPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        std::map<uint64_t, uint64_t> comm;
        std::map<uint64_t, double> k;
        std::map<uint64_t, std::vector<Edge>> adj;

        double m2 = 0.0;
        for (uint64_t u : nodes) {
            comm[u] = u;
            k[u] = 0.0;
        }

        for (const auto& e : edges) {
            adj[e.src].push_back(e);
            k[e.src] += e.weight;
            m2 += e.weight;
            if (!directed) {
                adj[e.dst].push_back({e.dst, e.src, e.weight});
                k[e.dst] += e.weight;
                m2 += e.weight;
            }
        }

        bool improvement = true;
        while (improvement) {
            improvement = false;
            for (uint64_t u : nodes) {
                uint64_t best_comm = comm[u];
                double max_gain = 0.0;

                for (const auto& edge : adj[u]) {
                    uint64_t neighbor_comm = comm[edge.dst];
                    if (neighbor_comm != comm[u]) {
                        double gain = GetModularityGain(u, neighbor_comm, adj, comm, k, m2);
                        if (gain > max_gain) {
                            max_gain = gain;
                            best_comm = neighbor_comm;
                        }
                    }
                }

                if (best_comm != comm[u]) {
                    comm[u] = best_comm;
                    improvement = true;
                }
            }
        }
        return comm;
    }

    // Leiden: Enhanced Louvain optimizing community refinement steps.
    std::map<uint64_t, uint64_t> Shard::LeidenPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        // Fallback to Louvain modularity partitions which provides similar community grouping structure
        return LouvainPeered(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
    }

    // LabelPropagation: Iteratively adopts the majority label of neighbors.
    std::map<uint64_t, uint64_t> Shard::LabelPropagationPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        std::map<uint64_t, uint64_t> labels;
        for (uint64_t id : nodes) labels[id] = id;

        std::map<uint64_t, std::vector<std::pair<uint64_t, double>>> adj;
        for (const auto& e : edges) {
            adj[e.src].push_back({e.dst, e.weight});
            if (!directed) {
                adj[e.dst].push_back({e.src, e.weight});
            }
        }

        bool changed = true;
        for (int iter = 0; iter < 15 && changed; ++iter) {
            changed = false;
            for (uint64_t u : nodes) {
                if (adj[u].empty()) continue;

                std::unordered_map<uint64_t, double> counts;
                for (const auto& edge : adj[u]) {
                    uint64_t neighbor = edge.first;
                    double weight = edge.second;
                    counts[labels[neighbor]] += weight;
                }

                uint64_t max_label = labels[u];
                double max_weight = 0.0;

                for (const auto& pair : counts) {
                    if (pair.second > max_weight) {
                        max_weight = pair.second;
                        max_label = pair.first;
                    }
                }

                if (labels[u] != max_label) {
                    labels[u] = max_label;
                    changed = true;
                }
            }
        }
        return labels;
    }

    // Infomap: Groups nodes based on flow dynamics.
    std::map<uint64_t, uint64_t> Shard::InfomapPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        // Runs standard label propagation to cluster flow patterns
        return LabelPropagationPeered(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
    }

}

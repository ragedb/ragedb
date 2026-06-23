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
#include <unordered_set>
#include <tuple>

namespace ragedb {

    // Internal helper to build neighbor sets
    static std::map<uint64_t, std::unordered_set<uint64_t>> GetNeighborSets(const std::vector<Shard::Edge>& edges) {
        std::map<uint64_t, std::unordered_set<uint64_t>> sets;
        for (const auto& e : edges) {
            sets[e.src].insert(e.dst);
            sets[e.dst].insert(e.src);
        }
        return sets;
    }

    // Triangles: Returns all triples (u, v, w) that form a mutual connection.
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> Shard::TrianglesPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        auto neighbors = GetNeighborSets(edges);

        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> result;

        for (uint64_t u : nodes) {
            const auto& u_set = neighbors[u];
            for (uint64_t v : u_set) {
                const auto& v_set = neighbors[v];
                for (uint64_t w : v_set) {
                    if (u_set.count(w)) {
                        result.push_back({u, v, w});
                    }
                }
            }
        }
        return result;
    }

    // UniqueTriangles: Returns unique triples (u, v, w) sorted such that u < v < w.
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> Shard::UniqueTrianglesPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        auto neighbors = GetNeighborSets(edges);

        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> result;

        for (uint64_t u : nodes) {
            const auto& u_set = neighbors[u];
            for (uint64_t v : u_set) {
                if (u < v) {
                    const auto& v_set = neighbors[v];
                    for (uint64_t w : v_set) {
                        if (v < w && u_set.count(w)) {
                            result.push_back({u, v, w});
                        }
                    }
                }
            }
        }
        return result;
    }

    // LocalClusteringCoefficient: Degree of node neighbor inter-connection compared to maximum possible connection.
    std::map<uint64_t, double> Shard::LocalClusteringCoefficientPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        auto neighbors = GetNeighborSets(edges);

        std::map<uint64_t, double> lcc;

        for (uint64_t u : nodes) {
            const auto& u_set = neighbors[u];
            double k = u_set.size();
            if (k <= 1.0) {
                lcc[u] = 0.0;
                continue;
            }

            double triangles = 0.0;
            for (uint64_t v : u_set) {
                const auto& v_set = neighbors[v];
                for (uint64_t w : v_set) {
                    if (w != v && u_set.count(w)) {
                        triangles += 1.0;
                    }
                }
            }
            double possible_links = k * (k - 1.0);
            lcc[u] = possible_links > 0.0 ? triangles / possible_links : 0.0;
        }
        return lcc;
    }

    // AverageClusteringCoefficient: Mean value of LCC across all nodes in the graph.
    double Shard::AverageClusteringCoefficientPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto lcc = LocalClusteringCoefficientPeered(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        if (lcc.empty()) return 0.0;

        double sum = 0.0;
        for (const auto& pair : lcc) sum += pair.second;
        return sum / lcc.size();
    }

}

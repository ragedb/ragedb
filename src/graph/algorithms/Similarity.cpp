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
#include <cmath>
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

    // Jaccard Similarity: Size of neighbor intersection divided by size of neighbor union.
    std::vector<std::tuple<uint64_t, uint64_t, double>> Shard::JaccardSimilarityPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        auto neighbors = GetNeighborSets(edges);

        std::vector<std::tuple<uint64_t, uint64_t, double>> result;

        for (size_t i = 0; i < nodes.size(); ++i) {
            uint64_t u = nodes[i];
            const auto& u_set = neighbors[u];
            if (u_set.empty()) continue;

            for (size_t j = i + 1; j < nodes.size(); ++j) {
                uint64_t v = nodes[j];
                const auto& v_set = neighbors[v];
                if (v_set.empty()) continue;

                size_t intersect_cnt = 0;
                for (uint64_t n : u_set) {
                    if (v_set.count(n)) intersect_cnt++;
                }

                size_t union_cnt = u_set.size() + v_set.size() - intersect_cnt;
                double score = union_cnt > 0 ? static_cast<double>(intersect_cnt) / union_cnt : 0.0;
                if (score > 0.0001) {
                    result.push_back({u, v, score});
                }
            }
        }
        return result;
    }

    // Cosine Similarity: Intersection divided by geometric mean of neighbor set sizes.
    std::vector<std::tuple<uint64_t, uint64_t, double>> Shard::CosineSimilarityPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        auto neighbors = GetNeighborSets(edges);

        std::vector<std::tuple<uint64_t, uint64_t, double>> result;

        for (size_t i = 0; i < nodes.size(); ++i) {
            uint64_t u = nodes[i];
            const auto& u_set = neighbors[u];
            if (u_set.empty()) continue;

            for (size_t j = i + 1; j < nodes.size(); ++j) {
                uint64_t v = nodes[j];
                const auto& v_set = neighbors[v];
                if (v_set.empty()) continue;

                size_t intersect_cnt = 0;
                for (uint64_t n : u_set) {
                    if (v_set.count(n)) intersect_cnt++;
                }

                double denom = std::sqrt(u_set.size() * v_set.size());
                double score = denom > 0.0 ? static_cast<double>(intersect_cnt) / denom : 0.0;
                if (score > 0.0001) {
                    result.push_back({u, v, score});
                }
            }
        }
        return result;
    }

    // Adamic-Adar Similarity: Sum of inverse logarithms of mutual neighbor degrees.
    std::vector<std::tuple<uint64_t, uint64_t, double>> Shard::AdamicAdarPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        auto neighbors = GetNeighborSets(edges);

        std::vector<std::tuple<uint64_t, uint64_t, double>> result;

        for (size_t i = 0; i < nodes.size(); ++i) {
            uint64_t u = nodes[i];
            const auto& u_set = neighbors[u];
            if (u_set.empty()) continue;

            for (size_t j = i + 1; j < nodes.size(); ++j) {
                uint64_t v = nodes[j];
                const auto& v_set = neighbors[v];
                if (v_set.empty()) continue;

                double score = 0.0;
                for (uint64_t n : u_set) {
                    if (v_set.count(n)) {
                        double deg = neighbors[n].size();
                        if (deg > 1.0) {
                            score += 1.0 / std::log(deg);
                        }
                    }
                }

                if (score > 0.0001) {
                    result.push_back({u, v, score});
                }
            }
        }
        return result;
    }

    // Preferential Attachment: Product of neighbor set sizes.
    std::vector<std::tuple<uint64_t, uint64_t, double>> Shard::PreferentialAttachmentPeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        auto neighbors = GetNeighborSets(edges);

        std::vector<std::tuple<uint64_t, uint64_t, double>> result;

        for (size_t i = 0; i < nodes.size(); ++i) {
            uint64_t u = nodes[i];
            for (size_t j = i + 1; j < nodes.size(); ++j) {
                uint64_t v = nodes[j];
                double score = static_cast<double>(neighbors[u].size() * neighbors[v].size());
                if (score > 0.0001) {
                    result.push_back({u, v, score});
                }
            }
        }
        return result;
    }

}

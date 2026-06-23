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
#include <tuple>

namespace ragedb {

    // Distance: Computes the shortest path length (in hops) between all reachable node pairs.
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> Shard::DistancePeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto max_nodes = AllNodesCountPeered().get0();
        auto nodes = AllNodeIdsPeered(0, max_nodes).get0();
        std::vector<Edge> edges = GetGraphEdges(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);

        std::map<uint64_t, std::vector<uint64_t>> adj;
        for (const auto& e : edges) {
            adj[e.src].push_back(e.dst);
            if (!directed) {
                adj[e.dst].push_back(e.src);
            }
        }

        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> dists;

        // BFS traversal starting at each node
        for (uint64_t s : nodes) {
            std::map<uint64_t, uint64_t> dist;
            std::queue<uint64_t> Q;
            Q.push(s);
            dist[s] = 0;

            while (!Q.empty()) {
                uint64_t u = Q.front();
                Q.pop();

                for (uint64_t v : adj[u]) {
                    if (!dist.count(v)) {
                        dist[v] = dist[u] + 1;
                        Q.push(v);
                        dists.push_back({s, v, dist[v]});
                    }
                }
            }
        }
        return dists;
    }

    // DiameterRange: Returns the minimum and maximum bounds of the graph diameter.
    std::pair<uint64_t, uint64_t> Shard::DiameterRangePeered(const std::string& rel_type, const std::string& edge_concept, const std::string& src_rel, const std::string& dst_rel, const std::string& weight_prop, bool directed, bool weighted) {
        auto dists = DistancePeered(rel_type, edge_concept, src_rel, dst_rel, weight_prop, directed, weighted);
        uint64_t max_d = 0;
        for (const auto& t : dists) {
            uint64_t d = std::get<2>(t);
            if (d > max_d) max_d = d;
        }
        return {max_d, max_d};
    }

}

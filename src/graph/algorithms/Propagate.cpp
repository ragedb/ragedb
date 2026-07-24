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
#include <algorithm>
#include <deque>
#include <map>
#include <unordered_set>
#include <vector>

namespace ragedb {

    namespace {
        // Coerce a numeric relationship property to double (int64 or double variants).
        double PropertyToDouble(const property_type_t& p, bool& ok) {
            ok = true;
            if (std::holds_alternative<double>(p)) return std::get<double>(p);
            if (std::holds_alternative<int64_t>(p)) return static_cast<double>(std::get<int64_t>(p));
            if (std::holds_alternative<bool>(p)) return std::get<bool>(p) ? 1.0 : 0.0;
            ok = false;
            return 0.0;
        }

        // Coerce a numeric relationship property to int64 (for the optional range-filter window).
        int64_t PropertyToInt(const property_type_t& p, bool& ok) {
            ok = true;
            if (std::holds_alternative<int64_t>(p)) return std::get<int64_t>(p);
            if (std::holds_alternative<double>(p)) return static_cast<int64_t>(std::get<double>(p));
            ok = false;
            return 0;
        }

        // One eligible fan-out edge from a node: the neighbor it reaches and the value it carries.
        struct FanEdge {
            uint64_t dst;
            double value;
        };
    }

    std::vector<Shard::PropagateResult> Shard::PropagateBFSPeered(
        const std::vector<uint64_t>& seeds, const std::vector<double>& seed_values,
        const std::vector<std::string>& rel_types, Direction direction, uint32_t max_depth,
        const std::string& value_prop, bool order_desc, uint32_t trunc_limit, double min_value,
        const std::string& filter_prop, int64_t filter_min, int64_t filter_max) {

        // Cross-seed accumulation: node -> (summed inflow, minimum depth).
        std::map<uint64_t, std::pair<double, uint32_t>> merged;
        const bool has_filter = !filter_prop.empty();

        auto claim = [&](uint64_t node, double value, uint32_t depth) {
            auto it = merged.find(node);
            if (it == merged.end()) {
                merged.emplace(node, std::make_pair(value, depth));
            } else {
                it->second.first += value;
                if (depth < it->second.second) it->second.second = depth;
            }
        };

        // Gather a node's eligible out-edges, ordered by carried value then truncated, then value-gated.
        // Truncation deliberately precedes the min_value gate (the FinBench truncation-strategy order):
        // which edges survive the cap changes which node claims which neighbor first.
        auto fan_out = [&](uint64_t node) {
            std::vector<Relationship> rels = rel_types.empty()
                ? NodeGetRelationshipsPeered(node, direction).get0()
                : NodeGetRelationshipsPeered(node, direction, rel_types).get0();

            std::vector<FanEdge> edges;
            edges.reserve(rels.size());
            for (const auto& rel : rels) {
                if (has_filter) {
                    bool fok = false;
                    int64_t fv = PropertyToInt(rel.getProperty(filter_prop), fok);
                    if (!fok || fv < filter_min || fv > filter_max) continue;
                }
                bool vok = false;
                double value = PropertyToDouble(rel.getProperty(value_prop), vok);
                if (!vok) continue;
                uint64_t dst = (rel.getStartingNodeId() == node) ? rel.getEndingNodeId()
                                                                 : rel.getStartingNodeId();
                edges.push_back({dst, value});
            }

            // Stable sort by carried value so ties keep the relationship-store order (determinism).
            if (order_desc) {
                std::stable_sort(edges.begin(), edges.end(),
                                 [](const FanEdge& a, const FanEdge& b) { return a.value > b.value; });
            } else {
                std::stable_sort(edges.begin(), edges.end(),
                                 [](const FanEdge& a, const FanEdge& b) { return a.value < b.value; });
            }
            if (trunc_limit > 0 && edges.size() > trunc_limit) {
                edges.resize(trunc_limit);
            }
            return edges;
        };

        // One independent BFS per seed, each with its own visited set (first-claim within the seed).
        for (size_t i = 0; i < seeds.size(); ++i) {
            uint64_t seed = seeds[i];
            double seed_value = (i < seed_values.size()) ? seed_values[i] : 0.0;

            std::unordered_set<uint64_t> visited;
            visited.insert(seed);
            // Queue entries carry (node, depth); the seed sits at depth 1.
            std::deque<std::pair<uint64_t, uint32_t>> queue;
            queue.emplace_back(seed, 1u);
            claim(seed, seed_value, 1u);

            while (!queue.empty()) {
                auto [node, depth] = queue.front();
                queue.pop_front();
                if (depth >= max_depth) continue;

                for (const auto& edge : fan_out(node)) {
                    if (edge.value <= min_value) continue;      // exclusive gate, applied after truncation
                    if (visited.count(edge.dst)) continue;      // first edge in BFS order claims the node
                    visited.insert(edge.dst);
                    claim(edge.dst, edge.value, depth + 1);
                    queue.emplace_back(edge.dst, depth + 1);
                }
            }
        }

        std::vector<PropagateResult> results;
        results.reserve(merged.size());
        for (const auto& [node, agg] : merged) {
            results.push_back({node, agg.first, agg.second});
        }
        // merged is a std::map, so results already emerge in node-id-ascending order.
        return results;
    }

}

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

namespace ragedb {

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::InvertViaLua(const std::map<uint64_t, std::vector<uint64_t>> &map) {
        std::map<uint64_t, std::vector<uint64_t>> inverted;
        for (const auto& [node_id, neighbor_ids] : map) {
            for (const auto& neighbor_id : neighbor_ids) {
                inverted[neighbor_id].emplace_back(node_id);
            }
        }
        return inverted;
    }


//    uint64_t Shard::mmtest2() { // 2.5 seconds, 41421291
//        uint64_t count = 0;
//        auto a = AllNodeIdsPeered(uint64_t(0), 1000000).get0();
//        auto outs = NodeIdsGetNeighborIdsPeered(a, Direction::OUT, "Links").get0(); // ab and bc
//        auto ins = NodeIdsGetDistinctNeighborIdsPeered(a, Direction::IN, "Links").get0(); //ac
//
//        for (auto node_a : a) {
//            auto bs = outs[node_a];
//            auto set = ins[node_a];
//            for (auto node_b : bs) {
//                for (auto node_c : outs[node_b]) {
//                    if (set.contains(node_c)) {
//                        count++;
//                    }
//                }
//            }
//        }
//        return count;
//    }

//    uint64_t Shard::mmtest2() { // 13.5 seconds, 41445060
//        uint64_t count = 0;
//        auto a = AllNodeIdsPeered(uint64_t(0), 1000000).get0();
//        auto outs = NodeIdsGetDistinctNeighborIdsPeered(a, Direction::OUT, "Links").get0(); // ab and bc and ca
//
//        for (auto node_a : a) {
//            auto bs = outs[node_a];
//            for (auto node_b : bs) {
//                for (auto node_c : outs[node_b.first]) {
//                    if (outs[node_c.first].contains(node_a)) {
//                        count++;
//                    }
//                }
//            }
//        }
//        return count;
//    }

//        uint64_t Shard::mmtest2() {
//            uint64_t count = 0;
//            auto a = AllNodeIdsPeered(uint64_t(0), 1000000).get0(); // takes 7-10 ms
//            //auto outs = NodeIdsGetNeighborIdsPeered(a, Direction::OUT, "Links").get0(); // takes 200 ms  - ab and bc and ca
//            auto outs = NodeIdsGetDistinctNeighborIdsPeered(a, Direction::OUT, "Links").get0(); // takes 730 ms   -
//            //auto ins = NodeIdsGetDistinctNeighborIdsPeered(a, Direction::IN, "Links").get0(); // takes 800 ms   - ac
//
////            for (auto node_a : a) {
////                auto abs = outs[node_a];
////                auto acs = ins[node_a];
////                for (auto node_b : abs) {
////                    auto bcs = outs[node_b];
////                    for (auto node_c : bcs) {
////                        if (acs.contains(node_c)) {
////                            count++;
////                        }
////                    }
////                }
////            }
//            return a.size();
//            //return count;
//        }

//        uint64_t Shard::mmtest2() { // 2.6 seconds 41421291
//            uint64_t count = 0;
//            auto a = AllNodeIdsPeered(uint64_t(0), 1000000).get0(); // takes 7-10 ms
//            auto outs = NodeIdsGetNeighborIdsPeered(a, Direction::OUT, "Links").get0(); // takes 200 ms  - ab and bc and ca
//            for (auto node_a : a) {
//                auto bs = outs[node_a];
//                auto set = NodeGetDistinctNeighborIdsPeered(node_a, Direction::IN, "Links").get0();
//                for (auto node_b : bs) {
//                    for (auto node_c : outs[node_b]) {
//                        if (set.contains(node_c)) {
//                            count++;
//                        }
//                    }
//                }
//            }
//
//            return count;
//        }

    std::vector<uint64_t> IntersectUnsortedIds(const std::vector<uint64_t>& ids1, const std::vector<uint64_t>& ids2) {
        std::vector<uint64_t> first(ids1);
        std::vector<uint64_t> second(ids2);
        std::vector<uint64_t> intersection;
        std::sort(first.begin(), first.end());
        std::sort(second.begin(), second.end());

        std::set_intersection(first.begin(), first.end(), second.begin(), second.end(), std::inserter(intersection, intersection.begin()));
        return intersection;
    }

        uint64_t Shard::mmtest2() { // 1.8 seconds, 41421291
            uint64_t count = 0;
            auto a = AllNodeIdsPeered(uint64_t(0), 1000000).get0(); // takes 7-10 ms
            auto outs = NodeIdsGetNeighborIdsPeered(a, Direction::OUT, "Links").get0(); // takes 200 ms  - ab and bc and ca
            auto ins = NodeIdsGetNeighborIdsPeered(a, Direction::IN, "Links").get0(); // takes 200 ms   - ac
            /*
             * Step 4 - Probe: We re-scan the accumulated ab tuples from the factorized table.
             * For each tuple, we first probe “Hash Table (a)<-(c)” and then “Hash Table (b)->(c)” to fetch two lists,
             * intersect them, and produce outputs. In this case there is only one tuple (a=1, b=0),
             * so we will fetch a=1’s backward list and b=0’s forwrad list, intersect these lists,
             * and produce the triangle (a=1, b=0, c=1001).
             */
//            for (auto node_a : a) {
//                std::sort(outs[node_a].begin(), outs[node_a].end());
//                std::sort(ins[node_a].begin(), ins[node_a].end());
//            }
            for (auto& node_a : a) {
                auto bs = outs[node_a];
                auto cs = ins[node_a];
                for (auto& node_b : bs) {
                    auto cs2 = outs[node_b];
                     count += Shard::IntersectIds(cs, cs2).size();
                }
            }

            return count;
        }
}
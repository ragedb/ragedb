/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    seastar::future<uint64_t> Shard::FindNodeCountPeered(const std::string& type, const std::string& property, const Operation& operation, const std::any& value) {
        uint16_t type_id = node_types.getTypeId(type);
        return FindNodeCountPeered(type_id, property, operation, value);
    }

    seastar::future<uint64_t> Shard::FindNodeCountPeered(uint16_t type_id, const std::string& property, const Operation& operation, const std::any& value) {
        seastar::future<std::vector<uint64_t>> v = container().map([type_id, property, operation, value] (Shard &local) {
            return local.FindNodeCount(type_id, property, operation, value);
        });

        return v.then([] (std::vector<uint64_t> counts) {
            return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::FindNodeIdsPeered(const std::string& type, const std::string& property, const Operation& operation, const std::any& value, uint64_t skip, uint64_t limit) {
        uint16_t type_id = node_types.getTypeId(type);
        return FindNodeIdsPeered(type_id, property, operation, value, skip, limit);
    }

    seastar::future<std::vector<uint64_t>> Shard::FindNodeIdsPeered(uint16_t node_type_id, const std::string& property, const Operation& operation, const std::any& value, uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [node_type_id, property, operation, value] (Shard &local_shard) mutable {
                return local_shard.FindNodeCount(node_type_id, property, operation, value);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, node_type_id, property, operation, value, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            uint64_t next = 0;
            int current_shard_id = 0;

            std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
            for (const auto& count : results) {
                next = current + count;
                if (next > skip) {
                    std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit);
                    requests.insert({ current_shard_id, pair });
                    if (next <= max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& request : requests) {
                auto future = container().invoke_on(request.first, [node_type_id, property, operation, value, request] (Shard &local_shard) mutable {
                    return local_shard.FindNodeIds(node_type_id, property, operation, value, request.second.first, request.second.second);
                });
                futures.push_back(std::move(future));
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<uint64_t>>& results) {
                std::vector<uint64_t> ids;
                ids.reserve(limit);
                for (auto result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                }
                return ids;
            });
        });
    }

    seastar::future<std::vector<Node>> Shard::FindNodesPeered(const std::string& type, const std::string& property, const Operation& operation, const std::any& value, uint64_t skip, uint64_t limit) {
        uint16_t type_id = node_types.getTypeId(type);
        return FindNodesPeered(type_id, property, operation, value, skip, limit);
    }

    seastar::future<std::vector<Node>> Shard::FindNodesPeered(uint16_t node_type_id, const std::string& property, const Operation& operation, const std::any& value, uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // TODO: Find out why I only get 1 back...
        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i < cpus; i++) {
            auto future = container().invoke_on(i, [node_type_id, property, operation, value] (Shard &local_shard) mutable {
                return local_shard.FindNodeCount(node_type_id, property, operation, value);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, node_type_id, property, operation, value, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            uint64_t next = 0;
            int current_shard_id = 0;

            std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
            for (const auto& count : results) {
                // If we have no results on this shard, skip it
                if (count == 0) {
                    current_shard_id++;
                    continue;
                }
                next = current + count;
                if (next > skip) {
                    std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                    requests.insert({ current_shard_id, pair });
                    if (next > max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<Node>>> futures;

            for (const auto& request : requests) {
                auto future = container().invoke_on(request.first, [node_type_id, property, operation, value, request] (Shard &local_shard) mutable {
                    return local_shard.FindNodes(node_type_id, property, operation, value, request.second.first, request.second.second);
                });
                futures.push_back(std::move(future));
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Node>>& results) {
                std::vector<Node> nodes;
                nodes.reserve(limit);
                for (auto result : results) {
                    nodes.insert(std::end(nodes), std::begin(result), std::end(result));
                }
                return nodes;
            });
        });
    }
}
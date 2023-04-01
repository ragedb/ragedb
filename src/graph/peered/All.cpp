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

    seastar::future<uint64_t> Shard::AllNodesCountPeered() {
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.AllNodesCount();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
                return std::accumulate(results.begin(), results.end(), uint64_t(0));
        });
    }

    seastar::future<uint64_t> Shard::AllNodesCountPeered(const std::string& type) {
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [type] (Shard &local_shard) mutable {
                return local_shard.NodeCount(type);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
            return std::accumulate(results.begin(), results.end(), uint64_t(0));
        });
    }

    seastar::future<std::map<std::string, uint64_t>> Shard::AllNodesCountsPeered() {
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.NodeCounts();
            });
            futures.push_back(std::move(future));
        }
        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            std::map<std::string, uint64_t> counts;

            for (const auto& type : NodeTypesGet()) {
                counts.emplace(type, uint64_t(0));
            }

            for (const auto& result : results) {
                for (const auto& [type_id, count] : result) {
                    auto current = counts[NodeTypeGetType(type_id)];
                    counts[NodeTypeGetType(type_id)] = current + count;
                }
            }

            return counts;
        });
    }

    seastar::future<uint64_t> Shard::AllRelationshipsCountPeered() {
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.AllRelationshipsCount();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
            return std::accumulate(results.begin(), results.end(), uint64_t(0));
        });
    }

    seastar::future<uint64_t> Shard::AllRelationshipsCountPeered(const std::string& type) {
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [type] (Shard &local_shard) mutable {
                return local_shard.RelationshipCount(type);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<uint64_t>& results) {
            return std::accumulate(results.begin(), results.end(), uint64_t(0));
        });
    }

    seastar::future<std::map<std::string, uint64_t>> Shard::AllRelationshipsCountsPeered() {
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.RelationshipCounts();
            });
            futures.push_back(std::move(future));
        }
        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            std::map<std::string, uint64_t> counts;

            for(const auto& type : RelationshipTypesGet()) {
                counts.emplace(type, uint64_t(0));
            }

            for (const auto& result : results) {
                for (const auto& [type_id, count] : result) {
                    auto current = counts[RelationshipTypeGetType(type_id)];
                    counts[RelationshipTypeGetType(type_id)] = current + count;
                }
            }

            return counts;
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::AllNodeIdsPeered(uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.NodeCounts();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            uint64_t current = 0;
            uint64_t next;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
            for (const auto& map : results) {
                std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
                for (const auto& [core_id, request_count] : map) {
                    // If we have no results of this type, skip it
                    if (request_count == 0) {
                        continue;
                    }
                    next = current + request_count;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                        threaded_requests.try_emplace(core_id, pair);
                        if (next > max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                if(!threaded_requests.empty()) {
                    requests.try_emplace(current_shard_id, threaded_requests);
                }
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& [request_shard_id, request] : requests) {
                for (const auto& [type_id, skip_and_limit] : request) {
                    auto future = container().invoke_on(request_shard_id, [type_id=type_id, skip = skip_and_limit.first, limit = skip_and_limit.second] (Shard &local_shard) mutable {
                        return local_shard.AllNodeIds(type_id, skip, limit);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2] (const std::vector<std::vector<uint64_t>>& results) {
                std::vector<uint64_t> ids;
                auto count = 0;
                for (const auto& result : results) {
                    count += result.size();
                }
                ids.reserve(count);
                for (const auto& result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                }
                return ids;
            });
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::AllNodeIdsPeered(const std::string& type, uint64_t skip, uint64_t limit) {
        uint16_t node_type_id = node_types.getTypeId(type);
        uint64_t max = skip + limit;

        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (uint16_t i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [node_type_id] (Shard &local_shard) mutable {
                return local_shard.NodeCount(node_type_id);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, node_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            uint64_t next;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
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
                    requests.try_emplace(current_shard_id, pair);
                    if (next > max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& [request_shard_id, skip_and_limit] : requests) {
                auto future = container().invoke_on(request_shard_id, [node_type_id, skip = skip_and_limit.first, limit = skip_and_limit.second] (Shard &local_shard) mutable {
                    return local_shard.AllNodeIds(node_type_id, skip, limit);
                });
                futures.push_back(std::move(future));
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<uint64_t>>& results) {
                std::vector<uint64_t> ids;
                ids.reserve(limit);
                for (const auto& result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                }
                return ids;
            });
        });
    }

    seastar::future<std::vector<Node>> Shard::AllNodesPeered(uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (uint16_t i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.NodeCounts();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            uint64_t current = 0;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
            for (const auto& map : results) {
                std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
                for (const auto& [core_id, request_count] : map) {
                    // If we have no results of this type, skip it
                    if (request_count == 0) {
                        continue;
                    }

                    uint64_t next = current + request_count;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                        threaded_requests.try_emplace(core_id, pair);
                        if (next > max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                if(!threaded_requests.empty()) {
                    requests.try_emplace(current_shard_id, threaded_requests);
                }
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<Node>>> futures;

            for (const auto& [request_shard_id, request] : requests) {
                for (const auto& [type_id, skip_and_limit] : request) {
                    auto future = container().invoke_on(request_shard_id, [type_id=type_id, skip=skip_and_limit.first, limit=skip_and_limit.second] (Shard &local_shard) mutable {
                        return local_shard.AllNodes(type_id, skip, limit);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Node>>& results) {
                std::vector<Node> requested_nodes;
                requested_nodes.reserve(limit);
                for (const auto& result : results) {
                    requested_nodes.insert(std::end(requested_nodes), std::begin(result), std::end(result));
                }
                return requested_nodes;
            });
        });
    }

    seastar::future<std::vector<Node>> Shard::AllNodesPeered(const std::string &type, uint64_t skip, uint64_t limit) {
        uint16_t node_type_id = node_types.getTypeId(type);
        uint64_t max = skip + limit;

        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (uint16_t i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [node_type_id] (Shard &local_shard) mutable {
                return local_shard.NodeCount(node_type_id);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, node_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
            for (const auto& count : results) {
                // If we have no results on this shard, skip it
                if (count == 0) {
                    current_shard_id++;
                    continue;
                }
                uint64_t next = current + count;
                if (next > skip) {
                    std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                    requests.try_emplace(current_shard_id, pair);
                    if (next > max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<Node>>> futures;

            for (const auto& [request_shard_id, skip_and_limit] : requests) {
                auto future = container().invoke_on(request_shard_id, [node_type_id, skip = skip_and_limit.first, limit = skip_and_limit.second] (Shard &local_shard) mutable {
                    return local_shard.AllNodes(node_type_id, skip, limit);
                });
                futures.push_back(std::move(future));
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Node>>& results) {
                std::vector<Node> requested_nodes;
                requested_nodes.reserve(limit);
                for (const auto& result : results) {
                    requested_nodes.insert(std::end(requested_nodes), std::begin(result), std::end(result));
                }
                return requested_nodes;
            });
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::AllRelationshipIdsPeered(uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.RelationshipCounts();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            uint64_t current = 0;
            uint64_t next;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
            for (const auto& map : results) {
                std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
                for (const auto& [core_id, request_count] : map) {
                    // If we have no results of this type, skip it
                    if (request_count == 0) {
                        continue;
                    }
                    next = current + request_count;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                        threaded_requests.try_emplace(core_id, pair);
                        if (next > max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                if(!threaded_requests.empty()) {
                    requests.try_emplace(current_shard_id, threaded_requests);
                }
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& [request_shard_id, request] : requests) {
                for (const auto& [type_id, skip_and_limit] : request) {
                    auto future = container().invoke_on(request_shard_id, [type_id=type_id, skip = skip_and_limit.first, limit = skip_and_limit.second] (Shard &local_shard) mutable {
                        return local_shard.AllRelationshipIds(type_id, skip, limit);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<uint64_t>>& results) {
                std::vector<uint64_t> ids;
                ids.reserve(limit);
                for (const auto& result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                }
                return ids;
            });
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::AllRelationshipIdsPeered(const std::string &rel_type, uint64_t skip, uint64_t limit) {
        uint16_t relationship_type_id = relationship_types.getTypeId(rel_type);
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (uint16_t i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [relationship_type_id] (Shard &local_shard) mutable {
                return local_shard.RelationshipCount(relationship_type_id);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, relationship_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            uint64_t next;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
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
                    requests.try_emplace(current_shard_id, pair);
                    if (next > max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& [request_shard_id, skip_and_limit] : requests) {
                auto future = container().invoke_on(request_shard_id, [relationship_type_id, skip = skip_and_limit.first, limit = skip_and_limit.second] (Shard &local_shard) mutable {
                    return local_shard.AllRelationshipIds(relationship_type_id, skip, limit);
                });
                futures.push_back(std::move(future));

            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<uint64_t>>& results) {
                std::vector<uint64_t> ids;
                ids.reserve(limit);
                for (const auto& result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                }
                return ids;
            });
        });
    }

    seastar::future<std::vector<Relationship>> Shard::AllRelationshipsPeered(uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (uint16_t i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.RelationshipCounts();
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
            uint64_t current = 0;
            uint64_t next;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
            for (const auto& map : results) {
                std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
                for (const auto& [core_id, request_count] : map) {
                    // If we have no results of this type, skip it
                    if (request_count == 0) {
                        continue;
                    }
                    next = current + request_count;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                        threaded_requests.try_emplace(core_id, pair);
                        if (next > max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                if(!threaded_requests.empty()) {
                    requests.try_emplace(current_shard_id, threaded_requests);
                }
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<Relationship>>> futures;

            for (const auto& [request_shard_id, request] : requests) {
                for (const auto& [type_id, skip_and_limit] : request) {
                    auto future = container().invoke_on(request_shard_id, [type_id=type_id, skip=skip_and_limit.first, limit=skip_and_limit.second] (Shard &local_shard) mutable {
                        return local_shard.AllRelationships(type_id, skip, limit);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Relationship>>& results) {
                std::vector<Relationship> requested_relationships;
                requested_relationships.reserve(limit);
                for (const auto& result : results) {
                    requested_relationships.insert(std::end(requested_relationships), std::begin(result), std::end(result));
                }
                return requested_relationships;
            });
        });
    }

    seastar::future<std::vector<Relationship>> Shard::AllRelationshipsPeered(const std::string &rel_type, uint64_t skip, uint64_t limit) {
        uint16_t relationship_type_id = relationship_types.getTypeId(rel_type);
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (uint16_t i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [relationship_type_id] (Shard &local_shard) mutable {
                return local_shard.RelationshipCount(relationship_type_id);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, relationship_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
            uint64_t current = 0;
            uint64_t next;
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
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
                    requests.try_emplace(current_shard_id, pair);
                    if (next > max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<Relationship>>> futures;

            for (const auto& [request_shard_id, skip_and_limit] : requests) {
                auto future = container().invoke_on(request_shard_id, [relationship_type_id, skip = skip_and_limit.first, limit = skip_and_limit.second] (Shard &local_shard) mutable {
                    return local_shard.AllRelationships(relationship_type_id, skip, limit);
                });
                futures.push_back(std::move(future));

            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Relationship>>& results) {
                std::vector<Relationship> requested_relationships;
                requested_relationships.reserve(limit);
                for (const auto& result : results) {
                    requested_relationships.insert(std::end(requested_relationships), std::begin(result), std::end(result));
                }
                return requested_relationships;
            });
        });
    }

}

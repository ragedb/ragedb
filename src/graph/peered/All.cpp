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
                for (auto entry : map) {
                    // If we have no results of this type, skip it
                    if (entry.second == 0) {
                        continue;
                    }
                    next = current + entry.second;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                        threaded_requests.insert({ entry.first, pair });
                        if (next > max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                if(!threaded_requests.empty()) {
                    requests.insert({current_shard_id, threaded_requests});
                }
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& request : requests) {
                for (auto entry : request.second) {
                    auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                        return local_shard.AllNodeIds(entry.first, entry.second.first, entry.second.second);
                    });
                    futures.push_back(std::move(future));
                }
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

    seastar::future<std::vector<uint64_t>> Shard::AllNodeIdsPeered(const std::string& type, uint64_t skip, uint64_t limit) {
        uint16_t node_type_id = relationship_types.getTypeId(type);
        uint64_t max = skip + limit;

        // Get the {Node Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
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
                    requests.insert({ current_shard_id, pair });
                    if (next > max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& request : requests) {
                auto future = container().invoke_on(request.first, [node_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllNodeIds(node_type_id, request.second.first, request.second.second);
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

    seastar::future<std::vector<Node>> Shard::AllNodesPeered(uint64_t skip, uint64_t limit) {
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
            int current_shard_id = 0;
            std::vector<uint64_t> ids;
            std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
            for (const auto& map : results) {
                std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
                for (auto entry : map) {
                    // If we have no results of this type, skip it
                    if (entry.second == 0) {
                        continue;
                    }
                    // current is 2, limit is 10, skip 3, entry.second is 15
                    // skip = skip - current

                    uint64_t next = current + entry.second;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                        threaded_requests.insert({ entry.first, pair });
                        if (next > max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                if(!threaded_requests.empty()) {
                    requests.insert({current_shard_id, threaded_requests});
                }
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<Node>>> futures;

            for (const auto& request : requests) {
                for (auto entry : request.second) {
                    auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                        return local_shard.AllNodes(entry.first, entry.second.first, entry.second.second);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Node>>& results) {
                std::vector<Node> requested_nodes;
                requested_nodes.reserve(limit);
                for (auto result : results) {
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
        for (int i=0; i<cpus; i++) {
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
                auto future = container().invoke_on(request.first, [node_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllNodes(node_type_id, request.second.first, request.second.second);
                });
                futures.push_back(std::move(future));

            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Node>>& results) {
                std::vector<Node> requested_nodes;
                requested_nodes.reserve(limit);
                for (auto result : results) {
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
                return local_shard.AllRelationshipIdCounts();
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
                for (auto entry : map) {
                    // If we have no results of this type, skip it
                    if (entry.second == 0) {
                        continue;
                    }
                    next = current + entry.second;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                        threaded_requests.insert({ entry.first, pair });
                        if (next > max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                if(!threaded_requests.empty()) {
                    requests.insert({current_shard_id, threaded_requests});
                }
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& request : requests) {
                for (auto entry : request.second) {
                    auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                        return local_shard.AllRelationshipIds(entry.first, entry.second.first, entry.second.second);
                    });
                    futures.push_back(std::move(future));
                }
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

    seastar::future<std::vector<uint64_t>> Shard::AllRelationshipIdsPeered(const std::string &rel_type, uint64_t skip, uint64_t limit) {
        uint16_t relationship_type_id = relationship_types.getTypeId(rel_type);
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<uint64_t>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [relationship_type_id] (Shard &local_shard) mutable {
                return local_shard.AllRelationshipIdCounts(relationship_type_id);
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
                    requests.insert({ current_shard_id, pair });
                    if (next > max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<uint64_t>>> futures;

            for (const auto& request : requests) {
                auto future = container().invoke_on(request.first, [relationship_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllRelationshipIds(relationship_type_id, request.second.first, request.second.second);
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

    seastar::future<std::vector<Relationship>> Shard::AllRelationshipsPeered(uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        // Get the {Relationship Type Id, Count} map for each core
        std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
                return local_shard.AllRelationshipIdCounts();
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
                for (auto entry : map) {
                    // If we have no results of this type, skip it
                    if (entry.second == 0) {
                        continue;
                    }
                    next = current + entry.second;
                    if (next > skip) {
                        std::pair<uint64_t, uint64_t> pair = std::make_pair((skip > current) ? skip - current : 0, limit - current);
                        threaded_requests.insert({ entry.first, pair });
                        if (next > max) {
                            break; // We have everything we need
                        }
                    }
                    current = next;
                }
                if(!threaded_requests.empty()) {
                    requests.insert({current_shard_id, threaded_requests});
                }
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<Relationship>>> futures;

            for (const auto& request : requests) {
                for (auto entry : request.second) {
                    auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                        return local_shard.AllRelationships(entry.first, entry.second.first, entry.second.second);
                    });
                    futures.push_back(std::move(future));
                }
            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Relationship>>& results) {
                std::vector<Relationship> requested_relationships;
                requested_relationships.reserve(limit);
                for (auto result : results) {
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
        for (int i=0; i<cpus; i++) {
            auto future = container().invoke_on(i, [relationship_type_id] (Shard &local_shard) mutable {
                return local_shard.AllRelationshipIdCounts(relationship_type_id);
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
                    requests.insert({ current_shard_id, pair });
                    if (next > max) {
                        break; // We have everything we need
                    }
                }
                current = next;
                current_shard_id++;
            }

            std::vector<seastar::future<std::vector<Relationship>>> futures;

            for (const auto& request : requests) {
                auto future = container().invoke_on(request.first, [relationship_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllRelationships(relationship_type_id, request.second.first, request.second.second);
                });
                futures.push_back(std::move(future));

            }

            auto p2 = make_shared(std::move(futures));
            return seastar::when_all_succeed(p2->begin(), p2->end()).then([p2, limit] (const std::vector<std::vector<Relationship>>& results) {
                std::vector<Relationship> requested_relationships;
                requested_relationships.reserve(limit);
                for (auto result : results) {
                    requested_relationships.insert(std::end(requested_relationships), std::begin(result), std::end(result));
                }
                return requested_relationships;
            });
        });
    }

}

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
    seastar::future<uint64_t> Shard::FindNodeCountPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
        seastar::future<std::vector<uint64_t>> v = container().map([type, property, operation, value] (Shard &local) {
            return local.FindNodeCount(type, property, operation, value);
        });

        return v.then([] (std::vector<uint64_t> counts) {
            return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
        });
    }

    seastar::future<uint64_t> Shard::FindRelationshipCountPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
        seastar::future<std::vector<uint64_t>> v = container().map([type, property, operation, value] (Shard &local) {
            return local.FindRelationshipCount(type, property, operation, value);
        });

        return v.then([] (std::vector<uint64_t> counts) {
            return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::FindNodeIdsPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        std::vector<seastar::future<std::vector<uint64_t>>> futures;

        for (int i=0; i < cpus; i++) {
            auto future = container().invoke_on(i, [type, property, operation, value, max] (Shard &local_shard) mutable {
                return local_shard.FindNodeIds(type, property, operation, value, 0, max);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<uint64_t>>& results) {
            uint64_t current = 0;

            std::vector<uint64_t> ids;

            for (const auto& result : results) {
                for( auto id : result) {
                    if (++current > skip) {
                        ids.push_back(id);
                    }
                    if (current >= max) {
                        return ids;
                    }
                }
            }
            return ids;
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::FindRelationshipIdsPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        std::vector<seastar::future<std::vector<uint64_t>>> futures;

        for (int i=0; i < cpus; i++) {
            auto future = container().invoke_on(i, [type, property, operation, value, max] (Shard &local_shard) mutable {
                return local_shard.FindRelationshipIds(type, property, operation, value, 0, max);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<uint64_t>>& results) {
            uint64_t current = 0;

            std::vector<uint64_t> ids;

            for (const auto& result : results) {
                for( auto id : result) {
                    if (++current > skip) {
                        ids.push_back(id);
                    }
                    if (current >= max) {
                        return ids;
                    }
                }
            }
            return ids;
        });
    }

    seastar::future<std::vector<Node>> Shard::FindNodesPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        std::vector<seastar::future<std::vector<Node>>> futures;
        for (int i=0; i < cpus; i++) {
            auto future = container().invoke_on(i, [type, property, operation, value, max] (Shard &local_shard) mutable {
                return local_shard.FindNodes(type, property, operation, value, 0, max);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<Node>>& results) {
            uint64_t current = 0;

            std::vector<Node> nodes;

            for (const auto& result : results) {
                for(const auto& node : result) {
                    if (++current > skip) {
                        nodes.push_back(node);
                    }
                    if (current >= max) {
                        return nodes;
                    }
                }
            }
            return nodes;
        });
    }

    seastar::future<std::vector<Relationship>> Shard::FindRelationshipsPeered(const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
        uint64_t max = skip + limit;

        std::vector<seastar::future<std::vector<Relationship>>> futures;
        for (int i=0; i < cpus; i++) {
            auto future = container().invoke_on(i, [type, property, operation, value, max] (Shard &local_shard) mutable {
                return local_shard.FindRelationships(type, property, operation, value, 0, max);
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p, skip, max] (const std::vector<std::vector<Relationship>>& results) {
            uint64_t current = 0;

            std::vector<Relationship> relationships;

            for (const auto& result : results) {
                for(const auto& relationship : result) {
                    if (++current > skip) {
                        relationships.push_back(relationship);
                    }
                    if (current >= max) {
                        return relationships;
                    }
                }
            }
            return relationships;
        });
    }
}
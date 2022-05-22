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

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(type, key);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(const std::string& type, const std::string& key, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(type, key, direction);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, rel_type](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(type, key, direction, rel_type);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t type_id) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, type_id](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(type, key, direction, type_id);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, rel_types](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(type, key, direction, rel_types);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(id);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(uint64_t id, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, direction](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(id, direction);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(uint64_t id, Direction direction, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, direction, rel_type](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(id, direction, rel_type);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(uint64_t id, Direction direction, uint16_t type_id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, direction, type_id](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(id, direction, type_id);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeGetNeighborIdsPeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, direction, rel_types](Shard &local_shard) {
            return local_shard.NodeGetNeighborIds(id, direction, rel_types);
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeIdsGetNeighborIdsPeered(const std::vector<uint64_t>& ids) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::vector<uint64_t>>> futures;
        for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                std::vector<uint64_t> ids;
                for (auto node_id : grouped_node_ids) {
                    auto neighbor_ids = local_shard.NodeGetNeighborIds(node_id);
                    ids.insert(std::end(ids), std::begin(neighbor_ids), std::end(neighbor_ids));
                }
                return ids;
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint64_t>>& results) {
            std::vector<uint64_t> combined;

            for(const std::vector<uint64_t>& sharded : results) {
                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
            }
            // sort followed by unique, to remove all duplicates
            std::ranges::sort(combined);
            auto ret = std::ranges::unique(combined);
            combined.erase(ret.begin(), ret.end());

            return combined;
        });
    }
    seastar::future<std::vector<uint64_t>> Shard::NodeIdsGetNeighborIdsPeered(const std::vector<uint64_t>& ids, Direction direction) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::vector<uint64_t>>> futures;
        for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, direction] (Shard &local_shard) {
                std::vector<uint64_t> ids;
                for (auto node_id : grouped_node_ids) {
                    auto neighbor_ids = local_shard.NodeGetNeighborIds(node_id, direction);
                    ids.insert(std::end(ids), std::begin(neighbor_ids), std::end(neighbor_ids));
                }
                return ids;
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint64_t>>& results) {
            std::vector<uint64_t> combined;

            for(const std::vector<uint64_t>& sharded : results) {
                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
            }
            // sort followed by unique, to remove all duplicates
            std::ranges::sort(combined);
            auto ret = std::ranges::unique(combined);
            combined.erase(ret.begin(), ret.end());

            return combined;
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeIdsGetNeighborIdsPeered(const std::vector<uint64_t>& ids, Direction direction, const std::string& rel_type) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::vector<uint64_t>>> futures;
        for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, direction, rel_type] (Shard &local_shard) {
                std::vector<uint64_t> ids;
                for (auto node_id : grouped_node_ids) {
                    auto neighbor_ids = local_shard.NodeGetNeighborIds(node_id, direction, rel_type);
                    ids.insert(std::end(ids), std::begin(neighbor_ids), std::end(neighbor_ids));
                }
                return ids;
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint64_t>>& results) {
            std::vector<uint64_t> combined;

            for(const std::vector<uint64_t>& sharded : results) {
                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
            }
            // sort followed by unique, to remove all duplicates
            std::ranges::sort(combined);
            auto ret = std::ranges::unique(combined);
            combined.erase(ret.begin(), ret.end());

            return combined;
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeIdsGetNeighborIdsPeered(const std::vector<uint64_t>& ids, Direction direction, uint16_t type_id) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::vector<uint64_t>>> futures;
        for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, direction, type_id] (Shard &local_shard) {
                std::vector<uint64_t> ids;
                for (auto node_id : grouped_node_ids) {
                    auto neighbor_ids = local_shard.NodeGetNeighborIds(node_id, direction, type_id);
                    ids.insert(std::end(ids), std::begin(neighbor_ids), std::end(neighbor_ids));
                }
                return ids;
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint64_t>>& results) {
            std::vector<uint64_t> combined;

            for(const std::vector<uint64_t>& sharded : results) {
                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
            }
            // sort followed by unique, to remove all duplicates
            std::ranges::sort(combined);
            auto ret = std::ranges::unique(combined);
            combined.erase(ret.begin(), ret.end());

            return combined;
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::NodeIdsGetNeighborIdsPeered(const std::vector<uint64_t>& ids, Direction direction, const std::vector<std::string> &rel_types) {
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids = PartitionIdsByShardId(ids);

        std::vector<seastar::future<std::vector<uint64_t>>> futures;
        for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids, direction, rel_types] (Shard &local_shard) {
                std::vector<uint64_t> ids;
                for (auto node_id : grouped_node_ids) {
                    auto neighbor_ids = local_shard.NodeGetNeighborIds(node_id);
                    ids.insert(std::end(ids), std::begin(neighbor_ids), std::end(neighbor_ids));
                }
                return ids;
            });
            futures.push_back(std::move(future));
        }

        auto p = make_shared(std::move(futures));
        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<uint64_t>>& results) {
            std::vector<uint64_t> combined;

            for(const std::vector<uint64_t>& sharded : results) {
                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
            }
            // sort followed by unique, to remove all duplicates
            std::ranges::sort(combined);
            auto ret = std::ranges::unique(combined);
            combined.erase(ret.begin(), ret.end());

            return combined;
        });
    }



    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                    return local_shard.NodeGetShardedNodeIDs(type, key); })
                .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                    std::vector<seastar::future<std::vector<Node>>> futures;
                    for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                            return local_shard.NodesGet(grouped_node_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for(const std::vector<Node>& sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, const std::string& rel_type) {
        if (uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id > 0) {
            uint16_t node_shard_id = CalculateShardId(type, key);
            return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(type, key, rel_type_id); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
                        std::vector<seastar::future<std::vector<Node>>> futures;
                        for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                return local_shard.NodesGet(grouped_node_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>> &results) {
                            std::vector<Node> combined;

                            for (const std::vector<Node> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Node>>();
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, uint16_t rel_type_id) {
        if (rel_type_id > 0) {
            uint16_t node_shard_id = CalculateShardId(type, key);
            return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(type, key, rel_type_id); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
                        std::vector<seastar::future<std::vector<Node>>> futures;
                        for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                return local_shard.NodesGet(grouped_node_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>> &results) {
                            std::vector<Node> combined;

                            for (const std::vector<Node> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Node>>();
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);
        return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) { 
            return local_shard.NodeGetShardedNodeIDs(type, key, rel_types);
             })
                .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
                    std::vector<seastar::future<std::vector<Node>>> futures;
                    for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                            return local_shard.NodesGet(grouped_node_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>> &results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node> &sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                    return local_shard.NodeGetShardedNodeIDs(external_id); })
                .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                    std::vector<seastar::future<std::vector<Node>>> futures;
                    for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                            return local_shard.NodesGet(grouped_node_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for(const std::vector<Node>& sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        if ( uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id > 0) {
            return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(external_id, rel_type_id); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
                        std::vector<seastar::future<std::vector<Node>>> futures;
                        for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                return local_shard.NodesGet(grouped_node_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>> &results) {
                            std::vector<Node> combined;

                            for (const std::vector<Node> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Node>>();
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id,  uint16_t rel_type_id) {
        if (rel_type_id > 0) {
            uint16_t node_shard_id = CalculateShardId(external_id);
            return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(external_id, rel_type_id); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
                        std::vector<seastar::future<std::vector<Node>>> futures;
                        for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                return local_shard.NodesGet(grouped_node_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>> &results) {
                            std::vector<Node> combined;

                            for (const std::vector<Node> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Node>>();
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);
        return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(external_id, rel_types); })
                .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
                    std::vector<seastar::future<std::vector<Node>>> futures;
                    for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                        auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                            return local_shard.NodesGet(grouped_node_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>> &results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node> &sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }


    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        switch(direction) {
            case Direction::OUT: {
                return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                            return local_shard.NodeGetShardedOutgoingNodeIDs(type, key); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                            std::vector<seastar::future<std::vector<Node>>> futures;
                            for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                                    return local_shard.NodesGet(grouped_node_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                std::vector<Node> combined;

                                for(const std::vector<Node>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            case Direction::IN: {
                return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingNodeIDs(type, key); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                            std::vector<seastar::future<std::vector<Node>>> futures;
                            for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                                    return local_shard.NodesGet(grouped_node_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                std::vector<Node> combined;

                                for(const std::vector<Node>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetNeighborsPeered(type, key);
        }
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        if (uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id != 0) {
            switch (direction) {
                case Direction::OUT: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { 
                        return local_shard.NodeGetShardedOutgoingNodeIDs(type, key, rel_type_id); 
                        })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                                std::vector<seastar::future<std::vector<Node>>> futures;
                                for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                        return local_shard.NodesGet(grouped_node_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                    std::vector<Node> combined;

                                    for (const std::vector<Node>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                case Direction::IN: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { 
                        return local_shard.NodeGetShardedIncomingNodeIDs(type, key, rel_type_id); 
                        })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                                std::vector<seastar::future<std::vector<Node>>> futures;
                                for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                        return local_shard.NodesGet(grouped_node_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                    std::vector<Node> combined;

                                    for (const std::vector<Node>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetNeighborsPeered(type, key, rel_type_id);
            }
        }

        return seastar::make_ready_future<std::vector<Node>>();
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t rel_type_id) {
        if (rel_type_id != 0) {
            uint16_t node_shard_id = CalculateShardId(type, key);
            switch (direction) {
                case Direction::OUT: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedOutgoingNodeIDs(type, key, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                                std::vector<seastar::future<std::vector<Node>>> futures;
                                for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                        return local_shard.NodesGet(grouped_node_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                    std::vector<Node> combined;

                                    for (const std::vector<Node>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                case Direction::IN: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingNodeIDs(type, key, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                                std::vector<seastar::future<std::vector<Node>>> futures;
                                for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                        return local_shard.NodesGet(grouped_node_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                    std::vector<Node> combined;

                                    for (const std::vector<Node>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetNeighborsPeered(type, key, rel_type_id);
            }
        }

        return seastar::make_ready_future<std::vector<Node>>();
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        switch(direction) {
            case Direction::OUT: {
                return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
                            return local_shard.NodeGetShardedOutgoingNodeIDs(type, key, rel_types); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                            std::vector<seastar::future<std::vector<Node>>> futures;
                            for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                                    return local_shard.NodesGet(grouped_node_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                std::vector<Node> combined;

                                for(const std::vector<Node>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            case Direction::IN: {
                return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingNodeIDs(type, key, rel_types); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                            std::vector<seastar::future<std::vector<Node>>> futures;
                            for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                                    return local_shard.NodesGet(grouped_node_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                std::vector<Node> combined;

                                for(const std::vector<Node>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetNeighborsPeered(type, key, rel_types);
        }
    }


    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        switch(direction) {
            case Direction::OUT: {
                return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                            return local_shard.NodeGetShardedOutgoingNodeIDs(external_id); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                            std::vector<seastar::future<std::vector<Node>>> futures;
                            for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                                    return local_shard.NodesGet(grouped_node_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                std::vector<Node> combined;

                                for(const std::vector<Node>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            case Direction::IN: {
                return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingNodeIDs(external_id); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                            std::vector<seastar::future<std::vector<Node>>> futures;
                            for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                                    return local_shard.NodesGet(grouped_node_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                std::vector<Node> combined;

                                for(const std::vector<Node>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetNeighborsPeered(external_id);
        }
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, Direction direction, const std::string& rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        if (uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id != 0) {
            switch (direction) {
                case Direction::OUT: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedOutgoingNodeIDs(external_id, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                                std::vector<seastar::future<std::vector<Node>>> futures;
                                for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                        return local_shard.NodesGet(grouped_node_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                    std::vector<Node> combined;

                                    for (const std::vector<Node>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                case Direction::IN: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingNodeIDs(external_id, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                                std::vector<seastar::future<std::vector<Node>>> futures;
                                for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                        return local_shard.NodesGet(grouped_node_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                    std::vector<Node> combined;

                                    for (const std::vector<Node>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetNeighborsPeered(external_id, rel_type_id);
            }
        }

        return seastar::make_ready_future<std::vector<Node>>();
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, Direction direction, uint16_t rel_type_id) {
        if (rel_type_id != 0) {
            uint16_t node_shard_id = CalculateShardId(external_id);
            switch (direction) {
                case Direction::OUT: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedOutgoingNodeIDs(external_id, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                                std::vector<seastar::future<std::vector<Node>>> futures;
                                for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                        return local_shard.NodesGet(grouped_node_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                    std::vector<Node> combined;

                                    for (const std::vector<Node>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                case Direction::IN: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingNodeIDs(external_id, rel_type_id); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                                std::vector<seastar::future<std::vector<Node>>> futures;
                                for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                                        return local_shard.NodesGet(grouped_node_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                    std::vector<Node> combined;

                                    for (const std::vector<Node>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetNeighborsPeered(external_id, rel_type_id);
            }
        }

        return seastar::make_ready_future<std::vector<Node>>();
    }

    seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        switch(direction) {
            case Direction::OUT: {
                return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
                            return local_shard.NodeGetShardedOutgoingNodeIDs(external_id, rel_types); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                            std::vector<seastar::future<std::vector<Node>>> futures;
                            for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                                    return local_shard.NodesGet(grouped_node_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                std::vector<Node> combined;

                                for(const std::vector<Node>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            case Direction::IN: {
                return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingNodeIDs(external_id, rel_types); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                            std::vector<seastar::future<std::vector<Node>>> futures;
                            for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                                    return local_shard.NodesGet(grouped_node_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Node>>& results) {
                                std::vector<Node> combined;

                                for(const std::vector<Node>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetNeighborsPeered(external_id, rel_types);
        }
    }
}

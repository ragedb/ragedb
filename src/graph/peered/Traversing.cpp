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

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodeGetLinks(type, key);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(const std::string &type, const std::string &key, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction](Shard &local_shard) {
            return local_shard.NodeGetLinks(type, key, direction);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, rel_type](Shard &local_shard) {
            return local_shard.NodeGetLinks(type, key, direction, rel_type);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, rel_types](Shard &local_shard) {
            return local_shard.NodeGetLinks(type, key, direction, rel_types);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(const std::string &type, const std::string &key, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, rel_type](Shard &local_shard) {
            return local_shard.NodeGetLinks(type, key, Direction::BOTH, rel_type);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(const std::string &type, const std::string &key, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
            return local_shard.NodeGetLinks(type, key, Direction::BOTH, rel_types);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(uint64_t external_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
            return local_shard.NodeGetLinks(external_id);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(uint64_t external_id, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction](Shard &local_shard) {
            return local_shard.NodeGetLinks(external_id, direction);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(uint64_t external_id, Direction direction, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction, rel_type](Shard &local_shard) {
            return local_shard.NodeGetLinks(external_id, direction, rel_type);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction, rel_types](Shard &local_shard) {
            return local_shard.NodeGetLinks(external_id, direction, rel_types);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(uint64_t external_id, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, rel_type](Shard &local_shard) {
            return local_shard.NodeGetLinks(external_id, Direction::BOTH, rel_type);
        });
    }

    seastar::future<std::vector<Link>> Shard::NodeGetLinksPeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
            return local_shard.NodeGetLinks(external_id, Direction::BOTH, rel_types);
        });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                    return local_shard.NodeGetShardedRelationshipIDs(type, key); })
                .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                    std::vector<seastar::future<std::vector<Relationship>>> futures;
                    for (const auto& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                        auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                            return local_shard.RelationshipsGet(grouped_rel_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
                        std::vector<Relationship> combined;

                        for(const std::vector<Relationship>& sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::string& rel_type) {
        if (uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
            rel_type_id > 0) {
            uint16_t node_shard_id = CalculateShardId(type, key);
            return container().invoke_on(node_shard_id, [type, key, rel_type](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(type, key, rel_type); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                        std::vector<seastar::future<std::vector<Relationship>>> futures;
                        for (const auto &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                return local_shard.RelationshipsGet(grouped_rel_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>> &results) {
                            std::vector<Relationship> combined;

                            for (const std::vector<Relationship> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(type, key, rel_types); })
                .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                    std::vector<seastar::future<std::vector<Relationship>>> futures;
                    for (const auto &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                        auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                            return local_shard.RelationshipsGet(grouped_rel_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>> &results) {
                        std::vector<Relationship> combined;

                        for (const std::vector<Relationship> &sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                    return local_shard.NodeGetShardedRelationshipIDs(external_id); })
                .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                    std::vector<seastar::future<std::vector<Relationship>>> futures;
                    for (const auto& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                        auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                            return local_shard.RelationshipsGet(grouped_rel_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
                        std::vector<Relationship> combined;

                        for(const std::vector<Relationship>& sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, const std::string& rel_type) {
        if (relationship_types.getTypeId(rel_type) > 0) {
            uint16_t node_shard_id = CalculateShardId(external_id);
            return container().invoke_on(node_shard_id, [external_id, rel_type](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(external_id, rel_type); })
                    .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                        std::vector<seastar::future<std::vector<Relationship>>> futures;
                        for (const auto &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                            auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                return local_shard.RelationshipsGet(grouped_rel_ids);
                            });
                            futures.push_back(std::move(future));
                        }

                        auto p = make_shared(std::move(futures));
                        return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>> &results) {
                            std::vector<Relationship> combined;

                            for (const std::vector<Relationship> &sharded : results) {
                                combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                            }
                            return combined;
                        });
                    });
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(external_id, rel_types); })
                .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
                    std::vector<seastar::future<std::vector<Relationship>>> futures;
                    for (const auto &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                        auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                            return local_shard.RelationshipsGet(grouped_rel_ids);
                        });
                        futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>> &results) {
                        std::vector<Relationship> combined;

                        for (const std::vector<Relationship> &sharded : results) {
                            combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                    });
                });
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        switch(direction) {
            case Direction::OUT: {
                return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                    return local_shard.NodeGetOutgoingRelationships(type, key); });
            }
            case Direction::IN: {
                return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                            std::vector<seastar::future<std::vector<Relationship>>> futures;
                            for (const auto& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                                    return local_shard.RelationshipsGet(grouped_rel_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
                                std::vector<Relationship> combined;

                                for(const std::vector<Relationship>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetRelationshipsPeered(type, key);
        }
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        if (relationship_types.getTypeId(rel_type) > 0) {
            uint16_t node_shard_id = CalculateShardId(type, key);
            switch (direction) {
                case Direction::OUT: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(type, key, rel_type); });
                }
                case Direction::IN: {
                    return container().invoke_on(node_shard_id, [type, key, rel_type](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key, rel_type); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                                std::vector<seastar::future<std::vector<Relationship>>> futures;
                                for (const auto &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                        return local_shard.RelationshipsGet(grouped_rel_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
                                    std::vector<Relationship> combined;

                                    for (const std::vector<Relationship>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetRelationshipsPeered(type, key, rel_type);
            }
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        switch(direction) {
            case Direction::OUT: {
                return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
                    return local_shard.NodeGetOutgoingRelationships(type, key, rel_types); });
            }
            case Direction::IN: {
                return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key, rel_types); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                            std::vector<seastar::future<std::vector<Relationship>>> futures;
                            for (const auto& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                                    return local_shard.RelationshipsGet(grouped_rel_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
                                std::vector<Relationship> combined;

                                for(const std::vector<Relationship>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetRelationshipsPeered(type, key, rel_types);
        }
    }


    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        switch(direction) {
            case Direction::OUT: {
                return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                    return local_shard.NodeGetOutgoingRelationships(external_id); });
            }
            case Direction::IN: {
                return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                            std::vector<seastar::future<std::vector<Relationship>>> futures;
                            for (const auto& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                                    return local_shard.RelationshipsGet(grouped_rel_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
                                std::vector<Relationship> combined;

                                for(const std::vector<Relationship>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetRelationshipsPeered(external_id);
        }
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction, const std::string& rel_type) {
        if (relationship_types.getTypeId(rel_type) > 0) {
            uint16_t node_shard_id = CalculateShardId(external_id);
            switch (direction) {
                case Direction::OUT: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(external_id, rel_type); });
                }
                case Direction::IN: {
                    return container().invoke_on(node_shard_id, [external_id, rel_type](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id, rel_type); })
                            .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                                std::vector<seastar::future<std::vector<Relationship>>> futures;
                                for (const auto &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                                    auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                                        return local_shard.RelationshipsGet(grouped_rel_ids);
                                    });
                                    futures.push_back(std::move(future));
                                }

                                auto p = make_shared(std::move(futures));
                                return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
                                    std::vector<Relationship> combined;

                                    for (const std::vector<Relationship>& sharded : results) {
                                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                    }
                                    return combined;
                                });
                            });
                }
                default:
                    return NodeGetRelationshipsPeered(external_id, rel_type);
            }
        }

        return seastar::make_ready_future<std::vector<Relationship>>();
    }

    seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        switch(direction) {
            case Direction::OUT: {
                return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
                    return local_shard.NodeGetOutgoingRelationships(external_id, rel_types); });
            }
            case Direction::IN: {
                return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
                            return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id, rel_types); })
                        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                            std::vector<seastar::future<std::vector<Relationship>>> futures;
                            for (const auto& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                                auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                                    return local_shard.RelationshipsGet(grouped_rel_ids);
                                });
                                futures.push_back(std::move(future));
                            }

                            auto p = make_shared(std::move(futures));
                            return seastar::when_all_succeed(p->begin(), p->end()).then([p] (const std::vector<std::vector<Relationship>>& results) {
                                std::vector<Relationship> combined;

                                for(const std::vector<Relationship>& sharded : results) {
                                    combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                                }
                                return combined;
                            });
                        });
            }
            default: return NodeGetRelationshipsPeered(external_id, rel_types);
        }
    }

}

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

    bool Shard::NodeIndexCreate(uint16_t type_id, const std::string& property) {
        if (node_indexes.find(type_id) != node_indexes.end()) {
            auto& type_map = node_indexes[type_id];
            if (type_map.find(property) != type_map.end()) {
                return false;
            }
        }
        node_indexes[type_id].emplace(property, std::make_unique<PropertyIndex>());
        return true;
    }

    bool Shard::NodeIndexDelete(uint16_t type_id, const std::string& property) {
        if (node_indexes.find(type_id) == node_indexes.end()) {
            return false;
        }
        auto& type_map = node_indexes[type_id];
        auto it = type_map.find(property);
        if (it == type_map.end()) {
            return false;
        }
        type_map.erase(it);
        if (type_map.empty()) {
            node_indexes.erase(type_id);
        }
        return true;
    }

    bool Shard::RelationshipIndexCreate(uint16_t type_id, const std::string& property) {
        if (relationship_indexes.find(type_id) != relationship_indexes.end()) {
            auto& type_map = relationship_indexes[type_id];
            if (type_map.find(property) != type_map.end()) {
                return false;
            }
        }
        relationship_indexes[type_id].emplace(property, std::make_unique<PropertyIndex>());
        return true;
    }

    bool Shard::RelationshipIndexDelete(uint16_t type_id, const std::string& property) {
        if (relationship_indexes.find(type_id) == relationship_indexes.end()) {
            return false;
        }
        auto& type_map = relationship_indexes[type_id];
        auto it = type_map.find(property);
        if (it == type_map.end()) {
            return false;
        }
        type_map.erase(it);
        if (type_map.empty()) {
            relationship_indexes.erase(type_id);
        }
        return true;
    }

    void Shard::NodeIndexInsert(uint16_t type_id, const std::string& property, const property_type_t& value, uint64_t external_id) {
        auto type_it = node_indexes.find(type_id);
        if (type_it != node_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                prop_it->second->insert(value, external_id);
            }
        }
    }

    void Shard::NodeIndexRemove(uint16_t type_id, const std::string& property, const property_type_t& value, uint64_t external_id) {
        auto type_it = node_indexes.find(type_id);
        if (type_it != node_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                prop_it->second->remove(value, external_id);
            }
        }
    }

    void Shard::RelationshipIndexInsert(uint16_t type_id, const std::string& property, const property_type_t& value, uint64_t external_id) {
        auto type_it = relationship_indexes.find(type_id);
        if (type_it != relationship_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                prop_it->second->insert(value, external_id);
            }
        }
    }

    void Shard::RelationshipIndexRemove(uint16_t type_id, const std::string& property, const property_type_t& value, uint64_t external_id) {
        auto type_it = relationship_indexes.find(type_id);
        if (type_it != relationship_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                prop_it->second->remove(value, external_id);
            }
        }
    }

    seastar::future<bool> Shard::NodeIndexCreatePeered(const std::string& type, const std::string& property) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, property] (Shard &local_shard) {
            return local_shard.NodeIndexCreate(type_id, property);
        }).then([type, type_id, property, this](std::vector<bool> results) {
            bool success = true;
            for (bool r : results) {
                success = success && r;
            }
            if (!success) {
                return seastar::make_ready_future<bool>(false);
            }

            return container().map([type, type_id, property, this](Shard &local_shard) {
                std::vector<seastar::future<>> futures;
                size_t max_id = local_shard.node_types.getKeys(type_id).size();
                for (uint64_t internal_id = 0; internal_id < max_id; ++internal_id) {
                    if (local_shard.node_types.ValidNodeId(type_id, internal_id)) {
                        property_type_t val = local_shard.node_types.getNodeProperty(type_id, internal_id, property);
                        if (val.index() >= 1 && val.index() <= 4) {
                            uint64_t external_id = Shard::internalToExternal(type_id, internal_id);
                            uint16_t target_shard = CalculateShardId(type, property, val);
                            futures.push_back(container().invoke_on(target_shard, [type_id, property, val, external_id](Shard &target) {
                                target.NodeIndexInsert(type_id, property, val, external_id);
                            }));
                        }
                    }
                }
                return seastar::when_all_succeed(futures.begin(), futures.end()).then([]() {
                    return true;
                });
            }).then([](std::vector<bool> results) {
                return seastar::make_ready_future<bool>(true);
            });
        });
    }

    seastar::future<bool> Shard::NodeIndexDeletePeered(const std::string& type, const std::string& property) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, property] (Shard &local_shard) {
            bool std_deleted = local_shard.NodeIndexDelete(type_id, property);
            bool fts_deleted = local_shard.NodeIndexFTSDelete(type_id, property);
            return std_deleted || fts_deleted;
        }).then([](std::vector<bool> results) {
            bool success = false;
            for (bool r : results) {
                success = success || r;
            }
            return seastar::make_ready_future<bool>(success);
        });
    }

    seastar::future<bool> Shard::RelationshipIndexCreatePeered(const std::string& type, const std::string& property) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, property] (Shard &local_shard) {
            return local_shard.RelationshipIndexCreate(type_id, property);
        }).then([type, type_id, property, this](std::vector<bool> results) {
            bool success = true;
            for (bool r : results) {
                success = success && r;
            }
            if (!success) {
                return seastar::make_ready_future<bool>(false);
            }

            return container().map([type, type_id, property, this](Shard &local_shard) {
                std::vector<seastar::future<>> futures;
                size_t max_id = local_shard.relationship_types.getStartingNodeIds(type_id).size();
                for (uint64_t internal_id = 0; internal_id < max_id; ++internal_id) {
                    if (local_shard.relationship_types.ValidRelationshipId(type_id, internal_id)) {
                        property_type_t val = local_shard.relationship_types.getRelationshipProperty(type_id, internal_id, property);
                        if (val.index() >= 1 && val.index() <= 4) {
                            uint64_t external_id = Shard::internalToExternal(type_id, internal_id);
                            uint16_t target_shard = CalculateShardId(type, property, val);
                            futures.push_back(container().invoke_on(target_shard, [type_id, property, val, external_id](Shard &target) {
                                target.RelationshipIndexInsert(type_id, property, val, external_id);
                            }));
                        }
                    }
                }
                return seastar::when_all_succeed(futures.begin(), futures.end()).then([]() {
                    return true;
                });
            }).then([](std::vector<bool> results) {
                return seastar::make_ready_future<bool>(true);
            });
        });
    }

    seastar::future<bool> Shard::RelationshipIndexDeletePeered(const std::string& type, const std::string& property) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, property] (Shard &local_shard) {
            bool std_deleted = local_shard.RelationshipIndexDelete(type_id, property);
            bool fts_deleted = local_shard.RelationshipIndexFTSDelete(type_id, property);
            return std_deleted || fts_deleted;
        }).then([](std::vector<bool> results) {
            bool success = false;
            for (bool r : results) {
                success = success || r;
            }
            return seastar::make_ready_future<bool>(success);
        });
    }

    std::vector<uint64_t> Shard::NodeIndexLookup(uint16_t type_id, const std::string& property, Operation operation, const property_type_t& value) {
        auto type_it = node_indexes.find(type_id);
        if (type_it != node_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                return prop_it->second->lookup_range(operation, value);
            }
        }
        return {};
    }

    std::vector<uint64_t> Shard::RelationshipIndexLookup(uint16_t type_id, const std::string& property, Operation operation, const property_type_t& value) {
        auto type_it = relationship_indexes.find(type_id);
        if (type_it != relationship_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                return prop_it->second->lookup_range(operation, value);
            }
        }
        return {};
    }

    std::map<std::string, std::vector<std::string>> Shard::NodeIndexesGet() {
        std::map<std::string, std::vector<std::string>> result;
        for (const auto& [type_id, prop_map] : node_indexes) {
            std::string type_name = node_types.getType(type_id);
            if (!type_name.empty()) {
                std::vector<std::string> properties;
                for (const auto& [prop_name, _] : prop_map) {
                    properties.push_back(prop_name);
                }
                if (!properties.empty()) {
                    result[type_name] = properties;
                }
            }
        }
        return result;
    }

    std::map<std::string, std::vector<std::string>> Shard::RelationshipIndexesGet() {
        std::map<std::string, std::vector<std::string>> result;
        for (const auto& [type_id, prop_map] : relationship_indexes) {
            std::string type_name = relationship_types.getType(type_id);
            if (!type_name.empty()) {
                std::vector<std::string> properties;
                for (const auto& [prop_name, _] : prop_map) {
                    properties.push_back(prop_name);
                }
                if (!properties.empty()) {
                    result[type_name] = properties;
                }
            }
        }
        return result;
    }

    bool Shard::NodeIndexExists(const std::string& type, const std::string& property) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) return false;
        auto type_it = node_indexes.find(type_id);
        if (type_it == node_indexes.end()) return false;
        return type_it->second.find(property) != type_it->second.end();
    }

    bool Shard::RelationshipIndexExists(const std::string& type, const std::string& property) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) return false;
        auto type_it = relationship_indexes.find(type_id);
        if (type_it == relationship_indexes.end()) return false;
        return type_it->second.find(property) != type_it->second.end();
    }

    bool Shard::NodeIndexFTSCreate(uint16_t type_id, const std::string& property) {
        if (node_fts_indexes.find(type_id) != node_fts_indexes.end()) {
            auto& type_map = node_fts_indexes[type_id];
            if (type_map.find(property) != type_map.end()) {
                return false;
            }
        }
        node_fts_indexes[type_id].emplace(property, std::make_unique<FullTextIndex>());
        return true;
    }

    bool Shard::NodeIndexFTSDelete(uint16_t type_id, const std::string& property) {
        if (node_fts_indexes.find(type_id) == node_fts_indexes.end()) {
            return false;
        }
        auto& type_map = node_fts_indexes[type_id];
        auto it = type_map.find(property);
        if (it == type_map.end()) {
            return false;
        }
        type_map.erase(it);
        if (type_map.empty()) {
            node_fts_indexes.erase(type_id);
        }
        return true;
    }

    bool Shard::RelationshipIndexFTSCreate(uint16_t type_id, const std::string& property) {
        if (relationship_fts_indexes.find(type_id) != relationship_fts_indexes.end()) {
            auto& type_map = relationship_fts_indexes[type_id];
            if (type_map.find(property) != type_map.end()) {
                return false;
            }
        }
        relationship_fts_indexes[type_id].emplace(property, std::make_unique<FullTextIndex>());
        return true;
    }

    bool Shard::RelationshipIndexFTSDelete(uint16_t type_id, const std::string& property) {
        if (relationship_fts_indexes.find(type_id) == relationship_fts_indexes.end()) {
            return false;
        }
        auto& type_map = relationship_fts_indexes[type_id];
        auto it = type_map.find(property);
        if (it == type_map.end()) {
            return false;
        }
        type_map.erase(it);
        if (type_map.empty()) {
            relationship_fts_indexes.erase(type_id);
        }
        return true;
    }

    void Shard::NodeIndexFTSInsert(uint16_t type_id, const std::string& property, const property_type_t& value, uint64_t external_id) {
        auto type_it = node_fts_indexes.find(type_id);
        if (type_it != node_fts_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                if (value.index() == 4) { // std::string
                    prop_it->second->insert(std::get<std::string>(value), external_id);
                }
            }
        }
    }

    void Shard::NodeIndexFTSRemove(uint16_t type_id, const std::string& property, const property_type_t& value, uint64_t external_id) {
        auto type_it = node_fts_indexes.find(type_id);
        if (type_it != node_fts_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                prop_it->second->remove(external_id);
            }
        }
    }

    void Shard::RelationshipIndexFTSInsert(uint16_t type_id, const std::string& property, const property_type_t& value, uint64_t external_id) {
        auto type_it = relationship_fts_indexes.find(type_id);
        if (type_it != relationship_fts_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                if (value.index() == 4) { // std::string
                    prop_it->second->insert(std::get<std::string>(value), external_id);
                }
            }
        }
    }

    void Shard::RelationshipIndexFTSRemove(uint16_t type_id, const std::string& property, const property_type_t& value, uint64_t external_id) {
        auto type_it = relationship_fts_indexes.find(type_id);
        if (type_it != relationship_fts_indexes.end()) {
            auto prop_it = type_it->second.find(property);
            if (prop_it != type_it->second.end() && prop_it->second) {
                prop_it->second->remove(external_id);
            }
        }
    }

    std::vector<std::pair<uint64_t, double>> Shard::NodeIndexFTSSearch(const std::string& type, const std::vector<std::string>& properties, const std::string& query, const std::map<std::string, std::string>& options) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) return {};

        bool fuzzy = false;
        if (auto it = options.find("fuzzy"); it != options.end()) {
            fuzzy = (it->second == "true" || it->second == "TRUE");
        }

        std::vector<std::pair<uint64_t, double>> results;
        std::map<uint64_t, double> merged_scores;
        for (const auto& property : properties) {
            auto type_it = node_fts_indexes.find(type_id);
            if (type_it != node_fts_indexes.end()) {
                auto prop_it = type_it->second.find(property);
                if (prop_it != type_it->second.end() && prop_it->second) {
                    auto prop_results = prop_it->second->search(query, fuzzy);
                    for (const auto& [ext_id, score] : prop_results) {
                        if (merged_scores.find(ext_id) == merged_scores.end() || merged_scores[ext_id] < score) {
                            merged_scores[ext_id] = score;
                        }
                    }
                }
            }
        }
        for (const auto& [ext_id, score] : merged_scores) {
            results.push_back({ext_id, score});
        }
        std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
            return a.second > b.second;
        });
        return results;
    }

    std::vector<std::pair<uint64_t, double>> Shard::RelationshipIndexFTSSearch(const std::string& type, const std::vector<std::string>& properties, const std::string& query, const std::map<std::string, std::string>& options) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) return {};

        bool fuzzy = false;
        if (auto it = options.find("fuzzy"); it != options.end()) {
            fuzzy = (it->second == "true" || it->second == "TRUE");
        }

        std::vector<std::pair<uint64_t, double>> results;
        std::map<uint64_t, double> merged_scores;
        for (const auto& property : properties) {
            auto type_it = relationship_fts_indexes.find(type_id);
            if (type_it != relationship_fts_indexes.end()) {
                auto prop_it = type_it->second.find(property);
                if (prop_it != type_it->second.end() && prop_it->second) {
                    auto prop_results = prop_it->second->search(query, fuzzy);
                    for (const auto& [ext_id, score] : prop_results) {
                        if (merged_scores.find(ext_id) == merged_scores.end() || merged_scores[ext_id] < score) {
                            merged_scores[ext_id] = score;
                        }
                    }
                }
            }
        }
        for (const auto& [ext_id, score] : merged_scores) {
            results.push_back({ext_id, score});
        }
        std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
            return a.second > b.second;
        });
        return results;
    }

    seastar::future<bool> Shard::NodeIndexFTSCreatePeered(const std::string& type, const std::string& property) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, property] (Shard &local_shard) {
            return local_shard.NodeIndexFTSCreate(type_id, property);
        }).then([type_id, property, this](std::vector<bool> results) {
            bool success = true;
            for (bool r : results) {
                success = success && r;
            }
            if (!success) {
                return seastar::make_ready_future<bool>(false);
            }

            return container().map([type_id, property](Shard &local_shard) {
                size_t max_id = local_shard.node_types.getKeys(type_id).size();
                for (uint64_t internal_id = 0; internal_id < max_id; ++internal_id) {
                    if (local_shard.node_types.ValidNodeId(type_id, internal_id)) {
                        property_type_t val = local_shard.node_types.getNodeProperty(type_id, internal_id, property);
                        if (val.index() == 4) { // std::string
                            uint64_t external_id = Shard::internalToExternal(type_id, internal_id);
                            local_shard.NodeIndexFTSInsert(type_id, property, val, external_id);
                        }
                    }
                }
                return true;
            }).then([](std::vector<bool> results) {
                return seastar::make_ready_future<bool>(true);
            });
        });
    }

    seastar::future<bool> Shard::NodeIndexFTSDeletePeered(const std::string& type, const std::string& property) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, property] (Shard &local_shard) {
            return local_shard.NodeIndexFTSDelete(type_id, property);
        }).then([](std::vector<bool> results) {
            bool success = true;
            for (bool r : results) {
                success = success && r;
            }
            return seastar::make_ready_future<bool>(success);
        });
    }

    seastar::future<bool> Shard::RelationshipIndexFTSCreatePeered(const std::string& type, const std::string& property) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, property] (Shard &local_shard) {
            return local_shard.RelationshipIndexFTSCreate(type_id, property);
        }).then([type_id, property, this](std::vector<bool> results) {
            bool success = true;
            for (bool r : results) {
                success = success && r;
            }
            if (!success) {
                return seastar::make_ready_future<bool>(false);
            }

            return container().map([type_id, property](Shard &local_shard) {
                size_t max_id = local_shard.relationship_types.getStartingNodeIds(type_id).size();
                for (uint64_t internal_id = 0; internal_id < max_id; ++internal_id) {
                    if (local_shard.relationship_types.ValidRelationshipId(type_id, internal_id)) {
                        property_type_t val = local_shard.relationship_types.getRelationshipProperty(type_id, internal_id, property);
                        if (val.index() == 4) { // std::string
                            uint64_t external_id = Shard::internalToExternal(type_id, internal_id);
                            local_shard.RelationshipIndexFTSInsert(type_id, property, val, external_id);
                        }
                    }
                }
                return true;
            }).then([](std::vector<bool> results) {
                return seastar::make_ready_future<bool>(true);
            });
        });
    }

    seastar::future<bool> Shard::RelationshipIndexFTSDeletePeered(const std::string& type, const std::string& property) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) {
            return seastar::make_ready_future<bool>(false);
        }

        return container().map([type_id, property] (Shard &local_shard) {
            return local_shard.RelationshipIndexFTSDelete(type_id, property);
        }).then([](std::vector<bool> results) {
            bool success = true;
            for (bool r : results) {
                success = success && r;
            }
            return seastar::make_ready_future<bool>(success);
        });
    }

    seastar::future<std::vector<std::pair<uint64_t, double>>> Shard::NodeIndexFTSSearchPeered(const std::string& type, const std::vector<std::string>& properties, const std::string& query, const std::map<std::string, std::string>& options) {
        return container().map([type, properties, query, options](Shard& local_shard) {
            return local_shard.NodeIndexFTSSearch(type, properties, query, options);
        }).then([](std::vector<std::vector<std::pair<uint64_t, double>>> shard_results) {
            std::map<uint64_t, double> merged_scores;
            for (const auto& local_res : shard_results) {
                for (const auto& [ext_id, score] : local_res) {
                    if (merged_scores.find(ext_id) == merged_scores.end() || merged_scores[ext_id] < score) {
                        merged_scores[ext_id] = score;
                    }
                }
            }
            std::vector<std::pair<uint64_t, double>> results;
            for (const auto& [ext_id, score] : merged_scores) {
                results.push_back({ext_id, score});
            }
            std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });
            if (results.size() > 100) {
                results.resize(100);
            }
            return results;
        });
    }

    seastar::future<std::vector<std::pair<uint64_t, double>>> Shard::RelationshipIndexFTSSearchPeered(const std::string& type, const std::vector<std::string>& properties, const std::string& query, const std::map<std::string, std::string>& options) {
        return container().map([type, properties, query, options](Shard& local_shard) {
            return local_shard.RelationshipIndexFTSSearch(type, properties, query, options);
        }).then([](std::vector<std::vector<std::pair<uint64_t, double>>> shard_results) {
            std::map<uint64_t, double> merged_scores;
            for (const auto& local_res : shard_results) {
                for (const auto& [ext_id, score] : local_res) {
                    if (merged_scores.find(ext_id) == merged_scores.end() || merged_scores[ext_id] < score) {
                        merged_scores[ext_id] = score;
                    }
                }
            }
            std::vector<std::pair<uint64_t, double>> results;
            for (const auto& [ext_id, score] : merged_scores) {
                results.push_back({ext_id, score});
            }
            std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });
            if (results.size() > 100) {
                results.resize(100);
            }
            return results;
        });
    }

    std::map<std::string, std::vector<std::string>> Shard::NodeIndexesFTSGet() {
        std::map<std::string, std::vector<std::string>> result;
        for (const auto& [type_id, prop_map] : node_fts_indexes) {
            std::string type_name = node_types.getType(type_id);
            if (!type_name.empty()) {
                std::vector<std::string> properties;
                for (const auto& [prop_name, _] : prop_map) {
                    properties.push_back(prop_name);
                }
                if (!properties.empty()) {
                    result[type_name] = properties;
                }
            }
        }
        return result;
    }

    std::map<std::string, std::vector<std::string>> Shard::RelationshipIndexesFTSGet() {
        std::map<std::string, std::vector<std::string>> result;
        for (const auto& [type_id, prop_map] : relationship_fts_indexes) {
            std::string type_name = relationship_types.getType(type_id);
            if (!type_name.empty()) {
                std::vector<std::string> properties;
                for (const auto& [prop_name, _] : prop_map) {
                    properties.push_back(prop_name);
                }
                if (!properties.empty()) {
                    result[type_name] = properties;
                }
            }
        }
        return result;
    }

    bool Shard::NodeIndexFTSExists(const std::string& type, const std::string& property) {
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id == 0) return false;
        auto type_it = node_fts_indexes.find(type_id);
        if (type_it == node_fts_indexes.end()) return false;
        return type_it->second.find(property) != type_it->second.end();
    }

    bool Shard::RelationshipIndexFTSExists(const std::string& type, const std::string& property) {
        uint16_t type_id = relationship_types.getTypeId(type);
        if (type_id == 0) return false;
        auto type_it = relationship_fts_indexes.find(type_id);
        if (type_it == relationship_fts_indexes.end()) return false;
        return type_it->second.find(property) != type_it->second.end();
    }

}

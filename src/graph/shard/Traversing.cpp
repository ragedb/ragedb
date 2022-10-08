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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id, rel_types);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_relationships_ids.try_emplace(i);
        }

        for (const auto& [rel_type_id, links] : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            for (Link link : links) {
                sharded_relationships_ids.at(shard_id).emplace_back(link.rel_id);
            }
        }

        for (const auto& [rel_type_id, links] : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            for (Link link : links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_relationships_ids.at(i).empty()) {
                sharded_relationships_ids.erase(i);
            }
        }

        return sharded_relationships_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id, const std::string& rel_type) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_relationships_ids.try_emplace(i);
        }

        auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getOutgoingRelationships(type_id).at(internal_id))) {
            for(Link link : group->links) {
                sharded_relationships_ids.at(shard_id).emplace_back(link.rel_id);
            }
        }

        group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
            for(Link link : group->links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_relationships_ids.at(i).empty()) {
                sharded_relationships_ids.erase(i);
            }
        }

        return sharded_relationships_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id,const std::vector<std::string> &rel_types) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint64_t internal_id = externalToInternal(id);
        uint16_t node_type_id = externalToTypeId(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_relationships_ids.try_emplace(i);
        }

        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id == 0) {
                continue;
            }
            auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    sharded_relationships_ids.at(shard_id).emplace_back(link.rel_id);
                }
            }

            group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                }
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_relationships_ids.at(i).empty()) {
                sharded_relationships_ids.erase(i);
            }
        }

        return sharded_relationships_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedNodeIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedNodeIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedNodeIDs(id, rel_types);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint64_t internal_id = externalToInternal(id);
        uint16_t node_type_id = externalToTypeId(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }

        for (const auto &[rel_type_id, links] : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            for (Link link : links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
            }
        }

        for (const auto &[rel_type_id, links] : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            for (Link link : links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id, const std::string& rel_type)
    {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }

        auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                             [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
            for(Link link : group->links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
            }
        }

        group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                        [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
            for(Link link : group->links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if (sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id,const std::vector<std::string> &rel_types) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }

        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id == 0) {
                continue;
            }
            auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
                }
            }

            group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                }
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetOutgoingRelationships(id);
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetOutgoingRelationships(id, rel_type);
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetOutgoingRelationships(id, rel_types);
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id) {
        std::vector<Relationship> node_relationships;
        if (!ValidNodeId(id)) {
            return node_relationships;
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        for (const auto& [rel_type_id, links] : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            for (Link link : links) {
                node_relationships.emplace_back(relationship_types.getRelationship(link.rel_id));
            }
        }
        return node_relationships;
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, const std::string& rel_type) {
        std::vector<Relationship> node_relationships;
        if (!ValidNodeId(id)) {
            return node_relationships;
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);

        auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                             [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
            for(Link link : group->links) {
                node_relationships.emplace_back(relationship_types.getRelationship(link.rel_id));
            }
        }
        return node_relationships;
    }

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, const std::vector<std::string> &rel_types) {
        std::vector<Relationship> node_relationships;
        if (!ValidNodeId(id)) {
            return node_relationships;
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id == 0) {
                continue;
            }
            auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    node_relationships.emplace_back(relationship_types.getRelationship(link.rel_id));
                }
            }

        }
        return node_relationships;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingRelationshipIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingRelationshipIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingRelationshipIDs(id, rel_types);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_relationships_ids.try_emplace(i);
        }

        for (const auto& [rel_type_id, links]  : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            for (Link link : links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_relationships_ids.at(node_shard_id).push_back(link.rel_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_relationships_ids.at(i).empty()) {
                sharded_relationships_ids.erase(i);
            }
        }

        return sharded_relationships_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::string& rel_type) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_relationships_ids.try_emplace(i);
        }

        auto group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
            for(Link link : group->links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_relationships_ids.at(i).empty()) {
                sharded_relationships_ids.erase(i);
            }
        }

        return sharded_relationships_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::vector<std::string> &rel_types) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }
            uint16_t node_type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
            for (uint16_t i = 0; i < cpus; i++) {
                sharded_relationships_ids.try_emplace(i);
            }
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id == 0) {
                    continue;
                }
                auto group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                    for(Link link : group->links) {
                        uint16_t node_shard_id = CalculateShardId(link.node_id);
                        sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                    }
                }
            }

            for (uint16_t i = 0; i < cpus; i++) {
                if(sharded_relationships_ids.at(i).empty()) {
                    sharded_relationships_ids.erase(i);
                }
            }

            return sharded_relationships_ids;

    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingNodeIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingNodeIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingNodeIDs(id, rel_types);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }

        for (const auto &[type_id, links] : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            for (Link link : links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, const std::string& rel_type) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }

        auto group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
            for(Link link : group->links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types)
    {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }
        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id == 0) {
                continue;
            }
            auto group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                }
            }

        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedOutgoingNodeIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedOutgoingNodeIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedOutgoingNodeIDs(id, rel_types);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }

        for (const auto& [rel_type_id, links]  : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            for (Link link : links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::string& rel_type) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }

        auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
            for(Link link : group->links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types) {
        if (!ValidNodeId(id)) {
        return {};
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.try_emplace(i);
        }
        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id == 0) {
                continue;
            }
            auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                for(Link link : group->links) {
                    uint16_t node_shard_id = CalculateShardId(link.node_id);
                    sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
                }
            }
        }

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_nodes_ids.at(i).empty()) {
                sharded_nodes_ids.erase(i);
            }
        }

        return sharded_nodes_ids;
    }

}

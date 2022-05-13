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

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetLinks(id);
    }

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, Direction direction) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetLinks(id, direction);
    }

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetLinks(id, direction, rel_type);
    }

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, Direction direction, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetLinks(id, direction, type_id);
    }

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetLinks(id, direction, rel_types);
    }
    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, uint64_t id2) {
      uint64_t id = NodeGetID(type, key);
      return NodeGetLinks(id, id2);
    }

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, uint64_t id2, Direction direction) {
      uint64_t id = NodeGetID(type, key);
      return NodeGetLinks(id, id2, direction);
    }

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, uint64_t id2, Direction direction, const std::string &rel_type) {
      uint64_t id = NodeGetID(type, key);
      return NodeGetLinks(id, id2, direction, rel_type);
    }

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, uint64_t id2, Direction direction, uint16_t type_id) {
      uint64_t id = NodeGetID(type, key);
      return NodeGetLinks(id, id2, direction, type_id);
    }

    std::vector<Link> Shard::NodeGetLinks(const std::string &type, const std::string &key, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types) {
      uint64_t id = NodeGetID(type, key);
      return NodeGetLinks(id, id2, direction, rel_types);
    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id) {
        if (!ValidNodeId(id)) {
            return std::vector<Link>();
        }

        uint64_t internal_id = externalToInternal(id);
        uint16_t type_id = externalToTypeId(id);
        std::vector<Link> ids;
        for (const auto &[type, list] : node_types.getOutgoingRelationships(type_id).at(internal_id)) {
            std::ranges::copy(list, std::back_inserter(ids));
        }
        for (const auto &[type, list] : node_types.getIncomingRelationships(type_id).at(internal_id)) {
            std::ranges::copy(list, std::back_inserter(ids));
        }
        return ids;

    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, Direction direction) {
        if (!ValidNodeId(id)) {
            return std::vector<Link>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<Link> ids;
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
          for (const auto &[type, list] : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
              std::ranges::copy(list, std::back_inserter(ids));
          }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
          for (const auto &[type, list] : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
              std::ranges::copy(list, std::back_inserter(ids));
          }
        }
        return ids;

    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, Direction direction, const std::string &rel_type) {
        if (!ValidNodeId(id)) {
            return std::vector<Link>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::vector<Link> ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
          auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id](const Group &g) { return g.rel_type_id == type_id; });

          if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
            ids.reserve(out_group->links.size());
            std::ranges::copy(out_group->links, std::back_inserter(ids));
          }
        }

        if (direction != Direction::OUT) {
          auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
            [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
            ids.reserve(ids.size() + in_group->links.size());
            std::ranges::copy(in_group->links, std::back_inserter(ids));
          }
        }
        return ids;
    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, Direction direction, uint16_t type_id) {
        if (!ValidNodeId(id)) {
          return std::vector<Link>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);

        std::vector<Link> ids;

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
          auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
              [type_id](const Group &g) { return g.rel_type_id == type_id; });

          if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
            ids.reserve(out_group->links.size());
            std::ranges::copy(out_group->links, std::back_inserter(ids));
          }
        }

        if (direction != Direction::OUT) {
          auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
            [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
            ids.reserve(ids.size() + in_group->links.size());
            std::ranges::copy(in_group->links, std::back_inserter(ids));
          }
        }
        return ids;
    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        if (!ValidNodeId(id)) {
            return std::vector<Link>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<Link> ids;
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
          // For each requested type sum up the values
          for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {
              auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

              if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                  std::ranges::copy(out_group->links, std::back_inserter(ids));
              }
            }
          }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
          // For each requested type sum up the values
          for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {
              auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

              if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                  std::ranges::copy(in_group->links, std::back_inserter(ids));
              }
            }
          }
        }
        return ids;

    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, uint64_t id2) {
        if (!ValidNodeId(id)) {
          return std::vector<Link>();
        }

        uint64_t internal_id = externalToInternal(id);
        uint16_t type_id = externalToTypeId(id);
        std::vector<Link> ids;
        for (const auto &[type, list] : node_types.getOutgoingRelationships(type_id).at(internal_id)) {
          std::ranges::copy_if(list, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
        }
        for (const auto &[type, list] : node_types.getIncomingRelationships(type_id).at(internal_id)) {
          std::ranges::copy_if(list, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
        }
        return ids;
    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, uint64_t id2, Direction direction) {
        if (!ValidNodeId(id)) {
          return std::vector<Link>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<Link> ids;
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
          for (const auto &[type, list] : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            std::ranges::copy_if(list, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
          }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
          for (const auto &[type, list] : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            std::ranges::copy_if(list, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
          }
        }
        return ids;
    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, uint64_t id2, Direction direction, const std::string &rel_type) {
        if (!ValidNodeId(id)) {
            return std::vector<Link>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        uint64_t internal_id = externalToInternal(id);
        std::vector<Link> ids;
        // Use the two ifs to handle ALL for a direction
        auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
            std::ranges::copy_if(out_group->links, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
        }
        if (direction != Direction::OUT) {
            std::ranges::copy_if(in_group->links, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
        }
        return ids;
    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, uint64_t id2, Direction direction, uint16_t type_id) {
        if (!ValidNodeId(id)) {
            return std::vector<Link>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        if (direction == Direction::IN) {
          auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
            [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
            return in_group->links;
          }
        }

        if (direction == Direction::OUT) {
          auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
            [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
            return out_group->links;
          }
        }

        std::vector<Link> ids;

        auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        std::ranges::copy_if(out_group->links, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
        std::ranges::copy_if(in_group->links, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });

        return ids;
    }

    std::vector<Link> Shard::NodeGetLinks(uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> &rel_types) {
        if (!ValidNodeId(id)) {
            return std::vector<Link>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::vector<Link> ids;
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::IN) {
          // For each requested type sum up the values
          for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {
              auto out_group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

              if (out_group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                  std::ranges::copy_if(out_group->links, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
              }
            }
          }
        }
        // Use the two ifs to handle ALL for a direction
        if (direction != Direction::OUT) {
          // For each requested type sum up the values
          for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {
              auto in_group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

              if (in_group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                  std::ranges::copy_if(in_group->links, std::back_inserter(ids), [id2](Link link) { return link.node_id == id2; });
              }
            }
          }
        }
        return ids;
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id, rel_type);
    }

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedRelationshipIDs(id, type_id);
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
            sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
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
            sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id, uint16_t type_id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }
        uint64_t internal_id = externalToInternal(id);
        uint16_t node_type_id = externalToTypeId(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
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

        for (uint16_t i = 0; i < cpus; i++) {
            if(sharded_relationships_ids.at(i).empty()) {
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
            sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
        }

        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {

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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);

        return NodeGetShardedNodeIDs(id, type_id);
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
        }

        auto group = find_if(std::begin(node_types.getOutgoingRelationships(node_type_id).at(internal_id)), std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id)),
                             [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
            for(Link link : group->links) {
                uint16_t node_shard_id = CalculateShardId(link.node_id);
                sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
            }
        }

        group = find_if(std::begin(node_types.getIncomingRelationships(node_type_id).at(internal_id)), std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id)),
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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id, uint16_t type_id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
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
            if(sharded_nodes_ids.at(i).empty()) {
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
        }

        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {
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

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetOutgoingRelationships(id, type_id);
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
        for (auto &types : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            for (Link link : types.links) {
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

    std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, uint16_t type_id) {
        std::vector<Relationship> node_relationships;
        if (!ValidNodeId(id)) {
            return node_relationships;
        }

        uint16_t node_type_id = externalToTypeId(id);
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
            if (type_id > 0) {
                auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                    for(Link link : group->links) {
                        node_relationships.emplace_back(relationship_types.getRelationship(link.rel_id));
                    }
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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingRelationshipIDs(id, type_id);
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
            sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
        }

        for (auto &types : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            for (Link link : types.links) {
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
            sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, uint16_t type_id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
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
                sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
            }
            for (const auto &rel_type : rel_types) {
                uint16_t type_id = relationship_types.getTypeId(rel_type);
                if (type_id > 0) {
                    auto group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                      [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                    if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                        for(Link link : group->links) {
                            uint16_t node_shard_id = CalculateShardId(link.node_id);
                            sharded_relationships_ids.at(node_shard_id).emplace_back(link.rel_id);
                        }
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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedIncomingNodeIDs(id, type_id);
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, uint16_t type_id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }
        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
        }
        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {
                auto group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                    for(Link link : group->links) {
                        uint16_t node_shard_id = CalculateShardId(link.node_id);
                        sharded_nodes_ids.at(node_shard_id).emplace_back(link.node_id);
                    }
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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
        uint64_t id = NodeGetID(type, key);
        return NodeGetShardedOutgoingNodeIDs(id, type_id);
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
        }

        for (auto &types : node_types.getOutgoingRelationships(node_type_id).at(internal_id)) {
            for (Link link : types.links) {
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
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

    std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, uint16_t type_id) {
        if (!ValidNodeId(id)) {
            return std::map<uint16_t, std::vector<uint64_t>>();
        }

        uint16_t node_type_id = externalToTypeId(id);
        uint64_t internal_id = externalToInternal(id);
        std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
        for (uint16_t i = 0; i < cpus; i++) {
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
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
            sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
        }
        for (const auto &rel_type : rel_types) {
            uint16_t type_id = relationship_types.getTypeId(rel_type);
            if (type_id > 0) {
                auto group = std::ranges::find_if(node_types.getOutgoingRelationships(node_type_id).at(internal_id),
                  [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

                if (group != std::end(node_types.getOutgoingRelationships(node_type_id).at(internal_id))) {
                    for(Link link : group->links) {
                        uint16_t node_shard_id = CalculateShardId(link.node_id);
                        sharded_nodes_ids.at(node_shard_id).push_back(link.node_id);
                    }
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

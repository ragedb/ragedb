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

    std::map<Link, std::vector<Link>> Shard::LinksGetLinks(const std::vector<Link>& links) {
      std::map<Link, std::vector<Link>> link_rels;
      for (Link link : links) {
        link_rels[link] = NodeGetLinks(link.node_id);
      }
      return link_rels;
    }

    std::map<Link, std::vector<Link>> Shard::LinksGetLinks(const std::vector<Link>& links, Direction direction) {
      std::map<Link, std::vector<Link>> link_rels;
      for (Link link : links) {
        link_rels[link] = NodeGetLinks(link.node_id, direction);
      }
      return link_rels;
    }

    std::map<Link, std::vector<Link>> Shard::LinksGetLinks(const std::vector<Link>& links, Direction direction, const std::string& rel_type) {
      std::map<Link, std::vector<Link>> link_rels;
      for (Link link : links) {
        link_rels[link] = NodeGetLinks(link.node_id, direction, rel_type);
      }
      return link_rels;
    }

    std::map<Link, std::vector<Link>> Shard::LinksGetLinks(const std::vector<Link>& links, Direction direction, const std::vector<std::string> &rel_types) {
      std::map<Link, std::vector<Link>> link_rels;
      for (const auto &link : links) {
        link_rels[link] = NodeGetLinks(link.node_id, direction, rel_types);
      }
      return link_rels;
    }

    std::map<Link, std::vector<Relationship>> Shard::LinksGetRelationships(const std::map<Link, std::vector<Link>>& links) {
      std::map<Link, std::vector<Relationship>> link_rels;
      for (const auto &[key, vector_of_links] : links) {
        for (const auto link : vector_of_links) {
          link_rels[key].push_back(RelationshipGet(link.rel_id));
        }
      }
      return link_rels;
    }

    std::map<Link, std::vector<Node>> Shard::LinksGetNeighbors(const std::map<Link, std::vector<Link>>& links) {
      std::map<Link, std::vector<Node>> link_nodes;
      for (const auto &[key, vector_of_links] : links) {
        for (const auto link : vector_of_links) {
          link_nodes[key].push_back(NodeGet(link.node_id));
        }
      }
      return link_nodes;
    }

    std::map<uint16_t, std::map<Link, std::vector<Link>>> Shard::LinksGetShardedIncomingLinks(const std::vector<Link>& links) {
      std::map<uint16_t, std::map<Link, std::vector<Link>>> sharded_link_links;

      for (uint16_t i = 0; i < cpus; i++) {
        sharded_link_links.insert({i, std::map<Link, std::vector<Link>>() });
      }

      for (const auto& ids : links) {
        uint64_t id = ids.node_id;
        if (ValidNodeId(id)) {
          uint16_t node_type_id = externalToTypeId(id);
          uint64_t internal_id = externalToInternal(id);

          for (const auto &types : node_types.getIncomingRelationships(node_type_id).at(internal_id)) {
            for (const auto &link : types.links()) {
              uint16_t node_shard_id = CalculateShardId(link.node_id);
              sharded_link_links.at(node_shard_id)[ids].push_back(link);
            }
          }
        }
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if(sharded_link_links.at(i).empty()) {
          sharded_link_links.erase(i);
        }
      }

      return sharded_link_links;
    }

    std::map<uint16_t, std::map<Link, std::vector<Link>>> Shard::LinksGetShardedIncomingLinks(const std::vector<Link>& links, const std::string& rel_type){
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        std::map<uint16_t, std::map<Link, std::vector<Link>>> sharded_link_links;

        for (uint16_t i = 0; i < cpus; i++) {
            sharded_link_links.insert({i, std::map<Link, std::vector<Link>>() });
        }

        for (const auto& ids : links) {
            uint64_t id = ids.node_id;
            if (ValidNodeId(id)) {
              uint16_t node_type_id = externalToTypeId(id);

              uint64_t internal_id = externalToInternal(id);

              auto group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id), [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

              if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for(const auto &link : group->links()) {
                  uint16_t node_shard_id = CalculateShardId(link.node_id);
                  sharded_link_links.at(node_shard_id)[ids].push_back(link);
                }
              }
            }
        }

      for (uint16_t i = 0; i < cpus; i++) {
        if(sharded_link_links.at(i).empty()) {
          sharded_link_links.erase(i);
        }
      }

      return sharded_link_links;
    }

    std::map<uint16_t, std::map<Link, std::vector<Link>>> Shard::LinksGetShardedIncomingLinks(const std::vector<Link>& links, const std::vector<std::string> &rel_types) {
      std::map<uint16_t, std::map<Link, std::vector<Link>>> sharded_link_links;

      for (uint16_t i = 0; i < cpus; i++) {
        sharded_link_links.insert({i, std::map<Link, std::vector<Link>>() });
      }
      std::vector<uint16_t> rel_type_ids;
      for (const auto &rel_type : rel_types) {
          uint16_t type_id = relationship_types.getTypeId(rel_type);
          if (type_id > 0) {
              rel_type_ids.emplace_back(type_id);
          }
      }
      for (const auto& ids : links) {
        uint64_t id = ids.node_id;
        if (ValidNodeId(id)) {
          uint16_t node_type_id = externalToTypeId(id);
          uint64_t internal_id = externalToInternal(id);

          for (const auto& type_id : rel_type_ids) {
              auto group = std::ranges::find_if(node_types.getIncomingRelationships(node_type_id).at(internal_id), [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

              if (group != std::end(node_types.getIncomingRelationships(node_type_id).at(internal_id))) {
                for (const auto &link : group->links()) {
                  uint16_t node_shard_id = CalculateShardId(link.node_id);
                  sharded_link_links.at(node_shard_id)[ids].push_back(link);
                }
              }

          }
        }
      }

      for (uint16_t i = 0; i < cpus; i++) {
        if(sharded_link_links.at(i).empty()) {
          sharded_link_links.erase(i);
        }
      }

      return sharded_link_links;
    }


}
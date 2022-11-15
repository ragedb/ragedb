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

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksViaLua(std::vector<Link> links) {
        return sol::as_nested(LinksGetLinksPeered(links).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksForDirectionViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_nested(LinksGetLinksPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_nested(LinksGetLinksPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_nested(LinksGetLinksPeered(links, direction, rel_types).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsViaLua(std::vector<Link> links) {
      return sol::as_nested(LinksGetRelationshipsPeered(links).get0());
    }
    
    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsForDirectionViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_nested(LinksGetRelationshipsPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_nested(LinksGetRelationshipsPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_nested(LinksGetRelationshipsPeered(links, direction, rel_types).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsViaLua(std::vector<Link> links) {
      return sol::as_nested(LinksGetNeighborsPeered(links).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsForDirectionViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_nested(LinksGetNeighborsPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_nested(LinksGetNeighborsPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_nested(LinksGetNeighborsPeered(links, direction, rel_types).get0());
    }

    sol::table Shard::LinksGetNeighborIdsViaLua(std::vector<Link> links) {
        sol::table neighbors = lua.create_table(0, links.size());
        for (const auto& [node_id, link_ids] : LinksGetLinksPeered(links).get0() ) {
            sol::table node_ids = lua.create_table(link_ids.size(), 0);
            for (const auto link : link_ids) {
                node_ids.add(link.node_id);
            }
            neighbors.set(node_id, node_ids);
        }
        return neighbors;
    }

    sol::table Shard::LinksGetNeighborIdsViaLua(std::vector<Link> links, Direction direction){
        sol::table neighbors = lua.create_table(0, links.size());
        for (const auto& [node_id, link_ids] : LinksGetLinksPeered(links, direction).get0() ) {
            sol::table node_ids = lua.create_table(link_ids.size(), 0);
            for (const auto link : link_ids) {
                node_ids.add(link.node_id);
            }
            neighbors.set(node_id, node_ids);
        }
        return neighbors;
    }

    sol::table Shard::LinksGetNeighborIdsViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
        sol::table neighbors = lua.create_table(0, links.size());
        for (const auto& [node_id, link_ids] : LinksGetLinksPeered(links, direction, rel_type).get0() ) {
            sol::table node_ids = lua.create_table(link_ids.size(), 0);
            for (const auto link : link_ids) {
                node_ids.add(link.node_id);
            }
            neighbors.set(node_id, node_ids);
        }
        return neighbors;
    }

    sol::table Shard::LinksGetNeighborIdsViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
        sol::table neighbors = lua.create_table(0, links.size());
        for (const auto& [node_id, link_ids] : LinksGetLinksPeered(links, direction, rel_types).get0() ) {
            sol::table node_ids = lua.create_table(link_ids.size(), 0);
            for (const auto link : link_ids) {
                node_ids.add(link.node_id);
            }
            neighbors.set(node_id, node_ids);
        }
        return neighbors;
    }

}
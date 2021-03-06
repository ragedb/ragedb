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
      return sol::as_table(LinksGetLinksPeered(links).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksForDirectionViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_table(LinksGetLinksPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_table(LinksGetLinksPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_table(LinksGetLinksPeered(links, direction, rel_types).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksForTypeViaLua(std::vector<Link> links, const std::string& rel_type) {
      return sol::as_table(LinksGetLinksPeered(links, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksForTypesViaLua(std::vector<Link> links, const std::vector<std::string> &rel_types) {
      return sol::as_table(LinksGetLinksPeered(links, rel_types).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsViaLua(std::vector<Link> links) {
      return sol::as_table(LinksGetRelationshipsPeered(links).get0());
    }
    
    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsForDirectionViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_table(LinksGetRelationshipsPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_table(LinksGetRelationshipsPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_table(LinksGetRelationshipsPeered(links, direction, rel_types).get0());
    }


    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsForTypeViaLua(std::vector<Link> links, const std::string& rel_type) {
      return sol::as_table(LinksGetRelationshipsPeered(links, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsForTypesViaLua(std::vector<Link> links, const std::vector<std::string> &rel_types) {
      return sol::as_table(LinksGetRelationshipsPeered(links, rel_types).get0());
    }


    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsViaLua(std::vector<Link> links) {
      return sol::as_table(LinksGetNeighborsPeered(links).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsForDirectionViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_table(LinksGetNeighborsPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_table(LinksGetNeighborsPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_table(LinksGetNeighborsPeered(links, direction, rel_types).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsForTypeViaLua(std::vector<Link> links, const std::string& rel_type) {
      return sol::as_table(LinksGetNeighborsPeered(links, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsForTypesViaLua(std::vector<Link> links, const std::vector<std::string> &rel_types) {
      return sol::as_table(LinksGetNeighborsPeered(links, rel_types).get0());
    }
    
}
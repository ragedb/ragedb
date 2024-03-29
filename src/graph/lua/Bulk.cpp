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

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_nested(LinksGetLinksPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_nested(LinksGetLinksPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Link>>> Shard::LinksGetLinksViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_nested(LinksGetLinksPeered(links, direction, rel_types).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsViaLua(std::vector<Link> links) {
      return sol::as_nested(LinksGetRelationshipsPeered(links).get0());
    }
    
    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_nested(LinksGetRelationshipsPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_nested(LinksGetRelationshipsPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Relationship>>> Shard::LinksGetRelationshipsViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_nested(LinksGetRelationshipsPeered(links, direction, rel_types).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsViaLua(std::vector<Link> links) {
      return sol::as_nested(LinksGetNeighborsPeered(links).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_nested(LinksGetNeighborsPeered(links, direction).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_nested(LinksGetNeighborsPeered(links, direction, rel_type).get0());
    }

    sol::nested<std::map<Link, std::vector<Node>>> Shard::LinksGetNeighborsViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_nested(LinksGetNeighborsPeered(links, direction, rel_types).get0());
    }

    sol::nested<std::map<Link, std::vector<uint64_t>>> Shard::LinksGetNeighborIdsViaLua(std::vector<Link> links) {
        return LinksGetNodeIdsPeered(links).get0();
    }

    sol::nested<std::map<Link, std::vector<uint64_t>>> Shard::LinksGetNeighborIdsViaLua(std::vector<Link> links, Direction direction){
       return LinksGetNodeIdsPeered(links, direction).get0();
    }

    sol::nested<std::map<Link, std::vector<uint64_t>>> Shard::LinksGetNeighborIdsViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
        return LinksGetNodeIdsPeered(links, direction, rel_type).get0();
    }

    sol::nested<std::map<Link, std::vector<uint64_t>>> Shard::LinksGetNeighborIdsViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
        return LinksGetNodeIdsPeered(links, direction, rel_types).get0();
    }

}
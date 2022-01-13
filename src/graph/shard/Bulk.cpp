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

    std::map<Link, std::vector<Link>> Shard::LinksGetRelationshipsIDs(std::vector<Link> links) {
      std::map<Link, std::vector<Link>> link_rels;
      for (Link link : links) {
        link_rels[link] = NodeGetRelationshipsIDs(link.node_id);
      }
      return link_rels;
    }

    std::map<Link, std::vector<Link>> Shard::LinksGetRelationshipsIDs(std::vector<Link> links, Direction direction) {
      std::map<Link, std::vector<Link>> link_rels;
      for (Link link : links) {
        link_rels[link] = NodeGetRelationshipsIDs(link.node_id, direction);
      }
      return link_rels;
    }

    std::map<Link, std::vector<Link>> Shard::LinksGetRelationshipsIDs(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      std::map<Link, std::vector<Link>> link_rels;
      for (Link link : links) {
        link_rels[link] = NodeGetRelationshipsIDs(link.node_id, direction, rel_type);
      }
      return link_rels;
    }

    std::map<Link, std::vector<Link>> Shard::LinksGetRelationshipsIDs(std::vector<Link> links, Direction direction, uint16_t type_id) {
      std::map<Link, std::vector<Link>> link_rels;
      for (Link link : links) {
        link_rels[link] = NodeGetRelationshipsIDs(link.node_id, direction, type_id);
      }
      return link_rels;
    }

    std::map<Link, std::vector<Link>> Shard::LinksGetRelationshipsIDs(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      std::map<Link, std::vector<Link>> link_rels;
      for (Link link : links) {
        link_rels[link] = NodeGetRelationshipsIDs(link.node_id, direction, rel_types);
      }
      return link_rels;
    }

}
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

    std::vector<Relationship> Shard::RelationshipsGet(const std::vector<Link>& links) {
      std::vector<Relationship> sharded_relationships;

      for(Link link : links) {
        sharded_relationships.emplace_back(RelationshipGet(link.rel_id));
      }

      return sharded_relationships;
    }

    std::vector<Relationship> Shard::RelationshipsGet(const std::vector<uint64_t>& rel_ids) {
        std::vector<Relationship> sharded_relationships;

        for(uint64_t id : rel_ids) {
            sharded_relationships.emplace_back(RelationshipGet(id));
        }

        return sharded_relationships;
    }

    std::map<uint64_t, std::string> Shard::RelationshipsGetType(const std::vector<uint64_t>& rel_ids) {
      std::map<uint64_t, std::string> sharded_relationship_types;

      for(uint64_t id : rel_ids) {
        sharded_relationship_types[id] = RelationshipGetType(id);
      }

      return sharded_relationship_types;
    }

    std::map<Link, std::string> Shard::RelationshipsGetType(const std::vector<Link>& links) {
      std::map<Link, std::string> sharded_relationship_types;

      for(Link link : links) {
        sharded_relationship_types[link] = RelationshipGetType(link.rel_id);
      }

      return sharded_relationship_types;
    }

    std::map<uint64_t, uint16_t> Shard::RelationshipsGetTypeId(const std::vector<uint64_t>& rel_ids) {
      std::map<uint64_t, uint16_t> sharded_relationship_type_ids;

      if (ValidRelationshipIds(rel_ids)) {
          for (auto [type_id, ids] : PartitionRelationshipIdsByTypeId(rel_ids)) {
              for (auto id : ids) {
                  sharded_relationship_type_ids[id] = type_id;
              }
          }
      }

      return sharded_relationship_type_ids;
    }

    std::map<Link, uint16_t> Shard::RelationshipsGetTypeId(const std::vector<Link>& links) {
      std::map<Link, uint16_t> sharded_relationship_type_ids;

      if (ValidLinksRelationshipIds(links)) {
          for (auto [type_id, typed_links] : PartitionLinkRelationshipIdsByTypeId(links)) {
              for (auto link : typed_links) {
                  sharded_relationship_type_ids[link] = type_id;
              }
          }
      }

      return sharded_relationship_type_ids;
    }

    std::map<uint64_t, property_type_t> Shard::RelationshipsGetProperty(const std::vector<uint64_t> &rel_ids, const std::string& property) {
        std::map<uint64_t, property_type_t> sharded_relationship_properties;

        if (ValidRelationshipIds(rel_ids)) {
            for (auto [type_id, ids] : PartitionRelationshipIdsByTypeId(rel_ids)) {
                for (auto [id, properties] : relationship_types.getRelationshipsProperty(type_id, ids, property)) {
                    sharded_relationship_properties[id] = properties;
                }
            }
        }

      return sharded_relationship_properties;
    }

    std::map<Link, property_type_t> Shard::RelationshipsGetProperty(const std::vector<Link>& links, const std::string& property) {
      std::map<Link, property_type_t> sharded_relationship_properties;

      if (ValidLinksRelationshipIds(links)) {
          for (auto [type_id, typed_links] : PartitionLinkRelationshipIdsByTypeId(links)) {

              std::vector<uint64_t> ids(typed_links.size());
              for (int i = 0; i < typed_links.size(); ++i) {
                  ids[i] = typed_links[i].rel_id;
              }

              auto properties = relationship_types.getRelationshipsProperty(type_id, ids, property);
              for(int i = 0; i < ids.size(); i++){
                  sharded_relationship_properties.emplace(typed_links[i], properties[ids[i]]);
              }
          }
      }

      return sharded_relationship_properties;
    }

    std::map<uint64_t, std::map<std::string, property_type_t>> Shard::RelationshipsGetProperties(const std::vector<uint64_t>& rel_ids) {
      std::map<uint64_t, std::map<std::string, property_type_t>> sharded_relationship_properties;

      if (ValidRelationshipIds(rel_ids)) {
          for (auto [type_id, ids] : PartitionRelationshipIdsByTypeId(rel_ids)) {
              for (auto [id, properties] : relationship_types.getRelationshipsProperties(type_id, ids)) {
                  sharded_relationship_properties[id] = properties;
              }
          }
      }

      return sharded_relationship_properties;
    }

    std::map<Link, std::map<std::string, property_type_t>> Shard::RelationshipsGetProperties(const std::vector<Link>& links) {
      std::map<Link, std::map<std::string, property_type_t>> sharded_relationship_properties;

      if (ValidLinksRelationshipIds(links)) {
          for (auto [type_id, typed_links] : PartitionLinkRelationshipIdsByTypeId(links)) {

              std::vector<uint64_t> ids(typed_links.size());
              for (int i = 0; i < typed_links.size(); ++i) {
                  ids[i] = typed_links[i].rel_id;
              }

              auto properties = relationship_types.getRelationshipsProperties(type_id, ids);
              for(int i = 0; i < ids.size(); i++){
                  sharded_relationship_properties.emplace(typed_links[i], properties[ids[i]]);
              }
          }
      }

      return sharded_relationship_properties;
    }

}
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

  uint64_t Shard::FilterNodeCount(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
    uint16_t type_id = node_types.getTypeId(type);
    return node_types.filterCount(ids, type_id, property, operation, value);
  }
  
  uint64_t Shard::FilterRelationshipCount(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value) {
    uint16_t type_id = relationship_types.getTypeId(type);
    return relationship_types.filterCount(ids, type_id, property, operation, value);
  }
  
  std::vector<uint64_t> Shard::FilterNodeIds(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t limit, Sort sort) {
    uint16_t type_id = node_types.getTypeId(type);
    return node_types.filterIds(ids, type_id, property, operation, value, limit, sort);
  }
  
  std::vector<uint64_t> Shard::FilterRelationshipIds(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    uint16_t type_id = relationship_types.getTypeId(type);
    return relationship_types.filterIds(ids, type_id, property, operation, value, skip, limit);
  }
  
  std::vector<Node> Shard::FilterNodes(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t limit, Sort sort) {
    uint16_t type_id = node_types.getTypeId(type);
    return node_types.filterNodes(ids, type_id, property, operation, value, limit, sort);
  }

  std::vector<std::map<std::string, property_type_t>> Shard::FilterNodeProperties(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t limit, Sort sort) {
      uint16_t type_id = node_types.getTypeId(type);
      return node_types.filterNodeProperties(ids, type_id, property, operation, value, limit, sort);
  }

  std::vector<Relationship> Shard::FilterRelationships(const std::vector<uint64_t>& ids, const std::string& type, const std::string& property, const Operation& operation, const property_type_t& value, uint64_t skip, uint64_t limit) {
    uint16_t type_id = relationship_types.getTypeId(type);
    return relationship_types.filterRelationships(ids, type_id, property, operation, value, skip, limit);
  }
}
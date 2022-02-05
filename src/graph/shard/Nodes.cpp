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

#include <utility>

#include "../Shard.h"

namespace ragedb {

      std::vector<Node> Shard::NodesGet(const std::vector<uint64_t>& node_ids) {
        std::vector<Node> sharded_nodes;

        for(uint64_t id : node_ids) {
            sharded_nodes.emplace_back(NodeGet(id));
        }

        return sharded_nodes;
    }

    std::vector<Node> Shard::NodesGet(const std::vector<Link>& links) {
      std::vector<Node> sharded_nodes;

      for(Link link : links) {
        sharded_nodes.emplace_back(NodeGet(link.node_id));
      }

      return sharded_nodes;
    }

    std::vector<std::string> Shard::NodesGetKey(const std::vector<uint64_t>& node_ids) {
      std::vector<std::string> sharded_node_keys;

      for(uint64_t id : node_ids) {
        sharded_node_keys.emplace_back(NodeGetKey(id));
      }

      return sharded_node_keys;
    }

    std::vector<std::string> Shard::NodesGetKey(const std::vector<Link>& links) {
      std::vector<std::string> sharded_node_keys;

      for(Link link : links) {
        sharded_node_keys.emplace_back(NodeGetKey(link.node_id));
      }

      return sharded_node_keys;
    }

    std::vector<std::string> Shard::NodesGetType(const std::vector<uint64_t>& node_ids) {
      std::vector<std::string> sharded_node_types;

      for(uint64_t id : node_ids) {
        sharded_node_types.emplace_back(NodeGetType(id));
      }

      return sharded_node_types;
    }

    std::vector<std::string> Shard::NodesGetType(const std::vector<Link>& links) {
      std::vector<std::string> sharded_node_types;

      for(Link link : links) {
        sharded_node_types.emplace_back(NodeGetType(link.node_id));
      }

      return sharded_node_types;
    }

    std::vector<uint16_t> Shard::NodesGetTypeId(const std::vector<uint64_t>& node_ids) {
      std::vector<uint16_t> sharded_node_type_ids;

      for(uint64_t id : node_ids) {
        sharded_node_type_ids.emplace_back(NodeGetTypeId(id));
      }

      return sharded_node_type_ids;
    }

    std::vector<uint16_t> Shard::NodesGetTypeId(const std::vector<Link>& links) {
      std::vector<uint16_t> sharded_node_type_ids;

      for(Link link : links) {
        sharded_node_type_ids.emplace_back(NodeGetTypeId(link.node_id));
      }

      return sharded_node_type_ids;
    }


    std::vector<property_type_t> Shard::NodesGetProperty(const std::vector<uint64_t> &node_ids, const std::string& property) {
      std::vector<property_type_t> sharded_node_properties;

      for(uint64_t id : node_ids) {
        sharded_node_properties.push_back(NodePropertyGet(id, property));
      }
      
      return sharded_node_properties;
    }

    std::vector<property_type_t> Shard::NodesGetProperty(const std::vector<Link>& links, const std::string& property) {
      std::vector<property_type_t> sharded_node_properties;

      for(Link link : links) {
        sharded_node_properties.emplace_back(NodePropertyGet(link.node_id, property));
      }

      return sharded_node_properties;
    }

    std::vector<std::map<std::string, property_type_t>> Shard::NodesGetProperties(const std::vector<uint64_t> &node_ids) {
      std::vector<std::map<std::string, property_type_t>> sharded_node_properties;

      for(uint64_t id : node_ids) {
        sharded_node_properties.emplace_back(NodePropertiesGet(id));
      }

      return sharded_node_properties;
    }

    std::vector<std::map<std::string, property_type_t>> Shard::NodesGetProperties(const std::vector<Link>& links) {
      std::vector<std::map<std::string, property_type_t>> sharded_node_properties;

      for(Link link : links) {
        sharded_node_properties.emplace_back(NodePropertiesGet(link.node_id));
      }

      return sharded_node_properties;
    }

}

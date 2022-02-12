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

    std::map<uint64_t, std::string> Shard::NodesGetKey(const std::vector<uint64_t>& node_ids) {
      std::map<uint64_t, std::string> sharded_node_keys;

      for(uint64_t id : node_ids) {
        sharded_node_keys[id] = NodeGetKey(id);
      }

      return sharded_node_keys;
    }

    std::map<Link, std::string> Shard::NodesGetKey(const std::vector<Link>& links) {
      std::map<Link, std::string> sharded_node_keys;

      for(Link link : links) {
        sharded_node_keys[link] = NodeGetKey(link.node_id);
      }

      return sharded_node_keys;
    }

    std::map<uint64_t, std::string> Shard::NodesGetType(const std::vector<uint64_t>& node_ids) {
      std::map<uint64_t, std::string> sharded_node_types;

      for(uint64_t id : node_ids) {
        sharded_node_types[id] = NodeGetType(id);
      }

      return sharded_node_types;
    }

    std::map<Link, std::string> Shard::NodesGetType(const std::vector<Link>& links) {
      std::map<Link, std::string> sharded_node_types;

      for(Link link : links) {
        sharded_node_types[link] = NodeGetType(link.node_id);
      }

      return sharded_node_types;
    }

    std::map<uint64_t, uint16_t> Shard::NodesGetTypeId(const std::vector<uint64_t>& node_ids) {
      std::map<uint64_t, uint16_t> sharded_node_type_ids;

      for(uint64_t id : node_ids) {
        sharded_node_type_ids[id] = NodeGetTypeId(id);
      }

      return sharded_node_type_ids;
    }

    std::map<Link, uint16_t> Shard::NodesGetTypeId(const std::vector<Link>& links) {
      std::map<Link, uint16_t> sharded_node_type_ids;

      for(Link link : links) {
        sharded_node_type_ids[link] = NodeGetTypeId(link.node_id);
      }

      return sharded_node_type_ids;
    }


    std::map<uint64_t, property_type_t> Shard::NodesGetProperty(const std::vector<uint64_t> &node_ids, const std::string& property) {
      std::map<uint64_t, property_type_t> sharded_node_properties;

      for(uint64_t id : node_ids) {
        sharded_node_properties[id] = NodeGetProperty(id, property);
      }
      
      return sharded_node_properties;
    }

    std::map<Link, property_type_t> Shard::NodesGetProperty(const std::vector<Link>& links, const std::string& property) {
      std::map<Link, property_type_t> sharded_node_properties;

      for(Link link : links) {
        sharded_node_properties[link] = NodeGetProperty(link.node_id, property);
      }

      return sharded_node_properties;
    }

    std::map<uint64_t, std::map<std::string, property_type_t>> Shard::NodesGetProperties(const std::vector<uint64_t> &node_ids) {
      std::map<uint64_t, std::map<std::string, property_type_t>> sharded_node_properties;

      for(uint64_t id : node_ids) {
        sharded_node_properties[id]  = NodeGetProperties(id);
      }

      return sharded_node_properties;
    }

    std::map<Link, std::map<std::string, property_type_t>> Shard::NodesGetProperties(const std::vector<Link>& links) {
      std::map<Link, std::map<std::string, property_type_t>> sharded_node_properties;

      for(Link link : links) {
        sharded_node_properties[link] = NodeGetProperties(link.node_id);
      }

      return sharded_node_properties;
    }

}

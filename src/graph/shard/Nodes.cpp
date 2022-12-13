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

    seastar::future<std::map<std::string, uint64_t>> Shard::NodesGetIds(uint16_t type_id, const std::vector<std::string>& keys) {
        std::map<std::string, uint64_t> node_ids;
        if (node_types.ValidTypeId(type_id)) {
            for (const auto& key : keys) {
                node_ids.emplace(key, node_types.getNodeId(type_id, key));
                co_await seastar::coroutine::maybe_yield();
            }
        }

        co_return node_ids;
    }

    std::vector<Node> Shard::NodesGet(const std::vector<uint64_t>& node_ids) {
        std::vector<Node> sharded_nodes;

        if (ValidNodeIds(node_ids)) {
            for (auto [type_id, ids] : PartitionNodeIdsByTypeId(node_ids)) {
                auto nodes = node_types.getNodes(type_id, ids);
                sharded_nodes.insert(sharded_nodes.end(), std::begin(nodes), std::end(nodes));
            }
        }

        return sharded_nodes;
    }

    std::vector<Node> Shard::NodesGet(const std::vector<Link>& links) {
      std::vector<Node> sharded_nodes;

      if (ValidLinksNodeIds(links)) {
          for (auto [type_id, typed_links] : PartitionLinkNodeIdsByTypeId(links)) {
              auto nodes = node_types.getNodes(type_id, typed_links);
              sharded_nodes.insert(sharded_nodes.end(), std::begin(nodes), std::end(nodes));
          }
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

    std::map<uint64_t, uint16_t> Shard::NodesGetTypeId(const std::vector<uint64_t>& node_ids) const {
      std::map<uint64_t, uint16_t> sharded_node_type_ids;

      if (ValidNodeIds(node_ids)) {
          for (auto [type_id, ids] : PartitionNodeIdsByTypeId(node_ids)) {
              for (auto id : ids) {
                  sharded_node_type_ids[id] = type_id;
              }
          }
      }

      return sharded_node_type_ids;
    }

    std::map<Link, uint16_t> Shard::NodesGetTypeId(const std::vector<Link>& links) const {
      std::map<Link, uint16_t> sharded_node_type_ids;

      if (ValidLinksNodeIds(links)) {
          for (auto [type_id, typed_links] : PartitionLinkNodeIdsByTypeId(links)) {
              for (auto link : typed_links) {
                  sharded_node_type_ids[link] = type_id;
              }
          }
      }

      return sharded_node_type_ids;
    }


    std::map<uint64_t, property_type_t> Shard::NodesGetProperty(const std::vector<uint64_t> &node_ids, const std::string& property) {
      std::map<uint64_t, property_type_t> sharded_node_properties;

      if (ValidNodeIds(node_ids)) {
          for (auto [type_id, ids] : PartitionNodeIdsByTypeId(node_ids)) {
              for (auto [id, properties] : node_types.getNodesProperty(type_id, ids, property)) {
                  sharded_node_properties[id] = properties;
              }
          }
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

      if (ValidNodeIds(node_ids)) {
          for (auto [type_id, ids] :  PartitionNodeIdsByTypeId(node_ids)) {
              for (auto [id, properties] : node_types.getNodesProperties(type_id, ids)) {
                  sharded_node_properties[id] = properties;
              }
          }
      }

      return sharded_node_properties;
    }

    std::map<Link, std::map<std::string, property_type_t>> Shard::NodesGetProperties(const std::vector<Link>& links) {
      std::map<Link, std::map<std::string, property_type_t>> sharded_node_properties;

      std::vector<uint64_t> node_ids(links.size());
      for (int i = 0; i < links.size(); ++i) {
          node_ids[i] = links[i].node_id;
      }

      // If the nodes are valid
      if (ValidNodeIds(node_ids)) {
          for (auto [type_id, typed_links] : PartitionLinkNodeIdsByTypeId(links)) {
              std::vector<uint64_t> ids(typed_links.size());
              for (int i = 0; i < typed_links.size(); ++i) {
                  ids[i] = typed_links[i].node_id;
              }

              auto properties = node_types.getNodesProperties(type_id, ids);
              for(int i = 0; i < ids.size(); i++){
                  sharded_node_properties.emplace(typed_links[i], properties[ids[i]]);
              }
          }
      }

      return sharded_node_properties;
    }

}

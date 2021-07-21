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

    uint64_t Shard::NodeAddEmpty(uint16_t type_id, const std::string &key) {
        uint64_t internal_id = node_types.getCount(type_id);
        uint64_t external_id = 0;

        // Check if the key exists
        auto key_search = node_types.getKeysToNodeId(type_id).find(key);
        if (key_search == std::end(node_types.getKeysToNodeId(type_id))) {
            // If we have deleted nodes, fill in the space by adding the new node here
            if (node_types.hasDeleted(type_id)) {
                internal_id = node_types.getDeletedIdsMinimum(type_id);
                external_id = internalToExternal(type_id, internal_id);
                // Replace the deleted node and remove it from the list
                node_types.getKeys(type_id).at(internal_id) = key;
                node_types.addId(type_id, internal_id);
            } else {
                external_id = internalToExternal(type_id, internal_id);
                // Set Metadata properties
                // Add the node to the end and prepare a place for its relationships
                node_types.getKeys(type_id).emplace_back(key);
                node_types.getOutgoingRelationships(type_id).emplace_back();
                node_types.getIncomingRelationships(type_id).emplace_back();
                node_types.addId(type_id, internal_id);
            }
            node_types.getKeysToNodeId(type_id).insert({key, external_id });
        }

        return external_id;
    }

    uint64_t Shard::NodeAdd(uint16_t type_id, const std::string &key, const std::string &properties) {
        uint64_t internal_id = node_types.getCount(type_id);
        uint64_t external_id = 0;

        // Check if the key exists
        auto key_search = node_types.getKeysToNodeId(type_id).find(key);
        if (key_search == std::end(node_types.getKeysToNodeId(type_id))) {
            // If we have deleted nodes, fill in the space by adding the new node here
            if (node_types.hasDeleted(type_id)) {
                internal_id = node_types.getDeletedIdsMinimum(type_id);
                external_id = internalToExternal(type_id, internal_id);

                // Replace the deleted node and remove it from the list
                node_types.getKeys(type_id).at(internal_id) = key;
                node_types.addId(type_id, internal_id);
                node_types.setPropertiesFromJSON(type_id, internal_id, properties);
            } else {
                external_id = internalToExternal(type_id, internal_id);
                // Add the node to the end and prepare a place for its relationships
                node_types.getKeys(type_id).emplace_back(key);
                node_types.getOutgoingRelationships(type_id).emplace_back();
                node_types.getIncomingRelationships(type_id).emplace_back();
                node_types.addId(type_id, internal_id);
                node_types.setPropertiesFromJSON(type_id, internal_id, properties);
            }
            node_types.getKeysToNodeId(type_id).insert({key, external_id });
        }

        return external_id;
    }

    uint64_t Shard::NodeGetID(const std::string &type, const std::string &key) {
        // Check if the Type exists
        uint16_t type_id = node_types.getTypeId(type);
        if (type_id > 0) {
            return node_types.getNodeId(type_id, key);
        }
        // Invalid Type or Key
        return 0;
    }

    std::vector<Node> Shard::NodesGet(const std::vector<uint64_t>& node_ids) {
        std::vector<Node> sharded_nodes;

        for(uint64_t id : node_ids) {
            sharded_nodes.push_back(NodeGet(id));
        }

        return sharded_nodes;
    }

    Node Shard::NodeGet(uint64_t id) {
        if (ValidNodeId(id)) {
            return node_types.getNode(id);
        }
        return Node();
    }

    Node Shard::NodeGet(const std::string& type, const std::string& key) {
        return NodeGet(NodeGetID(type, key));
    }

    uint16_t Shard::NodeGetTypeId(uint64_t id) {
        return externalToTypeId(id);
    }

    std::string Shard::NodeGetType(uint64_t id) {
        if (ValidNodeId(id)) {
            return node_types.getType(externalToTypeId(id));
        }
        return node_types.getType(0);
    }

    std::string Shard::NodeGetKey(uint64_t id) {
        if (ValidNodeId(id)) {
            return node_types.getKeys(externalToTypeId(id))[externalToInternal(id)];
        }
        return node_types.getType(0);
    }

    std::any Shard::NodePropertyGet(uint64_t id, const std::string& property) {
        if (ValidNodeId(id)) {
            node_types.getNodeProperty(id, property);
        }
        return std::any();
    }

    bool Shard::NodePropertySetFromJson(uint64_t id, const std::string& property, const std::string& value) {
        if (ValidNodeId(id)) {
            return node_types.setNodeProperty(id, property, value);
        }
        return false;
    }

    std::any Shard::NodePropertyGet(const std::string& type, const std::string& key, const std::string& property) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertyGet(id, property);
    }

    bool Shard::NodePropertySetFromJson(const std::string& type, const std::string& key, const std::string& property, const std::string& value) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertySetFromJson(id, property, value);
    }

    bool Shard::NodePropertyDelete(const std::string& type, const std::string& key, const std::string& property) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertyDelete(id, property);
    }

    bool Shard::NodePropertyDelete(uint64_t id, const std::string& property) {
        if (ValidNodeId(id)) {
            return node_types.deleteNodeProperty(id, property);
        }
        return false;
    }

    std::map<std::string, std::any> Shard::NodePropertiesGet(const std::string &type, const std::string &key) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertiesGet(id);
    }

    std::map<std::string, std::any> Shard::NodePropertiesGet(uint64_t id) {
        // If the node is valid
        if (ValidNodeId(id)) {
            return node_types.getNodeProperties(externalToTypeId(id), externalToInternal(id));
        }
        return std::map<std::string, std::any>();
    }

    bool Shard::NodePropertiesSetFromJson(const std::string& type, const std::string& key, const std::string& value) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertiesSetFromJson(id, value);
    }

    bool Shard::NodePropertiesSetFromJson(uint64_t id, const std::string& value) {
        // If the node is valid
        if (ValidNodeId(id)) {
            return node_types.setPropertiesFromJSON(externalToTypeId(id), externalToInternal(id), value);
        }
        return false;
    }

    bool Shard::NodePropertiesResetFromJson(const std::string& type, const std::string& key, const std::string& value) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertiesResetFromJson(id, value);
    }

    bool Shard::NodePropertiesResetFromJson(uint64_t id, const std::string& value) {
        // If the node is valid
        if (ValidNodeId(id)) {
            node_types.deleteProperties(externalToTypeId(id), externalToInternal(id));
            return node_types.setPropertiesFromJSON(externalToTypeId(id), externalToInternal(id), value);
        }
        return false;
    }

    bool Shard::NodePropertiesDelete(const std::string& type, const std::string& key) {
        uint64_t id = NodeGetID(type, key);
        return NodePropertiesDelete(id);
    }

    bool Shard::NodePropertiesDelete(uint64_t id) {
        // If the node is valid
        if (ValidNodeId(id)) {
            return node_types.deleteProperties(externalToTypeId(id), externalToInternal(id));
        }
        return false;
    }

}

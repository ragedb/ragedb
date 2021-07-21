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

    seastar::future<uint64_t> Shard::NodeAddEmptyPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);
        uint16_t type_id = node_types.getTypeId(type);

        // The node type exists, so continue on
        if (type_id > 0) {
            return container().invoke_on(node_shard_id, [type_id, key](Shard &local_shard) {
                return local_shard.NodeAddEmpty(type_id, key);
            });
        }

        // The node type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [node_shard_id, type, key, this] (Shard &local_shard) {
            return local_shard.NodeTypeInsertPeered(type).then([node_shard_id, type, key, this] (uint16_t node_type_id) {
                return container().invoke_on(node_shard_id, [node_type_id, key](Shard &local_shard) {
                    return local_shard.NodeAddEmpty(node_type_id, key);
                });
            });
        });
    }

    seastar::future<uint64_t> Shard::NodeAddPeered(const std::string &type, const std::string &key, const std::string &properties) {
        uint16_t node_shard_id = CalculateShardId(type, key);
        uint16_t node_type_id = node_types.getTypeId(type);

        // The node type exists, so continue on
        if (node_type_id > 0) {
            return container().invoke_on(node_shard_id, [node_type_id, key, properties](Shard &local_shard) {
                return local_shard.NodeAdd(node_type_id, key, properties);
            });
        }

        // The node type needs to be set by Shard 0 and propagated
        return container().invoke_on(0, [node_shard_id, type, key, properties, this](Shard &local_shard) {
            return local_shard.NodeTypeInsertPeered(type).then([node_shard_id, key, properties, this](uint16_t node_type_id) {
                return container().invoke_on(node_shard_id, [node_type_id, key, properties](Shard &local_shard) {
                    return local_shard.NodeAdd(node_type_id, key, properties);
                });
            });
        });
    }

    seastar::future<uint64_t> Shard::NodeGetIDPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key] (Shard &local_shard) {
            return local_shard.NodeGetID(type, key);
        });
    }

    seastar::future<Node> Shard::NodeGetPeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodeGet(type, key);
        });
    }

    seastar::future<Node> Shard::NodeGetPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGet(id);
        });
    }

    seastar::future<bool> Shard::NodeRemovePeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key] (Shard &local_shard) {
            return local_shard.NodeGetID(type, key);
        }).then([this] (uint64_t external_id) {
            return NodeRemovePeered(external_id);
        });
    }

    seastar::future<bool> Shard::NodeRemovePeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);
        //TODO
        return seastar::make_ready_future<bool>(false);
    }

    seastar::future<uint16_t> Shard::NodeGetTypeIdPeered(uint64_t id) {
        return seastar::make_ready_future<uint16_t>(NodeGetTypeId(id));
    }

    seastar::future<std::string> Shard::NodeGetTypePeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGetType(id);
        });
    }

    seastar::future<std::string> Shard::NodeGetKeyPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodeGetKey(id);
        });
    }

    seastar::future<std::any> Shard::NodePropertyGetPeered(const std::string &type, const std::string &key, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
            return local_shard.NodePropertyGet(type, key, property);
        });
    }

    seastar::future<std::any> Shard::NodePropertyGetPeered(uint64_t id, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
            return local_shard.NodePropertyGet(id, property);
        });
    }

    seastar::future<bool> Shard::NodePropertySetFromJsonPeered(const std::string &type, const std::string &key, const std::string &property, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
            return local_shard.NodePropertySetFromJson(type, key, property, value);
        });
    }

    seastar::future<bool> Shard::NodePropertySetFromJsonPeered(uint64_t id, const std::string &property, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
            return local_shard.NodePropertySetFromJson(id, property, value);
        });
    }

    seastar::future<bool> Shard::NodePropertyDeletePeered(const std::string &type, const std::string &key, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
            return local_shard.NodePropertyDelete(type, key, property);
        });
    }

    seastar::future<bool> Shard::NodePropertyDeletePeered(uint64_t id, const std::string &property) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
            return local_shard.NodePropertyDelete(id, property);
        });
    }

    seastar::future<std::map<std::string, std::any>> Shard::NodePropertiesGetPeered(const std::string& type, const std::string& key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodePropertiesGet(type, key);
        });
    }

    seastar::future<std::map<std::string, std::any>> Shard::NodePropertiesGetPeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodePropertiesGet(id);
        });
    }

    seastar::future<bool> Shard::NodePropertiesSetFromJsonPeered(const std::string &type, const std::string &key, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) {
            return local_shard.NodePropertiesSetFromJson(type, key, value);
        });
    }

    seastar::future<bool> Shard::NodePropertiesSetFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
            return local_shard.NodePropertiesSetFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::NodePropertiesResetFromJsonPeered(const std::string &type, const std::string &key, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) {
            return local_shard.NodePropertiesResetFromJson(type, key, value);
        });
    }

    seastar::future<bool> Shard::NodePropertiesResetFromJsonPeered(uint64_t id, const std::string &value) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
            return local_shard.NodePropertiesResetFromJson(id, value);
        });
    }

    seastar::future<bool> Shard::NodePropertiesDeletePeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodePropertiesDelete(type, key);
        });
    }

    seastar::future<bool> Shard::NodePropertiesDeletePeered(uint64_t id) {
        uint16_t node_shard_id = CalculateShardId(id);

        return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
            return local_shard.NodePropertiesDelete(id);
        });
    }

}
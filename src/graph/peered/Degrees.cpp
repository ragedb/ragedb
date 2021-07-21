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

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
            return local_shard.NodeGetDegree(type, key);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction](Shard &local_shard) {
            return local_shard.NodeGetDegree(type, key, direction);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, rel_type](Shard &local_shard) {
            return local_shard.NodeGetDegree(type, key, direction, rel_type);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, rel_type](Shard &local_shard) {
            return local_shard.NodeGetDegree(type, key, BOTH, rel_type);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, direction, rel_types](Shard &local_shard) {
            return local_shard.NodeGetDegree(type, key, direction, rel_types);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(type, key);

        return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
            return local_shard.NodeGetDegree(type, key, BOTH, rel_types);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
            return local_shard.NodeGetDegree(external_id);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, Direction direction) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction](Shard &local_shard) {
            return local_shard.NodeGetDegree(external_id, direction);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, Direction direction, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction, rel_type](Shard &local_shard) {
            return local_shard.NodeGetDegree(external_id, direction, rel_type);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, const std::string &rel_type) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, rel_type](Shard &local_shard) {
            return local_shard.NodeGetDegree(external_id, BOTH, rel_type);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, direction, rel_types](Shard &local_shard) {
            return local_shard.NodeGetDegree(external_id, direction, rel_types);
        });
    }

    seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
        uint16_t node_shard_id = CalculateShardId(external_id);

        return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
            return local_shard.NodeGetDegree(external_id, BOTH, rel_types);
        });
    }

}
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

    uint64_t Shard::NodeGetDegreeViaLua(const std::string& type, const std::string& key) {
        return NodeGetDegreePeered(type, key).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(const std::string& type, const std::string& key, Direction direction) {
        return NodeGetDegreePeered(type, key, direction).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return NodeGetDegreePeered(type, key, direction, rel_type).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
        return NodeGetDegreePeered(type, key, rel_type).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(const std::string& type, const std::string& key, Direction direction,
                                                            const std::vector<std::string>& rel_types) {
        return NodeGetDegreePeered(type, key, direction, rel_types).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(const std::string& type, const std::string& key,
                                                const std::vector<std::string>& rel_types) {
        return NodeGetDegreePeered(type, key, rel_types).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(uint64_t id) {
        return NodeGetDegreePeered(id).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(uint64_t id, Direction direction) {
        return NodeGetDegreePeered(id, direction).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return NodeGetDegreePeered(id, direction, rel_type).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(uint64_t id, const std::string& rel_type) {
        return NodeGetDegreePeered(id, rel_type).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return NodeGetDegreePeered(id, direction, rel_types).get0();
    }

    uint64_t Shard::NodeGetDegreeViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
        return NodeGetDegreePeered(id, rel_types).get0();
    }

}

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
    seastar::future<uint64_t> Shard::FindNodeCountPeered(const std::string& type, const std::string& property, Operation operation, std::any value) {
        uint16_t type_id = node_types.getTypeId(type);
        return FindNodeCountPeered(type_id, property, operation, value);
    }

    seastar::future<uint64_t> Shard::FindNodeCountPeered(uint16_t type_id, const std::string& property, Operation operation, std::any value) {
        seastar::future<std::vector<uint64_t>> v = container().map([type_id, property, operation, value] (Shard &local) {
            return local.FindNodeCount(type_id, property, operation, value);
        });

        return v.then([] (std::vector<uint64_t> counts) {
            return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
        });
    }

    seastar::future<std::vector<uint64_t>> Shard::FindNodeIdsPeered(const std::string& type, const std::string& property, Operation operation, std::any value) {
        uint16_t type_id = node_types.getTypeId(type);
        return FindNodeIdsPeered(type_id, property, operation, value);
    }

    seastar::future<std::vector<uint64_t>> Shard::FindNodeIdsPeered(uint16_t type_id, const std::string& property, Operation operation, std::any value) {
        seastar::future<std::vector<std::vector<uint64_t>>> v = container().map([type_id, property, operation, value] (Shard &local) {
            return local.FindNodeIds(type_id, property, operation, value);
        });

        return v.then([] (std::vector<std::vector<uint64_t>> ids) {
            return accumulate(std::begin(ids), std::end(ids), std::vector<uint64_t>(),
                              [](std::vector<uint64_t> a, std::vector<uint64_t> b) {
                a.insert(a.end(), b.begin(), b.end());
                return a;
            });
        });
    }

}
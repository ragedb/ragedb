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

#include <ranges>
#include <functional>
#include "Group.h"

namespace ragedb {
    Group::Group(uint16_t rel_type_id, std::unordered_multimap<uint64_t, uint64_t> ids) : rel_type_id(rel_type_id), link_map(std::move(ids)) {}

    size_t Group::size() {
        return link_map.size();
    }

    std::vector<Link> Group::links() const {
        std::vector<Link> ids;
        ids.reserve(link_map.size());
        std::transform(begin(link_map), end(link_map), back_inserter(ids), [] (const std::pair<uint64_t, uint64_t>& pair) { return  Link(pair.first, pair.second); });
        return ids;
    }

    std::vector<uint64_t> Group::unique_node_ids() const {
        std::vector<uint64_t> ids;
        ids.reserve(link_map.size());
        for (auto it = link_map.cbegin(), end = link_map.cend(); it != end; it = link_map.equal_range(it->first).second) {
            ids.emplace_back(it->first);
        }

        std::transform(begin(link_map), end(link_map), back_inserter(ids), [] (const std::pair<uint64_t, uint64_t>& pair) { return pair.first; });
        return ids;
    }

    std::vector<uint64_t> Group::node_ids() const {
        std::vector<uint64_t> ids;
        ids.reserve(link_map.size());
        std::transform(begin(link_map), end(link_map), back_inserter(ids), [] (const std::pair<uint64_t, uint64_t>& pair) { return pair.first; });
        return ids;
    }

    std::vector<uint64_t> Group::rel_ids() const{
        std::vector<uint64_t> ids;
        ids.reserve(link_map.size());
        std::transform(begin(link_map), end(link_map), back_inserter(ids), [] (const std::pair<uint64_t, uint64_t>& pair) { return pair.second; });
        return ids;
    }
}
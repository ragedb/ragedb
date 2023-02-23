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
    Group::Group(uint16_t rel_type_id, std::vector<Link> ids) : rel_type_id(rel_type_id), links(std::move(ids)) {}

    std::vector<uint64_t> Group::node_ids() const {
        std::vector<uint64_t> ids;
        ids.reserve(links.size());
        std::transform(begin(links), end(links), back_inserter(ids), std::mem_fn(&Link::node_id));
        return ids;
    }

    std::vector<uint64_t> Group::rel_ids() const{
        std::vector<uint64_t> ids;
        ids.reserve(links.size());
        std::transform(begin(links), end(links), back_inserter(ids), std::mem_fn(&Link::rel_id));
        return ids;
    }
}
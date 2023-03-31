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

#ifndef RAGEDB_GROUP_H
#define RAGEDB_GROUP_H

#include <vector>
#include <cstdint>
#include <boost/unordered/unordered_map.hpp>
#include "Link.h"

namespace ragedb {

    class Group {
    public:
        Group(uint16_t rel_type_id, boost::unordered_multimap<uint64_t, uint64_t> links);
        uint16_t rel_type_id;
        boost::unordered_multimap<uint64_t, uint64_t> link_map;
        size_t size();
        std::vector<Link> links() const;
        std::vector<uint64_t> unique_node_ids() const;
        std::vector<uint64_t> node_ids() const;
        std::vector<uint64_t> rel_ids() const;
    };
}

#endif //RAGEDB_GROUP_H

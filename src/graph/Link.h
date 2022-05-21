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

#ifndef RAGEDB_LINK_H
#define RAGEDB_LINK_H

#include <cstdint>
#include <compare>
#include <ostream>
#include <tuple>

namespace ragedb {
    class Link {

    public:
        Link(uint64_t nodeId, uint64_t relId);
        uint64_t node_id;
        uint64_t rel_id;

        constexpr auto to_tuple() const noexcept {
            return std::tie(node_id, rel_id);
        }

        [[nodiscard]] uint64_t getNodeId() const;

        [[nodiscard]] uint64_t getRelationshipId() const;

        auto operator<=>(const Link &) const = default;

        friend std::ostream& operator<<(std::ostream& os, const Link& ids);
    };
}


#endif //RAGEDB_LINK_H

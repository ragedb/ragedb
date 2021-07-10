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

#include "Link.h"

namespace ragedb {

    Link::Link(uint64_t nodeId, uint64_t relId) : node_id(nodeId), rel_id(relId) {}

    std::ostream &operator<<(std::ostream &os, const Link &link) {
        os << "{ node_id: " << link.node_id << ", rel_id: " << link.rel_id << " } ";
        return os;
    }
}
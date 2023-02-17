/*
 * Copyright Max De Marzi. All Rights Reserved.
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

#include "Path.h"

namespace ragedb {

    Path::Path(Node start_node) {
        nodes.emplace_back(start_node);
    }
    Path::Path(std::vector<Node> nodes) : nodes(nodes) {}
    Path::Path(std::vector<Node> nodes, std::vector<Relationship> relationships) : nodes(nodes), relationships(relationships) {}

    Node Path::GetEndNode() {
        return nodes.back();
    }

    Node Path::GetStartNode() {
        return nodes.front();
    }

    std::vector<Node> Path::GetNodes() {
        return nodes;
    }

    Relationship Path::GetLastRelationship() {
        return relationships.back();
    }

    std::vector<Relationship> Path::GetRelationships() {
        return relationships;
    }

    int Path::length() {
        return relationships.size();
    }
}
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

    // Constructs a path containing just the start node (0 relationships).
    Path::Path(Node start_node) {
        nodes.emplace_back(start_node);
    }
    // Constructs a path from a list of nodes.
    Path::Path(std::vector<Node> nodes) : nodes(nodes) {}
    // Constructs a path from lists of nodes and relationships.
    Path::Path(std::vector<Node> nodes, std::vector<Relationship> relationships) : nodes(nodes), relationships(relationships) {}

    // Returns the final node in the path sequence.
    Node Path::GetEndNode() const {
        return nodes.back();
    }

    // Returns the initial starting node of the path.
    Node Path::GetStartNode() const {
        return nodes.front();
    }

    // Returns all nodes in traversal order.
    std::vector<Node> Path::GetNodes() const {
        return nodes;
    }

    // Returns the last traversed relationship in the path sequence.
    Relationship Path::GetLastRelationship() const {
        return relationships.back();
    }

    // Returns all relationships in traversal order.
    std::vector<Relationship> Path::GetRelationships() const {
        return relationships;
    }

    // Returns the path length (the number of relationships).
    int Path::length() const {
        return relationships.size();
    }
}
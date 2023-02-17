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

#include "WeightedPath.h"

namespace ragedb {

    WeightedPath::WeightedPath(Node start_node) : path_weight(0.0) {
        nodes.emplace_back(start_node);
    }
    WeightedPath::WeightedPath(std::vector<Node> nodes): nodes(nodes), path_weight(0.0) {}
    WeightedPath::WeightedPath(std::vector<Node> nodes, std::vector<Relationship> relationships, double weight) : nodes(nodes), relationships(relationships), path_weight(weight) {}

    Node WeightedPath::GetEndNode() {
        return nodes.back();
    }
    Node WeightedPath::GetStartNode() {
        return nodes.front();
    }
    std::vector<Node> WeightedPath::GetNodes() {
        return nodes;
    }
    Relationship WeightedPath::GetLastRelationship() {
        return relationships.back();
    }
    std::vector<Relationship> WeightedPath::GetRelationships() {
        return relationships;
    }
    int WeightedPath::length() {
        return relationships.size();
    }
    double WeightedPath::weight() {
        return path_weight;
    }
}
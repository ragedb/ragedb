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

#ifndef RAGEDB_PATH_H
#define RAGEDB_PATH_H

#include "../Node.h"
#include "../Relationship.h"
namespace ragedb {
    /**
     * @brief Enum representing the kind of shortest path selection algorithm to run.
     */
    enum class ShortestPathKind {
        NONE,    ///< Default traversal. Not running shortest path selection.
        ALL,     ///< Returns all shortest paths of minimal length.
        ANY,     ///< Returns any single shortest path of minimal length.
        K,       ///< Returns up to k overall shortest paths.
        K_GROUP  ///< Returns all shortest paths within up to k depth groups/lengths.
    };

    /**
     * @brief Represents a traversed path consisting of nodes and connecting relationships.
     */
    class Path {
      private:
        std::vector<Node> nodes;                    ///< The ordered list of nodes in the path.
        std::vector<Relationship> relationships;    ///< The ordered list of relationships connecting the nodes.

      public:
        /**
         * @brief Constructs a zero-length path containing only a starting node.
         */
        Path(Node start_node);

        /**
         * @brief Constructs a path from a list of nodes.
         */
        Path(std::vector<Node> nodes);

        /**
         * @brief Constructs a path from lists of nodes and relationships.
         */
        Path(std::vector<Node> nodes, std::vector<Relationship> relationships);

        /**
         * @brief Gets the last node in the path.
         */
        Node GetEndNode() const;

        /**
         * @brief Gets the first node in the path.
         */
        Node GetStartNode() const;

        /**
         * @brief Gets all nodes in the path in traversal order.
         */
        std::vector<Node> GetNodes() const;

        /**
         * @brief Gets the last relationship traversed in the path.
         */
        Relationship GetLastRelationship() const;

        /**
         * @brief Gets all relationships in the path in traversal order.
         */
        std::vector<Relationship> GetRelationships() const;

        /**
         * @brief Returns the length of the path (number of relationships).
         */
        int length() const;

    };
}

#endif// RAGEDB_PATH_H

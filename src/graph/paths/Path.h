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
    class Path {
      private:
        std::vector<Node> nodes;
        std::vector<Relationship> relationships;

      public:
        Path(Node start_node);
        Path(std::vector<Node> nodes);
        Path(std::vector<Node> nodes, std::vector<Relationship> relationships);

        Node GetEndNode();
        Node GetStartNode();
        std::vector<Node> GetNodes();
        Relationship GetLastRelationship();
        std::vector<Relationship> GetRelationships();
        int length();

    };
}

#endif// RAGEDB_PATH_H

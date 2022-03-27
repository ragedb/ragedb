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

#ifndef RAGEDB_DATABASE_H
#define RAGEDB_DATABASE_H


#include "Graph.h"
#include "handlers/HealthCheck.h"
#include "handlers/Schema.h"
#include "handlers/Nodes.h"
#include "handlers/Relationships.h"
#include "handlers/NodeProperties.h"
#include "handlers/RelationshipProperties.h"
#include "handlers/Degrees.h"
#include "handlers/Neighbors.h"
#include "handlers/Connected.h"
#include "handlers/Lua.h"
#include "handlers/Restore.h"

class Database {
public:
  explicit Database(std::string name): graph(name), healthCheck(graph), schema(graph), nodes(graph), relationships(graph), nodeProperties(graph), relationshipProperties(graph),
                                        degrees(graph), neighbors(graph), connected(graph), lua(graph), restore(graph) {
    graph.Start();
  }
  ragedb::Graph graph;
  HealthCheck healthCheck;
  Schema schema;
  Nodes nodes;
  Relationships relationships;
  NodeProperties nodeProperties;
  RelationshipProperties relationshipProperties;
  Degrees degrees;
  Neighbors neighbors;
  Connected connected;
  Lua lua;
  Restore restore;
};


#endif// RAGEDB_DATABASE_H

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

#ifndef RAGEDB_SANDBOX_H
#define RAGEDB_SANDBOX_H

#include <sol/sol.hpp>

namespace ragedb {

  class Sandbox {
    sol::state &lua;
    sol::environment env;

  public:
    explicit Sandbox(sol::state &lua) : lua(lua) { buildEnvironment(); }

    sol::environment &getEnvironment() { return env; }

  private:
    void buildEnvironment();

    inline static const std::vector<std::string> ALLOWED_LUA_LIBRARIES = {
      "string",
      "table",
      "math"
    };

    inline static const std::vector<std::string> RESTRICTED_LUA_FUNCTIONS = {
      "loadstring",
      "loadfile",
      "dofile"
    };

    // http://lua-users.org/wiki/SandBoxes
    inline static const std::vector<std::string> ALLOWED_CUSTOM_FUNCTIONS = {
      "json",
      "date",
      "ftcsv"
    };


    inline static const std::vector<std::string> ALLOWED_GRAPH_OBJECTS = {
      "Direction",
      "Link",
      "Node",
      "Operation",
      "Relationship",
      "Roar"
    };

    inline static const std::vector<std::string> ALLOWED_GRAPH_FUNCTIONS = {
      "AllNodes",
      "AllNodeIds",
      "AllRelationships",
      "AllRelationshipIds",

      "FilterNodeCount",
      "FilterNodeIds",
      "FilterNodes",
      "FilterRelationshipCount",
      "FilterRelationshipIds",
      "FilterRelationships",

      "FindNodeCount",
      "FindNodeIds",
      "FindNodes",
      "FindRelationshipCount",
      "FindRelationshipIds",
      "FindRelationships",

      "LinksGetLinks",
      "LinksGetNeighbors",
      "LinksGetRelationships",

      "NodeAdd",
      "NodeDeleteProperties",
      "NodeDeleteProperty",
      "NodeGet",
      "NodeGetDegree",
      "NodeGetId",
      "NodeGetKey",
      "NodeGetLinks",
      "NodeGetNeighbors",
      "NodeGetProperties",
      "NodeGetProperty",
      "NodeGetRelationships",
      "NodeGetType",
      "NodePropertyTypeAdd",
      "NodePropertyTypeDelete",
      "NodeRemove",
      "NodeResetPropertiesFromJson",
      "NodeSetPropertiesFromJson",
      "NodeSetProperty",
      "NodeSetPropertyFromJson",
      "NodeTypeGet",
      "NodeTypeGetType",
      "NodeTypeInsert",
      "NodeTypesGet",
      "NodeTypesGetCount",

      "NodesGet",
      "NodesGetKey",

      "NodesGetProperties",
      "NodesGetProperty",
      "NodesGetType",

      "RelationshipAdd",
      "RelationshipDeleteProperties",
      "RelationshipDeleteProperty",
      "RelationshipGet",
      "RelationshipGetEndingNodeId",
      "RelationshipGetProperties",
      "RelationshipGetProperty",
      "RelationshipGetStartingNodeId",
      "RelationshipGetType",
      "RelationshipPropertyTypeAdd",
      "RelationshipPropertyTypeDelete",
      "RelationshipRemove",
      "RelationshipResetPropertiesFromJson",
      "RelationshipSetPropertiesFromJson",
      "RelationshipSetProperty",
      "RelationshipSetPropertyFromJson"
      "RelationshipTypeGet",
      "RelationshipTypeGetType",
      "RelationshipTypeInsert",
      "RelationshipTypesGet",
      "RelationshipTypesGetCount",

      "RelationshipsGet",
      "RelationshipsGetProperties",
      "RelationshipsGetProperty",
      "RelationshipsGetType",
    };

    inline static const std::vector<std::string> ALLOWED_LUA_FUNCTIONS = {
      // A sort of "hard" sandbox preset
      "assert",
      "error",
      "ipairs",
      "next",
      "pairs",
      "pcall",
      "print",
      "select",
      "tonumber",
      "tostring",
      "type",
      "unpack",
      "_VERSION",
      "xpcall",

      // A softer sandbox preset
      // These functions are unsafe as they can bypass or change metatables,
      // but they are required to implement classes.
      //"rawequal",
      //"rawget",
      "rawset", // Used by ftcsv
      //"setmetatable"
    };

  };

}


#endif// RAGEDB_SANDBOX_H

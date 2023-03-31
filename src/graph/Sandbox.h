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
#include "Permission.h"

namespace ragedb {

  class Sandbox {
    sol::state &lua;
    sol::environment env;

  public:
    Sandbox(sol::state &lua, Permission permission) : lua(lua) { buildEnvironment(permission); }

    sol::environment &getEnvironment() { return env; }

  private:
    void buildEnvironment(Permission permission);

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
      "ftcsv",
      "template",
      "html"
    };


    inline static const std::vector<std::string> ALLOWED_GRAPH_OBJECTS = {
      "Date",
      "Direction",
      "Link",
      "Node",
      "Operation",
      "Relationship",
      "Roar",
      "Sort"
    };


    inline static const std::vector<std::string> ALLOWED_GRAPH_ADMIN_FUNCTIONS = {
      "NodePropertyTypeAdd",
      "NodePropertyTypeDelete",
      "NodeTypeInsert",

      "RelationshipPropertyTypeAdd",
      "RelationshipPropertyTypeDelete",
      "RelationshipTypeInsert",
    };

    inline static const std::vector<std::string> ALLOWED_GRAPH_WRITE_FUNCTIONS = {
      "LoadCSV",

      "NodeAdd",
      "NodeDeleteProperties",
      "NodeDeleteProperty",
      "NodeRemove",
      "NodeResetPropertiesFromJson",
      "NodeSetPropertiesFromJson",
      "NodeSetProperty",
      "NodeSetPropertyFromJson",

      "RelationshipAdd",
      "RelationshipDeleteProperties",
      "RelationshipDeleteProperty",
      "RelationshipRemove",
      "RelationshipResetPropertiesFromJson",
      "RelationshipSetPropertiesFromJson",
      "RelationshipSetProperty",
      "RelationshipSetPropertyFromJson",

      "StreamCSV"
    };

    inline static const std::vector<std::string> ALLOWED_GRAPH_READ_FUNCTIONS = {
      "AllNodes",
      "AllNodesCount",
      "AllNodesCounts",
      "AllNodeIds",
      "AllRelationships",
      "AllRelationshipsCount",
      "AllRelationshipsCounts",
      "AllRelationshipIds",

      "DateToISO",
      "DateToDouble",

      "DifferenceIds",
      "DifferenceNodes",
      "DifferenceRelationships",
      "DifferenceIdsCount",

      "FilterNodeCount",
      "FilterNodeIds",
      "FilterNodes",
      "FilterNodeProperties",
      "FilterRelationshipCount",
      "FilterRelationshipIds",
      "FilterRelationships",
      "FilterRelationshipProperties",

      "FindNodeCount",
      "FindNodeIds",
      "FindNodes",
      "FindRelationshipCount",
      "FindRelationshipIds",
      "FindRelationships",

      "IntersectUnsortedIds",
      "IntersectNodes",
      "IntersectRelationships",
      "IntersectIdsCount",

      "Invert",

      "KHopCount",
      "KHopIds",
      "KHopNodes",

      "LinksGetLinks",
      "LinksGetNeighbors",
      "LinksGetNeighborIds",
      "LinksGetRelationships",
      "LinksGetRelationshipsIds",

        // TODO: Replace these with overloads once Sol allows vector overloads
      "LinksGetNode",
      "LinksGetNodeKey",
      "LinksGetNodeType",
      "LinksGetNodeProperty",
      "LinksGetNodeProperties",

      "LinksGetRelationship",
      "LinksGetRelationshipType",
      "LinksGetRelationshipProperty",
      "LinksGetRelationshipProperties",
        "mmtest",
        "mmtest2",

      "NodeIdsGet",
      "NodeIdsGetKey",
      "NodeIdsGetProperties",
      "NodeIdsGetProperty",
      "NodeIdsGetType",
      "NodeIdsGetNeighborIds",
      "NodeIdsGetDistinctNeighborIds",
      "NodeIdsGetUniqueNeighborIds",
      "NodeIdsGetNeighbors",

      "NodeGet",
      "NodeGetConnected",
      "NodeGetDegree",
      "NodeGetId",
      "NodeGetKey",
      "NodeGetLinks",
      "NodeGetNeighbors",
      "NodeGetNeighborIds",
      "NodeGetDistinctNeighborIds",
      "NodeGetProperties",
      "NodeGetProperty",
      "NodesGetPropertyRaw",
      "NodeGetRelationships",
      "NodesGetNeighbors",
      "NodeGetType",
      "NodeTypeGet",
      "NodeTypeGetType",
      "NodeTypesGet",
      "NodeTypesGetCount",

      "NodesGetNeighborIds",
      "NodesGetDistinctNeighborIds",

      "RelationshipGet",
      "RelationshipGetEndingNodeId",
      "RelationshipGetProperties",
      "RelationshipGetProperty",
      "RelationshipGetStartingNodeId",
      "RelationshipGetType",
      "RelationshipTypeGet",
      "RelationshipTypeGetType",
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
      "setmetatable" // Used by template
    };

  };

}


#endif// RAGEDB_SANDBOX_H
